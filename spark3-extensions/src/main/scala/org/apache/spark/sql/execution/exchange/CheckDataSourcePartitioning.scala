/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.ExtendedBatchScanExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.util.collection.BitSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Does extra check for plan nodes with [[DataSourceV2Partitioning]] and insert shuffles if they
 * are not compatible with each other.
 */
case class CheckDataSourcePartitioning(spark: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    val updatedPlan = reorderJoinPredicates(pushdownRequiredClustering(plan))
    updatedPlan.transformUp {
      case operator: SparkPlan =>
        val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
        val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
        var children: Seq[SparkPlan] = operator.children
        assert(requiredChildDistributions.length == children.length)
        assert(requiredChildOrderings.length == children.length)

        // Get the indexes of children which have specified distribution requirements and need to have
        // same number of partitions.
        val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
          case (UnspecifiedDistribution, _) => false
          case (_: BroadcastDistribution, _) => false
          case _ => true
        }.map(_._2)

        // assuming transitivity in partitioning compatibility check
        val allCompatible = childrenIndexes.map(children(_).outputPartitioning).sliding(2).map {
          case Seq(_) => true
          case Seq(a, b) => a.isCompatibleWith(b)
        }.forall(p => p)

        if (!allCompatible) {
          // insert shuffle for all children that are data source partitioning
          children = children.zip(requiredChildDistributions).zipWithIndex.map {
            case ((child, _), idx) if !childrenIndexes.contains(idx) =>
              child
            case ((child, distribution), _)
              if child.outputPartitioning.isInstanceOf[DataSourceV2Partitioning] =>
              val numPartitions = distribution.requiredNumPartitions
                .getOrElse(spark.sessionState.conf.numShufflePartitions)
              ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
            case ((child, _), _) =>
              child
          }
        }

        operator.withNewChildren(children)
    }
  }

  private def reorder(
      leftKeys: IndexedSeq[Expression],
      rightKeys: IndexedSeq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): Option[(Seq[Expression], Seq[Expression])] = {
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return None
    }

    // Check if the current order already satisfies the expected order.
    if (expectedOrderOfKeys.zip(currentOrderOfKeys).forall(p => p._1.semanticEquals(p._2))) {
      return Some(leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          return None
      }
    }
    Some(leftKeysBuffer.toSeq, rightKeysBuffer.toSeq)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      reorderJoinKeysRecursively(
        leftKeys,
        rightKeys,
        Some(leftPartitioning),
        Some(rightPartitioning))
        .getOrElse((leftKeys, rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * Recursively reorders the join keys based on partitioning. It starts reordering the
   * join keys to match HashPartitioning on either side, followed by PartitioningCollection.
   */
  private def reorderJoinKeysRecursively(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Option[Partitioning],
      rightPartitioning: Option[Partitioning]): Option[(Seq[Expression], Seq[Expression])] = {
    (leftPartitioning, rightPartitioning) match {
      case (Some(PartitioningCollection(partitionings)), _) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, Some(p), rightPartitioning))
        }.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(PartitioningCollection(partitionings))) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, leftPartitioning, Some(p)))
        }.orElse(None)
      case (Some(DataSourceV2Partitioning(clustering, _)), _) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(DataSourceV2Partitioning(clustering, _))) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, rightKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, leftPartitioning, None))
      case _ =>
        None
    }
  }

  /**
   * Copied from the same method in [[EnsureRequirements]], but only handle
   * [[DataSourceV2Partitioning]].
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isPartial) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isPartial)

      case other => other
    }
  }


  /**
   * Pushdown required clustering expressions to downstream scan nodes, which then use this to
   * group input partitions accordingly and potentially avoid shuffling.
   *
   * For instance, an aggregate may require distribution on [a, b, c], while data source reports
   * clustering distribution [a, b, c, d] with corresponding input partitions. With the info,
   * Spark may choose to group partitions on [a, b, c] and adjust the reported distribution
   * accordingly, to avoid shuffle.
   */
  private def pushdownRequiredClustering(
      plan: SparkPlan,
      keys: Option[Seq[Expression]] = None,
      fromJoin: Boolean = false): SparkPlan = plan match {
    case p: SortMergeJoinExec =>
      val map = Map(p.children.head -> p.leftKeys, p.children(1) -> p.rightKeys)
      plan.mapChildren(c => pushdownRequiredClustering(c, Some(map(c)), fromJoin = true))
    case p: ShuffledHashJoinExec =>
      val map = Map(p.children.head -> p.leftKeys, p.children(1) -> p.rightKeys)
      plan.mapChildren(c => pushdownRequiredClustering(c, Some(map(c)), fromJoin = true))
    case p: UnaryExecNode if p.requiredChildDistribution.head.isInstanceOf[ClusteredDistribution] =>
      val keys = p.requiredChildDistribution.head.asInstanceOf[ClusteredDistribution].clustering
      plan.mapChildren(c => pushdownRequiredClustering(c, Some(keys)))
    case p: ExtendedBatchScanExec if keys.isDefined &&
      p.distribution.isDefined &&
      p.distribution.get.isInstanceOf[ClusteredDistribution] =>

      val reportedKeys = p.distribution.get.asInstanceOf[ClusteredDistribution].clustering
      var effectiveKeys = keys.get.toSet

      // Find out which clustering expressions are used in upstream join/aggregate, and pass down
      // the indices for these to scan nodes. Note this assumes the partition expressions from
      // each join child are properly ordered and matched. In case there are duplicated partition
      // expressions, this always choose the first match from index 0.
      val keyIndices = new BitSet(reportedKeys.length)
      reportedKeys.zipWithIndex.foreach { case (k, idx) =>
        val leaf = DataSourceV2Partitioning.collectLeafExpression(k)
        effectiveKeys.find(_.semanticEquals(leaf)).foreach { e =>
          effectiveKeys -= e
          keyIndices.set(idx)
        }
      }

      // If effective keys are from join and not all of them are present in the reported keys,
      // then we'll have to shuffle. Here we clear all the indices to indicate that scan node
      // shouldn't do the sorting and grouping on input partitions.
      if (effectiveKeys.nonEmpty && fromJoin) {
        keyIndices.clear()
      }

      p.copy(effectiveKeyIndices = Some(keyIndices))
    case _ =>
      plan.mapChildren(c => pushdownRequiredClustering(c, keys, fromJoin))
  }

  private implicit class PartitioningWrapper(p: Partitioning) {
    private[exchange] def isCompatibleWith(other: Partitioning): Boolean = (p, other) match {
      case (left: DataSourceV2Partitioning, right: DataSourceV2Partitioning) =>
        if (left.numPartitions == right.numPartitions &&
          DataSourceV2Partitioning.clusteringCompatible(left.expressions, right.expressions)) {
          val ordering = RowOrdering.createNaturalAscendingOrdering(left.expressions.map(_.dataType))
          left.partitionValues.zip(right.partitionValues).forall { case (v1, v2) =>
            ordering.compare(v1, v2) == 0
          }
        } else {
          false
        }
      case (left: DataSourceV2Partitioning, PartitioningCollection(partitionings)) =>
        partitionings.exists(_.isCompatibleWith(left))
      case (_: DataSourceV2Partitioning, _) =>
        false
      case (PartitioningCollection(partitionings), right: DataSourceV2Partitioning) =>
        partitionings.exists(_.isCompatibleWith(right))
      case (_, _: DataSourceV2Partitioning) =>
        false
      case _ =>
        true
    }
  }
}

