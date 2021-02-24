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

/**
 * Does extra check for plan nodes with [[DataSourceV2Partitioning]] and insert shuffles if they
 * are not compatible with each other.
 */
case class CheckDataSourcePartitioning(spark: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
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

