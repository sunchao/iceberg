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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.DataSourceV2Partitioning
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.connector.iceberg.read.HasPartitionKey
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.util.collection.BitSet
import scala.collection.mutable

// The only reason we need this class and cannot reuse BatchScanExec is because
// BatchScanExec caches input partitions and we cannot apply file filtering before execution
// Spark calls supportsColumnar during physical planning which, in turn, triggers split planning
// We must ensure the result is not cached so that we can push down file filters later
// The only difference compared to BatchScanExec is that we are using def instead of lazy val for splits
case class ExtendedBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    cachePartitions: Boolean = false,
    distribution: Option[Distribution],
    ordering: Seq[SortOrder],
    effectiveKeyIndices: Option[BitSet] = None) extends DataSourceV2ScanExecBase {

  @transient private lazy val batch = scan.toBatch
  @transient private lazy val cachedPartitions = batch.planInputPartitions()

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: ExtendedBatchScanExec =>
      this.batch == other.batch &&
        this.effectiveKeyIndices == other.effectiveKeyIndices
    case _ => false
  }

  override def hashCode(): Int = batch.hashCode()

  override def partitions: Seq[InputPartition] = {
    if (cachePartitions) {
      cachedPartitions
    } else {
      batch.planInputPartitions()
    }
  }

  @transient lazy val combinedPartitions: Seq[Seq[InputPartition]] = {
    if (conf.bucketingEnabled && partitionValues.isDefined) {
      partitionValues.get.map(_._2)
    } else {
      partitions.map(Seq(_))
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override def inputRDD: RDD[InternalRow] = {
    new ExtendedDataSourceRDD(sparkContext, combinedPartitions, readerFactory, supportsColumnar)
  }

  override def doCanonicalize(): ExtendedBatchScanExec = {
    this.copy(output = output.map(QueryPlan.normalizeExpressions(_, output)))
  }

  override def outputPartitioning: physical.Partitioning = {
    if (partitions.length == 1) {
      SinglePartition
    } else if (distribution.isDefined &&
      distribution.get.isInstanceOf[ClusteredDistribution] &&
      effectiveKeyIndices.isEmpty) {
      // `effectiveKeyIndices` is not defined - meaning that the partitioning is not yet used by
      // `CheckDataSourcePartitioning`, and therefore we just pass empty partition values to save
      // calculation since it is not used
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      DataSourceV2Partitioning(clustering, Seq.empty)
    } else if (partitionValues.isDefined) {
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      val indices = effectiveKeyIndices.get
      val prunedClustering = clustering.zipWithIndex.filter(t => indices.get(t._2)).map(_._1)
      DataSourceV2Partitioning(prunedClustering, partitionValues.get.map(_._1))
    } else {
      super.outputPartitioning
    }
  }


  /**
   * Partition values for all the input partitions.
   *
   * NOTE: this is defined iff:
   *
   *  1. all input partitions implement [[HasPartitionKey]]
   *  1. each input partition has a unique partition value.
   *  1. `distribution` is a [[ClusteredDistribution]]
   *
   * Otherwise, the value is None.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   *
   * This should only be called if `conf.bucketingEnabled` is on. It could potentially be
   * expensive since it needs to sort all the input partitions according to their keys.
   */
  @transient lazy val partitionValues: Option[Seq[(InternalRow, Seq[InputPartition])]] = {
    if (!conf.bucketingEnabled ||
      distribution.isEmpty ||
      !distribution.get.isInstanceOf[ClusteredDistribution] ||
      effectiveKeyIndices.isEmpty ||
      // this means none of the clustering keys are used so no longer need data source partitioning
      effectiveKeyIndices.get.cardinality() == 0) {
      None
    } else {
      val cd = distribution.get.asInstanceOf[ClusteredDistribution]
      val partitions = this.partitions
      val result = partitions.takeWhile {
        case _: HasPartitionKey => true
        case _ => false
      }.map(p => (p.asInstanceOf[HasPartitionKey].partitionKey(), p))

      if (result.length != partitions.length) {
        None
      } else {
        val indices = effectiveKeyIndices.get
        val dataType = cd.clustering.map(_.dataType)
        val projectedDataType = dataType.zipWithIndex
          .filter(t => indices.get(t._2))
          .map(_._1)
        val groupedPartitions = result.groupBy { t =>
          val oldPartitionValue = t._1
          val newPartitionValue = new SpecificInternalRow(projectedDataType)
          val it = indices.iterator
          var newIdx = 0
          while (it.hasNext) {
            val i = it.next
            newPartitionValue.update(newIdx, oldPartitionValue.get(i, dataType(i)))
            newIdx += 1
          }
          newPartitionValue
        }.map(p => (p._1, p._2.map(t => t._2))).toSeq

        // also sort the input partitions according to their partition key order. This ensures
        // a canonical order from both sides of a bucketed join, for example.
        val keyOrdering: Ordering[(InternalRow, Seq[InputPartition])] = {
          RowOrdering.createNaturalAscendingOrdering(projectedDataType).on(_._1)
        }
        Some(groupedPartitions.sorted(keyOrdering))
      }
    }
  }
}

