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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    ordering: Seq[SortOrder]) extends DataSourceV2ScanExecBase {

  @transient private lazy val batch = scan.toBatch
  @transient private lazy val cachedPartitions = batch.planInputPartitions()

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: ExtendedBatchScanExec => this.batch == other.batch
    case _ => false
  }

  override def hashCode(): Int = batch.hashCode()

  override def partitions: Seq[InputPartition] = {
    if (conf.bucketingEnabled && partitionValues.isDefined) {
      partitionValues.get.map(_._1)
    } else {
      inputPartitions()
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override def inputRDD: RDD[InternalRow] = {
    new DataSourceRDD(sparkContext, partitions, readerFactory, supportsColumnar)
  }

  override def doCanonicalize(): ExtendedBatchScanExec = {
    this.copy(output = output.map(QueryPlan.normalizeExpressions(_, output)))
  }

  override def outputPartitioning: physical.Partitioning = {
    if (partitions.length == 1) {
      SinglePartition
    } else if (partitionValues.isDefined) {
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      DataSourceV2Partitioning(clustering, partitionValues.get.map(_._2))
    } else {
      super.outputPartitioning
    }
  }

  private def inputPartitions(): Seq[InputPartition] = {
    if (cachePartitions) {
      cachedPartitions
    } else {
      batch.planInputPartitions()
    }
  }

  /**
   * Partition values for all the input partitions.
   *
   * NOTE: this is defined iff:
   *   1. all input partitions implement [[HasPartitionKey]]
   *      2. each input partition has a unique partition value.
   *      3. `distribution` is a [[ClusteredDistribution]]
   *
   * Otherwise, the value is None.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   *
   * This should only be called if `conf.bucketingEnabled` is on. It could potentially be
   * expensive since it needs to sort all the input partitions according to their keys.
   *
   * TODO: data sources should be able to plan multiple partitions with the same key and Spark
   * should choose to combine them whenever necessary
   */
  @transient lazy val partitionValues: Option[Seq[(InputPartition, InternalRow)]] = {
    val result = new ArrayBuffer[(InputPartition, InternalRow)]()
    val seen = mutable.HashSet[InternalRow]()
    val partitions = inputPartitions()
    for (p <- partitions) {
      p match {
        case hp: HasPartitionKey if !seen.contains(hp.partitionKey()) =>
          result += ((p, hp.partitionKey))
          seen += hp.partitionKey
        case _ =>
      }
    }

    if (result.nonEmpty && result.length == partitions.length &&
      distribution.isDefined && distribution.get.isInstanceOf[ClusteredDistribution]) {
      // also sort the input partitions according to their partition key order. This ensures
      // a canonical order from both sides of a bucketed join, for example.
      val clustering = distribution.get.asInstanceOf[ClusteredDistribution].clustering
      val keyOrdering: Ordering[(InputPartition, InternalRow)] =
        RowOrdering.createNaturalAscendingOrdering(clustering.map(_.dataType)).on(_._2)
      Some(result.sorted(keyOrdering))
    } else {
      None
    }
  }
}
