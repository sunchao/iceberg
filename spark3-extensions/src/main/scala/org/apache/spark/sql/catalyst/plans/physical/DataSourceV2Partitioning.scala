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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IcebergBucketTransform
import org.apache.spark.sql.catalyst.expressions.IcebergDayTransform
import org.apache.spark.sql.catalyst.expressions.IcebergHourTransform
import org.apache.spark.sql.catalyst.expressions.IcebergMonthTransform
import org.apache.spark.sql.catalyst.expressions.IcebergYearTransform
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical
import scala.annotation.tailrec

/**
 * Represents a partitioning where rows are split across partitions based on transforms defined
 * by `expressions`. `partitionValues` should contain value of partition key(s) in ascending order,
 * after evaluated by the transforms in `expressions`, for each input partition. In addition, its
 * length must be the same as the number of input partitions (and thus is a 1-1 mapping), and each
 * row in `partitionValues` must be unique.
 *
 * For example, if `expressions` is `[years(ts_col)]`, then a valid value of `partitionValues` is
 * `[50, 51, 52]`, which represents 3 input partitions with distinct partition values. All rows
 * in each partition have the same value for column `ts_col` (which is of timestamp type), after
 * being applied by the `years` transform.
 *
 * On the other hand, `[50, 50, 51]` is not a valid value for `partitionValues` since `50` is
 * duplicated twice.
 *
 * - `expressions`: partition expressions for the partitioning.
 * - `partitionValues`: the values for the cluster keys of the distribution, must be in ascending
 *   order.
 */
case class DataSourceV2Partitioning(
    expressions: Seq[Expression],
    partitionValues: Seq[InternalRow]) extends Partitioning {
  override val numPartitions: Int = partitionValues.length

  override def satisfies0(required: physical.Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case p: HashClusteredDistribution =>
          val attributes = expressions.map(DataSourceV2Partitioning.collectLeafExpression)
          p.expressions.length == attributes.length &&
            p.expressions.zip(attributes).forall {
              case (l, r) => l.semanticEquals(r)
            }
        case p: ClusteredDistribution =>
          val attributes = expressions.map(DataSourceV2Partitioning.collectLeafExpression)
          attributes.forall(c => p.clustering.exists(_.semanticEquals(c)))
        case _ =>
          false
      }
    }
  }
}

object DataSourceV2Partitioning {
  def clusteringCompatible(left: Seq[Expression], right: Seq[Expression]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
      expressionCompatible(l, r)
    }
  }

  // TODO: we should compare functions using their ID and argument length
  private def expressionCompatible(left: Expression, right: Expression): Boolean = {
    (left, right) match {
      case (_: IcebergYearTransform, _: IcebergYearTransform) => true
      case (_: IcebergMonthTransform, _: IcebergMonthTransform) => true
      case (_: IcebergDayTransform, _: IcebergDayTransform) => true
      case (_: IcebergHourTransform, _: IcebergHourTransform) => true
      case (IcebergBucketTransform(leftNumBuckets, _),
      IcebergBucketTransform(rightNumBuckets, _)) =>
        leftNumBuckets == rightNumBuckets
      case _ => false
    }
  }

  @tailrec
  def collectLeafExpression(e: Expression): Expression = e match {
    case IcebergYearTransform(child) => collectLeafExpression(child)
    case IcebergMonthTransform(child) => collectLeafExpression(child)
    case IcebergDayTransform(child) => collectLeafExpression(child)
    case IcebergHourTransform(child) => collectLeafExpression(child)
    case IcebergBucketTransform(_, child) => collectLeafExpression(child)
    case a: LeafExpression => a
    case _ =>
      throw new IllegalArgumentException(s"unexpected expression: $e")
  }
}
