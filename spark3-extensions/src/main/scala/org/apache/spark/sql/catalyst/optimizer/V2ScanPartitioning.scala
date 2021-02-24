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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.DistributionAndOrderingUtils
import org.apache.spark.sql.connector.iceberg.read.SupportsReportPartitioning
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2ScanRelation

case class V2ScanPartitioning(spark: SparkSession) extends Rule[LogicalPlan] {
  private val conf = spark.sessionState.conf

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case d @ DataSourceV2ScanRelation(_, scan, output) =>
      scan match {
        case v: SupportsReportPartitioning =>
          val distribution = DistributionAndOrderingUtils.toCatalyst(
            v.distribution, plan, conf.resolver)
          val ordering = v.ordering.map(
            DistributionAndOrderingUtils.toCatalyst(_, plan, conf.resolver).asInstanceOf[SortOrder])
          ExtendedDataSourceV2ScanRelation(d, output, Some(distribution), ordering)
        case _ =>
          ExtendedDataSourceV2ScanRelation(d, output, None, Seq.empty)
      }
  }
}
