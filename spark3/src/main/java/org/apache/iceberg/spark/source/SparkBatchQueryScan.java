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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkBatchQueryScan extends SparkBatchScan {

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;

  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkBatchQueryScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                      boolean caseSensitive, Schema expectedSchema, List<Expression> filters,
                      CaseInsensitiveStringMap options) {

    super(table, io, encryption, caseSensitive, expectedSchema, filters, options);

    this.snapshotId = Spark3Util.propertyAsLong(options, SparkReadOptions.SNAPSHOT_ID, null);
    this.asOfTimestamp = Spark3Util.propertyAsLong(options, SparkReadOptions.AS_OF_TIMESTAMP, null);

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = Spark3Util.propertyAsLong(options, "start-snapshot-id", null);
    this.endSnapshotId = Spark3Util.propertyAsLong(options, "end-snapshot-id", null);
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either " +
                SparkReadOptions.SNAPSHOT_ID + " or " + SparkReadOptions.AS_OF_TIMESTAMP + " is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      TableScan scan = table()
          .newScan()
          .caseSensitive(caseSensitive())
          .project(expectedSchema());

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
      }

      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize(scan)));
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, String.valueOf(splitLookback()));
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost()));

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
      }

      // check if we should combine splits by partition boundary or not
      if (splitByPartition()) {
        try (
            CloseableIterable<FileScanTask> files = scan.planFiles();
            CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(files,
                splitSize(scan))
        ) {
          ListMultimap<StructLike, FileScanTask> groupedFiles = Multimaps.newListMultimap(
              Maps.newHashMap(), Lists::newArrayList);
          splitFiles.forEach(f -> groupedFiles.put(f.partition(), f));

          long splitSize = splitSize(scan);
          this.tasks = Lists.newArrayList(
              CloseableIterable.concat(
                  groupedFiles.asMap().values().stream().map(t ->
                      TableScanUtil.planTasks(CloseableIterable.withNoopClose(t),
                          splitSize, splitLookback(), splitOpenFileCost()
                      )).collect(Collectors.toList()))
          );
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close table scan: " + scan, e);
        }
      } else {
        try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
          this.tasks = Lists.newArrayList(tasksIterable);
        }  catch (IOException e) {
          throw new UncheckedIOException("Failed to close table scan: " + scan, e);
        }
      }
    }

    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(), snapshotId, startSnapshotId, endSnapshotId,
        asOfTimestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}
