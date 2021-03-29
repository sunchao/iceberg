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

package org.apache.iceberg.spark.extensions;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataSourcePartitioning extends SparkExtensionsTestBase implements AdaptiveSparkPlanHelper {
  private final String tableName2;
  private final TableIdentifier tableIdent2;

  public TestDataSourcePartitioning(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    tableName2 = (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default.table2";
    tableIdent2 = TableIdentifier.of(Namespace.of("default"), "table2");
  }

  @Before
  public void setupSparkConf() {
    spark.conf().set("spark.sql.adaptive.enabled", "true");
    spark.conf().set("spark.sql.adaptive.forceApply", "true");
    spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName2);
  }

  @Test
  public void testBasic() {
    sql("CREATE TABLE %s (key bigint NOT NULL, data string) USING iceberg PARTITIONED BY " +
        "(bucket(16, key))", tableName);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    sql("CREATE TABLE %s (id bigint NOT NULL, value int) USING iceberg PARTITIONED BY " +
        "(bucket(16, id))", tableName2);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName2);
    Table table2 = validationCatalog.loadTable(tableIdent2);

    distributionMode = table2.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, 1), (2, 2), (3, 3), (4, 4)", tableName2);

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1L, "a", 1});
    expected.add(new Object[]{2L, "b", 2});
    expected.add(new Object[]{3L, "c", 3});
    expected.add(new Object[]{4L, "d", 4});
    checkResult("SELECT key, data, value FROM %s t1 JOIN %s t2 ON t1.key = t2.id",
        0, expected, tableName, tableName2);
  }

  @Test
  public void testMultiPartitionKeys() {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, data string) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01' as timestamp), 'a'), " +
            "(2, cast('2020-02-01' as timestamp), 'b'), (3, cast('2020-03-01' as timestamp), 'c')" +
            ", (4, cast('2020-04-01' as timestamp), 'd')", tableName);

    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, value int) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName2);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName2);
    Table table2 = validationCatalog.loadTable(tableIdent2);

    distributionMode = table2.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01' as timestamp), 10), " +
        "(2, cast('2020-02-01' as timestamp), 20), (2, cast('2020-02-01' as timestamp), 25), " +
        "(3, cast ('2020-03-01' as timestamp), 30), (4, cast('2020-04-01' as timestamp), 40)",
        tableName2);

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1L, "a", 10});
    expected.add(new Object[]{2L, "b", 20});
    expected.add(new Object[]{2L, "b", 25});
    expected.add(new Object[]{3L, "c", 30});
    expected.add(new Object[]{4L, "d", 40});

    checkResult("SELECT t1.id, data, value FROM %s t1 JOIN %s t2 ON " +
            "t1.id = t2.id AND t1.ts = t2.ts", 0, expected, tableName, tableName2);

    // partition keys and join keys order mismatch will trigger shuffle
    checkResult("SELECT t1.id, data, value FROM %s t1 JOIN %s t2 ON " +
        "t1.ts = t2.ts AND t1.id = t2.id", 2, expected, tableName, tableName2);
  }

  @Test
  public void testGroupingPartitions() {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, data string) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01 00:00:00' as timestamp), 'a'), " +
        "(2, cast('2020-02-01 00:00:00' as timestamp), 'b'), " +
        "(3, cast('2020-03-01 00:00:00' as timestamp), 'c'), " +
        "(4, cast('2020-04-01 00:00:00' as timestamp), 'd')", tableName);

    sql("CREATE TABLE %s (id bigint NOT NULL, key string, value int) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), key)", tableName2);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName2);
    Table table2 = validationCatalog.loadTable(tableIdent2);

    distributionMode = table2.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1,'aaa', 10), (2, 'bbb', 20), (2, 'bbb', 25), " +
            "(3, 'ccc', 30), (4, 'ddd', 40)", tableName2);

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1L, "a", Timestamp.valueOf("2020-01-01 00:00:00"), "aaa", 10});
    expected.add(new Object[]{2L, "b", Timestamp.valueOf("2020-02-01 00:00:00"), "bbb", 20});
    expected.add(new Object[]{2L, "b", Timestamp.valueOf("2020-02-01 00:00:00"), "bbb", 25});
    expected.add(new Object[]{3L, "c", Timestamp.valueOf("2020-03-01 00:00:00"), "ccc", 30});
    expected.add(new Object[]{4L, "d", Timestamp.valueOf("2020-04-01 00:00:00"), "ddd", 40});

    checkResult("SELECT t1.id, data, ts, key, value FROM %s t1 JOIN %s t2 ON t1.id = t2.id",
        0, expected, tableName, tableName2);
  }

  @Test
  public void testGroupingPartitionsFallback() {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, data string) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01 00:00:00' as timestamp), 'a'), " +
        "(2, cast('2020-02-01 00:00:00' as timestamp), 'b'), " +
        "(3, cast('2020-03-01 00:00:00' as timestamp), 'c'), " +
        "(4, cast('2020-04-01 00:00:00' as timestamp), 'd')", tableName);

    sql("CREATE TABLE %s (id bigint NOT NULL, key string, value int) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), key)", tableName2);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName2);
    Table table2 = validationCatalog.loadTable(tableIdent2);

    distributionMode = table2.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1,'aaa', 10), (2, 'bbb', 20), (2, 'bbb', 25), " +
        "(3, 'ccc', 30), (4, 'ddd', 40)", tableName2);

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1L, "a", 10});
    expected.add(new Object[]{2L, "b", 20});
    expected.add(new Object[]{2L, "b", 25});
    expected.add(new Object[]{3L, "c", 30});
    expected.add(new Object[]{4L, "d", 40});

    // `t1.ts` and `t2.key` are not selected, as result, due to projection pushdown in Spark the
    // corresponding expressions in clustered distribution cannot be resolved (see
    // `ExtendedDataSourceV2Strategy` for more details. In this case we should fallback to old ways.
    checkResult("SELECT t1.id, data, value FROM %s t1 JOIN %s t2 ON t1.id = t2.id",
        2, expected, tableName, tableName2);
  }

  @Test
  public void testOneSideUnpartitioned() {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, data string) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    String distributionMode = table.properties().get(TableProperties.WRITE_DISTRIBUTION_MODE);
    Assert.assertEquals("Distribution mode must match", "hash", distributionMode);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01' as timestamp), 'a'), " +
        "(2, cast('2020-02-01' as timestamp), 'b'), (3, cast('2020-03-01' as timestamp), 'c')" +
        ", (4, cast('2020-04-01' as timestamp), 'd')", tableName);

    sql("CREATE TABLE %s (id bigint NOT NULL, ts TIMESTAMP, value int) USING iceberg " +
        "PARTITIONED BY (bucket(32, id), days(ts))", tableName2);
    sql("ALTER TABLE %s WRITE UNORDERED DISTRIBUTED BY PARTITION", tableName2);

    sql("INSERT INTO TABLE %s VALUES (1, cast('2020-01-01' as timestamp), 10), " +
            "(2, cast('2020-02-01' as timestamp), 20), (2, cast('2020-02-01' as timestamp), 25), " +
            "(3, cast ('2020-03-01' as timestamp), 30), (4, cast('2020-04-01' as timestamp), 40)",
        tableName2);

    List<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{1L, "a", 10});
    expected.add(new Object[]{2L, "b", 20});
    expected.add(new Object[]{2L, "b", 25});
    expected.add(new Object[]{3L, "c", 30});
    expected.add(new Object[]{4L, "d", 40});

    // only one table specifies distribution and thus will use hash partitioning, and the other
    // side will also be converted to hash partitioning
    checkResult("SELECT t1.id, data, value FROM %s t1 JOIN %s t2 ON " +
        "t1.ts = t2.ts AND t1.id = t2.id", 2, expected, tableName, tableName2);
  }

  private void checkResult(String query, int numShuffle, List<Object[]> expected, Object... args) {
    Dataset<Row> ds = spark.sql(String.format(query, args));
    final List<SparkPlan> shuffles = new ArrayList<>();
    foreach(ds.queryExecution().executedPlan(), n -> {
      if (n instanceof ShuffleExchangeExec) {
        shuffles.add(n);
      }
      return null;
    });
    Assert.assertEquals(numShuffle, shuffles.size());
    List<Object[]> actual = sql(query, args).stream().sorted(comparator(ds.schema())).collect(Collectors.toList());
    assertEquals("Incorrect query result", expected, actual);
  }

  private static Comparator<Object[]> comparator(StructType type) {
    final List<Comparator<Object>> comparators =
        Arrays.stream(type.fields())
            .map(StructField::dataType)
            .map(SparkSchemaUtil::convert)
            .map(dt -> (Type.PrimitiveType) dt) // requires all primitive types
            .map(Comparators::forType)
            .collect(Collectors.toList());

    return (o1, o2) -> {
      for (int i = 0; i < o1.length; i++) {
        int cmp = comparators.get(i).compare(o1[i], o2[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    };
  }
}
