/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.tests.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.tests.hive.util.TemporaryHiveTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.testing.assertions.Assert.assertEventually;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MAJOR;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MINOR;
import static io.prestosql.tests.hive.TransactionalTableType.ACID;
import static io.prestosql.tests.hive.TransactionalTableType.INSERT_ONLY;
import static io.prestosql.tests.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = 5 * 60 * 1000)
    public void testReadFullAcid(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName("read_full_acid", isPartitioned, bucketingType))) {
            String tableName = table.getName();
            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    hiveTableProperties(ACID, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // test minor compacted data read
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");
            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("5m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1), row(22, 2));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));

            // test major compaction
            compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("5m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = 5 * 60 * 1000)
    public void testReadInsertOnly(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName("insert_only", isPartitioned, bucketingType))) {
            String tableName = table.getName();

            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("col", 4) + " " +
                    "STORED AS ORC " +
                    hiveTableProperties(INSERT_ONLY, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 1");
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));

            // test minor compacted data read
            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("5m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3));

            if (getHiveVersionMajor() >= 4) {
                // Major compaction on insert only table does not work prior to Hive 4:
                // https://issues.apache.org/jira/browse/HIVE-21280

                // test major compaction
                onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 4");
                compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("5m"));
                assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3), row(4));
            }
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testFailAcidBeforeHive3()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("This tests behavior of ACID table before Hive 3 ");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable("test_fail_acid_before_hive3_" + randomTableSuffix())) {
            String tableName = table.getName();
            onHive().executeQuery("" +
                    "CREATE TABLE " + tableName + "(a bigint) " +
                    "CLUSTERED BY(a) INTO 4 BUCKETS " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='true')");

            assertThat(() -> query("SELECT * FROM " + tableName))
                    .failsWithMessage("Failed to open transaction. Transactional tables support requires Hive metastore version at least 3.0");
        }
    }

    @DataProvider
    public Object[][] partitioningAndBucketingTypeDataProvider()
    {
        return Stream.of(BucketingType.values())
                .flatMap(value -> Stream.of(
                        new Object[] {false, value},
                        new Object[] {true, value}))
                .toArray(Object[][]::new);
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType, BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.addAll(transactionalTableType.getHiveTableProperties());
        tableProperties.addAll(bucketingType.getHiveTableProperties());
        tableProperties.add("'NO_AUTO_COMPACTION'='true'");
        return tableProperties.build().stream().collect(joining(",", "TBLPROPERTIES (", ")"));
    }

    private static void compactTableAndWait(CompactionMode compactMode, String tableName, String partitionString, Duration timeout)
    {
        Set<String> existingCompactionsIds = getTableCompactions(compactMode, tableName)
                .map(row -> row.get("compactionid"))
                .collect(toUnmodifiableSet());

        onHive().executeQuery(format("ALTER TABLE %s %s COMPACT '%s'", tableName, partitionString, compactMode.name()));

        Set<String> tableCompactionsIds = Sets.difference(getTableCompactions(compactMode, tableName)
                .map(row -> row.get("compactionid"))
                .collect(toUnmodifiableSet()),
                existingCompactionsIds);

        assertFalse(tableCompactionsIds.isEmpty(), "tableCompactionsIds are empty");

        assertEventually(timeout, () -> {
            Set<String> completedCompactionsIds = getTableCompactions(compactMode, tableName)
                    .filter(row -> row.get("state").equals("succeeded"))
                    .map(row -> row.get("compactionid"))
                    .collect(toUnmodifiableSet());

            assertTrue(completedCompactionsIds.containsAll(tableCompactionsIds));
        });
    }

    private static Stream<Map<String, String>> getTableCompactions(CompactionMode compactionMode, String tableName)
    {
        return Stream.of(onHive().executeQuery("SHOW COMPACTIONS")).flatMap(TestHiveTransactionalTable::mapRows)
                .filter(row -> isCompactionForTable(compactionMode, tableName, row));
    }

    private static Stream<Map<String, String>> mapRows(QueryResult result)
    {
        if (result.getRowsCount() == 0) {
            return Stream.of();
        }

        List<?> columnNames = result.row(0).stream()
                .filter(Objects::nonNull)
                .collect(toUnmodifiableList());

        ImmutableList.Builder<Map<String, String>> rows = ImmutableList.builder();
        for (int rowIndex = 1; rowIndex < result.getRowsCount(); rowIndex++) {
            ImmutableMap.Builder<String, String> singleRow = ImmutableMap.builder();
            List<?> row = result.row(rowIndex);

            for (int column = 0; column < columnNames.size(); column++) {
                String columnName = ((String) columnNames.get(column)).toLowerCase(ENGLISH);
                singleRow.put(columnName, (String) row.get(column));
            }

            rows.add(singleRow.build());
        }

        return rows.build().stream();
    }

    private static String tableName(String testName, boolean isPartitioned, BucketingType bucketingType)
    {
        return format("test_%s_%b_%s_%s", testName, isPartitioned, bucketingType.name(), randomTableSuffix());
    }

    private static boolean isCompactionForTable(CompactionMode compactMode, String tableName, Map<String, String> row)
    {
        return row.get("table").equals(tableName.toLowerCase(ENGLISH)) &&
                row.get("type").equals(compactMode.name());
    }

    public enum CompactionMode {
        MAJOR,
        MINOR,
        /**/;
    }
}
