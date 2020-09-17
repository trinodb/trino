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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.testng.services.Flaky;
import io.prestosql.tests.hive.util.TemporaryHiveTable;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
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

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHiveTransactionalTable.class);

    private static final int TEST_TIMEOUT = 10 * 60 * 1000;

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadFullAcid()
    {
        doTestReadFullAcid(false, BucketingType.NONE);
    }

    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadFullAcidBucketed()
    {
        doTestReadFullAcid(false, BucketingType.BUCKETED_DEFAULT);
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadFullAcidPartitioned()
    {
        doTestReadFullAcid(true, BucketingType.NONE);
    }

    // This test is in STORAGE_FORMATS group to ensure test coverage of transactional tables with various
    // metastore and HDFS setups (kerberized or not, impersonation or not).
    @Test(groups = {HIVE_TRANSACTIONAL, STORAGE_FORMATS}, timeOut = TEST_TIMEOUT)
    public void testReadFullAcidPartitionedBucketed()
    {
        doTestReadFullAcid(true, BucketingType.BUCKETED_DEFAULT);
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadFullAcidBucketedV1()
    {
        doTestReadFullAcid(false, BucketingType.BUCKETED_V1);
    }

    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadFullAcidBucketedV2()
    {
        doTestReadFullAcid(false, BucketingType.BUCKETED_V2);
    }

    private void doTestReadFullAcid(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
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

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (24, 4)");
            onHive().executeQuery("DELETE FROM " + tableName + " where fcol=4");

            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // test minor compacted data read
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");

            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactly(row(20, 3));

            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("3m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1), row(22, 2));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(21, 1));

            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactly(row(20, 3));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));

            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactly(row(20, 3));

            // test major compaction
            compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("3m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    public void testReadInsertOnly(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
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

            assertThat(query("SELECT col FROM " + tableName + " WHERE col=2")).containsExactly(row(2));

            // test minor compacted data read
            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("3m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));
            assertThat(query("SELECT col FROM " + tableName + " WHERE col=2")).containsExactly(row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3));

            if (getHiveVersionMajor() >= 4) {
                // Major compaction on insert only table does not work prior to Hive 4:
                // https://issues.apache.org/jira/browse/HIVE-21280

                // test major compaction
                onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 4");
                compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("3m"));
                assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3), row(4));
            }
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    public void testReadFullAcidWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_full_acid_acid_converted_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        verify(bucketingType.getHiveTableProperties().isEmpty()); // otherwise we would need to include that in the CREATE TABLE's TBLPROPERTIES
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                bucketingType.getHiveClustering("fcol", 4) + " " +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='false')");

        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            // read with original files and insert delta
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");
            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1), row(22, 2));

            // read with original files and delete delta
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol = 2");
            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1));

            // read with original files and insert+delete delta (UPDATE)
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE fcol = 1" + (isPartitioned ? " AND part_col = 2 " : ""));
            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(23, 1));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    public void testReadInsertOnlyWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_insert_only_acid_converted_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        verify(bucketingType.getHiveTableProperties().isEmpty()); // otherwise we would need to include that in the CREATE TABLE's TBLPROPERTIES
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                bucketingType.getHiveClustering("col", 4) + " " +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='false')");
        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (1)");
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (2)");
            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(INSERT_ONLY, bucketingType));

            // read with original files
            assertThat(query("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2));

            // read with original files and delta
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (3)");
            assertThat(query("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2), row(3));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL)
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
        return new Object[][] {
                {false, BucketingType.NONE},
                {false, BucketingType.BUCKETED_DEFAULT},
                {true, BucketingType.NONE},
                {true, BucketingType.BUCKETED_DEFAULT},
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "testCreateAcidTableDataProvider")
    public void testCtasAcidTable(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(format("ctas_transactional_%s", randomTableSuffix()))) {
            String tableName = table.getName();
            query("CREATE TABLE " + tableName + " " +
                    prestoTableProperties(ACID, isPartitioned, bucketingType) +
                    " AS SELECT * FROM (VALUES (21, 1, 1), (22, 1, 2), (23, 2, 2)) t(col, fcol, partcol)");

            // can we query from Presto
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE partcol = 2 ORDER BY col"))
                    .containsOnly(row(22, 1), row(23, 2));

            // can we query from Hive
            assertThat(onHive().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE partcol = 2 ORDER BY col"))
                    .containsOnly(row(22, 1), row(23, 2));
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "testCreateAcidTableDataProvider")
    public void testCreateAcidTable(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(format("create_transactional_%s", randomTableSuffix()))) {
            String tableName = table.getName();
            query("CREATE TABLE " + tableName + " (col INTEGER, fcol INTEGER, partcol INTEGER)" +
                    prestoTableProperties(ACID, isPartitioned, bucketingType));

            assertThat(() -> query("INSERT INTO " + tableName + " VALUES (1,2,3)")).failsWithMessageMatching(".*Writes to Hive transactional tables are not supported.*");
        }
    }

    @DataProvider
    public Object[][] testCreateAcidTableDataProvider()
    {
        return new Object[][] {
                {false, BucketingType.NONE},
                {false, BucketingType.BUCKETED_DEFAULT},
                {false, BucketingType.BUCKETED_V1},
                {false, BucketingType.BUCKETED_V2},
                {true, BucketingType.NONE},
                {true, BucketingType.BUCKETED_DEFAULT},
        };
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType, BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.addAll(transactionalTableType.getHiveTableProperties());
        tableProperties.addAll(bucketingType.getHiveTableProperties());
        tableProperties.add("'NO_AUTO_COMPACTION'='true'");
        return tableProperties.build().stream().collect(joining(",", "TBLPROPERTIES (", ")"));
    }

    private static String prestoTableProperties(TransactionalTableType transactionalTableType, boolean isPartitioned, BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.addAll(transactionalTableType.getPrestoTableProperties());
        tableProperties.addAll(bucketingType.getPrestoTableProperties("fcol", 4));
        if (isPartitioned) {
            tableProperties.add("partitioned_by = ARRAY['partcol']");
        }
        return tableProperties.build().stream().collect(joining(",", "WITH (", ")"));
    }

    private static void compactTableAndWait(CompactionMode compactMode, String tableName, String partitionString, Duration timeout)
    {
        log.info("Running %s compaction on %s", compactMode, tableName);

        Failsafe.with(
                new RetryPolicy<>()
                        .withMaxDuration(java.time.Duration.ofMillis(timeout.toMillis()))
                        .withMaxAttempts(Integer.MAX_VALUE))  // limited by MaxDuration
                .onFailure(event -> {
                    throw new IllegalStateException(format("Could not compact table %s in %d retries", tableName, event.getAttemptCount()), event.getFailure());
                })
                .onSuccess(event -> log.info("Finished %s compaction on %s in %s (%d tries)", compactMode, tableName, event.getElapsedTime(), event.getAttemptCount()))
                .run(() -> tryCompactingTable(compactMode, tableName, partitionString, Duration.valueOf("60s")));
    }

    private static void tryCompactingTable(CompactionMode compactMode, String tableName, String partitionString, Duration timeout)
            throws TimeoutException
    {
        Instant beforeCompactionStart = Instant.now();
        onHive().executeQuery(format("ALTER TABLE %s %s COMPACT '%s'", tableName, partitionString, compactMode.name())).getRowsCount();

        log.info("Started compactions after %s: %s", beforeCompactionStart, getTableCompactions(compactMode, tableName, Optional.empty()));

        long loopStart = System.nanoTime();
        while (true) {
            try {
                // Compaction takes couple of second so there is no need to check state more frequent than 1s
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            // Since we disabled auto compaction for uniquely named table and every compaction is triggered in this test
            // we can expect that single compaction in given mode should complete before proceeding.
            List<Map<String, String>> startedCompactions = getTableCompactions(compactMode, tableName, Optional.of(beforeCompactionStart));
            verify(startedCompactions.size() < 2, "Expected at most 1 compaction");

            if (startedCompactions.isEmpty()) {
                log.info("Compaction has not started yet. Existing compactions: " + getTableCompactions(compactMode, tableName, Optional.empty()));
                continue;
            }

            String compactionState = startedCompactions.get(0).get("state");

            if (compactionState.equals("failed")) {
                log.info("Compaction has failed: %s", startedCompactions.get(0));
                // This will retry compacting table
                throw new IllegalStateException("Compaction has failed");
            }

            if (compactionState.equals("succeeded")) {
                return;
            }

            if (Duration.nanosSince(loopStart).compareTo(timeout) > 0) {
                log.info("Waiting for compaction has timed out: %s", startedCompactions.get(0));
                throw new TimeoutException("Compaction has timed out");
            }
        }
    }

    private static List<Map<String, String>> getTableCompactions(CompactionMode compactionMode, String tableName, Optional<Instant> startedAfter)
    {
        return Stream.of(onHive().executeQuery("SHOW COMPACTIONS")).flatMap(TestHiveTransactionalTable::mapRows)
                .filter(row -> isCompactionForTable(compactionMode, tableName, row))
                .filter(row -> {
                    if (startedAfter.isPresent()) {
                        try {
                            // start time is expressed in milliseconds
                            return Long.parseLong(row.get("start time")) >= startedAfter.get().truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
                        }
                        catch (NumberFormatException ignored) {
                        }
                    }

                    return true;
                }).collect(toImmutableList());
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

    public enum CompactionMode
    {
        MAJOR,
        MINOR,
        /**/;
    }
}
