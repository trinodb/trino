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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.tempto.assertions.QueryAssert;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import io.prestosql.testng.services.Flaky;
import io.prestosql.tests.hive.util.TemporaryHiveTable;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.BucketingType.BUCKETED_V2;
import static io.prestosql.tests.hive.BucketingType.NONE;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MAJOR;
import static io.prestosql.tests.hive.TestHiveTransactionalTable.CompactionMode.MINOR;
import static io.prestosql.tests.hive.TransactionalTableType.ACID;
import static io.prestosql.tests.hive.TransactionalTableType.INSERT_ONLY;
import static io.prestosql.tests.hive.util.TableLocationUtils.getTablePath;
import static io.prestosql.tests.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHiveTransactionalTable.class);

    private static final int TEST_TIMEOUT = 15 * 60 * 1000;

    @Inject
    private TestHiveMetastoreClientFactory testHiveMetastoreClientFactory;

    @Inject
    private HdfsClient hdfsClient;

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

            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("6m"));
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
            compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("6m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(20, 3), row(23, 1));
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
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
            compactTableAndWait(MINOR, tableName, hivePartitionString, Duration.valueOf("6m"));
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));
            assertThat(query("SELECT col FROM " + tableName + " WHERE col=2")).containsExactly(row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3));

            if (getHiveVersionMajor() >= 4) {
                // Major compaction on insert only table does not work prior to Hive 4:
                // https://issues.apache.org/jira/browse/HIVE-21280

                // test major compaction
                onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 4");
                compactTableAndWait(MAJOR, tableName, hivePartitionString, Duration.valueOf("6m"));
                assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3), row(4));
            }
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
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
    public void testUpdateFullAcidWithOriginalFilesPrestoInserting(boolean isPartitioned, BucketingType bucketingType)
    {
        withTemporaryTable("presto_update_full_acid_acid_converted_table_read", true, isPartitioned, bucketingType, tableName -> {
            onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
            verify(bucketingType.getHiveTableProperties().isEmpty()); // otherwise we would need to include that in the CREATE TABLE's TBLPROPERTIES
            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            if (isPartitioned) {
                query("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 4, 2)");
            }
            else {
                query("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 4)");
            }

            // read with original files and insert delta
            if (isPartitioned) {
                query("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 3, 2)");
            }
            else {
                query("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 3)");
            }

            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1), row(22, 2));

            // read with original files and delete delta
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol = 2");

            assertThat(query("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1));
        });
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    public void testUpdateFullAcidWithOriginalFilesPrestoInsertingAndDeleting(boolean isPartitioned, BucketingType bucketingType)
    {
        withTemporaryTable("presto_update_full_acid_acid_converted_table_read", true, isPartitioned, bucketingType, tableName -> {
            onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
            verify(bucketingType.getHiveTableProperties().isEmpty()); // otherwise we would need to include that in the CREATE TABLE's TBLPROPERTIES
            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (10, 100), (11, 110), (12, 120), (13, 130), (14, 140)");
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (15, 150), (16, 160), (17, 170), (18, 180), (19, 190)");

            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE col < 12")).containsOnly(row(10, 100), row(11, 110));

            String fields = isPartitioned ? "(col, fcol, part_col)" : "(col, fcol)";
            query(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(30, 5, 2, isPartitioned, 3)));
            query(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(40, 5, 2, isPartitioned, 3)));

            query("DELETE FROM " + tableName + " WHERE col IN (11, 12)");
            query("DELETE FROM " + tableName + " WHERE col IN (16, 17)");
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol >= 100")).containsOnly(row(10, 100), row(13, 130), row(14, 140), row(15, 150), row(18, 180), row(19, 190));

            // read with original files and delete delta
            query("DELETE FROM " + tableName + " WHERE col = 18 OR col = 14 OR (fcol = 2 AND (col / 2) * 2 = col)");

            assertThat(onHive().executeQuery("SELECT col, fcol FROM " + tableName))
                    .containsOnly(row(10, 100), row(13, 130), row(15, 150), row(19, 190), row(31, 2), row(33, 2), row(41, 2), row(43, 2));

            assertThat(query("SELECT col, fcol FROM " + tableName))
                    .containsOnly(row(10, 100), row(13, 130), row(15, 150), row(19, 190), row(31, 2), row(33, 2), row(41, 2), row(43, 2));
        });
    }

    String makeValues(int colStart, int colCount, int fcol, boolean isPartitioned, int partCol)
    {
        return IntStream.range(colStart, colStart + colCount - 1)
                .boxed()
                .map(n -> isPartitioned ? format("(%s, %s, %s)", n, fcol, partCol) : format("(%s, %s)", n, fcol))
                .collect(Collectors.joining(", "));
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/4927", match = "Hive table .* is is corrupt. Found sub-directory in bucket directory for partition")
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
        withTemporaryTable("create_transactional", true, isPartitioned, bucketingType, tableName -> {
            query("CREATE TABLE " + tableName + " (col INTEGER, fcol INTEGER, partcol INTEGER)" +
                    prestoTableProperties(ACID, isPartitioned, bucketingType));

            query("INSERT INTO " + tableName + " VALUES (1, 2, 3)");
            assertThat(query("SELECT * FROM " + tableName)).containsOnly(row(1, 2, 3));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testSimpleUnpartitionedTransactionalInsert()
    {
        withTemporaryTable("unpartitioned_transactional_insert", true, false, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));

            onPresto().executeQuery(format("INSERT INTO %s VALUES (11, 100), (12, 200), (13, 300)", tableName));

            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(11, 100L), row(12, 200L), row(13, 300L));

            onPresto().executeQuery(format("INSERT INTO %s VALUES (14, 400), (15, 500), (16, 600)", tableName));

            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(11, 100L), row(12, 200L), row(13, 300L), row(14, 400L), row(15, 500L), row(16, 600L));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testTransactionalPartitionInsert()
    {
        withTemporaryTable("transactional_partition_insert", true, true, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));

            onPresto().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            verifySelectForPrestoAndHive(format("SELECT COUNT(*) FROM %s", tableName), "column1 > 10", row(20));

            onPresto().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 30),
                    makeInsertValues(2, 21, 30)));

            verifySelectForPrestoAndHive(format("SELECT COUNT(*) FROM %s", tableName), "column1 > 15 AND column1 <= 25", row(20));

            onHive().executeQuery(format("DELETE FROM %s WHERE column1 > 15 AND column1 <= 25", tableName));

            verifySelectForPrestoAndHive(format("SELECT COUNT(*) FROM %s", tableName), "column1 > 15 AND column1 <= 25", row(0));

            onPresto().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 20, 23),
                    makeInsertValues(2, 20, 23)));

            verifySelectForPrestoAndHive(format("SELECT COUNT(*) FROM %s", tableName), "column1 > 15 AND column1 <= 25", row(8));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testTransactionalBucketedPartitionedInsert()
    {
        testTransactionalBucketedPartitioned(false);
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testTransactionalBucketedPartitionedInsertOnly()
    {
        testTransactionalBucketedPartitioned(true);
    }

    private void testTransactionalBucketedPartitioned(boolean insertOnly)
    {
        withTemporaryTable("bucketed_partitioned_insert_only", true, true, BUCKETED_V2, tableName -> {
            String insertOnlyProperty = insertOnly ? ", 'transactional_properties'='insert_only'" : "";
            onHive().executeQuery(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS" +
                            " STORED AS ORC TBLPROPERTIES ('transactional' = 'true'%s)",
                    tableName, insertOnlyProperty));

            onPresto().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForPrestoAndHive(format("SELECT customer FROM %s", tableName), "purchase = 'lemons'", row("Ann"), row("Lou"));

            verifySelectForPrestoAndHive(format("SELECT purchase FROM %s", tableName), "customer = 'Fred'", row("cards"), row("cereal"), row("limes"), row("chips"));

            onPresto().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForPrestoAndHive(format("SELECT customer FROM %s", tableName), "purchase = 'corn'", row("Debby"), row("Joe"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testTransactionalUnpartitionedDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("unpartitioned_delete", true, false, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INTEGER, column2 BIGINT) WITH (format = 'ORC', transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column2 = 100", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(2, 200), row(3, 300), row(4, 400), row(5, 500));

            execute(inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700)", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column1 = 4", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(2, 200), row(3, 300), row(5, 500), row(6, 600), row(7, 700));

            execute(deleter, format("DELETE FROM %s WHERE column1 <= 3 OR column1 = 6", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(5, 500), row(7, 700));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testMultiDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("unpartitioned_multi_delete", true, false, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000)", tableName));

            execute(deleter, format("DELETE FROM %s WHERE column1 = 9", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column1 = 2 OR column1 = 3", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(1, 100), row(4, 400), row(5, 500), row(6, 600), row(7, 700), row(8, 800), row(10, 1000));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testTransactionalMetadataDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("metadata_delete", true, true, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
            execute(inserter, format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            execute(deleter, format("DELETE FROM %s WHERE column2 = 1", tableName));
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "column2 = 1", row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testNonTransactionalMetadataDelete()
    {
        withTemporaryTable("non_transactional_metadata_delete", false, true, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column2 BIGINT, column1 INT) WITH (partitioned_by = ARRAY['column1'])", tableName));

            execute(HiveOrPresto.PRESTO, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 10),
                    makeInsertValues(2, 1, 10)));

            execute(HiveOrPresto.PRESTO, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 11, 20),
                    makeInsertValues(2, 11, 20)));

            execute(HiveOrPresto.PRESTO, format("DELETE FROM %s WHERE column1 = 1", tableName));
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "column1 = 1", row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testUnpartitionedDeleteAll(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("unpartitioned_delete_all", true, false, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(deleter, "DELETE FROM " + tableName);
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "true", row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testMultiColumnDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("multi_column_delete", true, false, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            String where = " WHERE column1 >= 2 AND column2 <= 400";
            execute(deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "column1 IN (1, 5)", row(1, 100), row(5, 500));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testPartitionAndRowsDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("partition_and_rows_delete", true, true, NONE, tableName -> {
            onPresto().executeQuery("CREATE TABLE " + tableName +
                    " (column2 BIGINT, column1 INT) WITH (transactional = true, partitioned_by = ARRAY['column1'])");
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (1, 200), (2, 300), (2, 400), (2, 500)", tableName));
            String where = " WHERE column1 = 2 OR column2 = 200";
            execute(deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForPrestoAndHive("SELECT column1, column2 FROM " + tableName, "true", row(1, 100));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testPartitionedInsertAndRowLevelDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("partitioned_row_level_delete", true, true, NONE, tableName -> {
            onPresto().executeQuery(format("CREATE TABLE %s (column2 INT, column1 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column1'])", tableName));

            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 40),
                    makeInsertValues(2, 21, 40)));

            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "column2 > 10 AND column2 <= 30", row(40));

            execute(deleter, format("DELETE FROM %s WHERE column2 > 10 AND column2 <= 30", tableName));
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "column2 > 10 AND column2 <= 30", row(0));
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "true", row(40));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testBucketedPartitionedDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("bucketed_partitioned_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForPrestoAndHive(format("SELECT customer FROM %s", tableName), "purchase = 'lemons'", row("Ann"), row("Lou"));

            verifySelectForPrestoAndHive(format("SELECT purchase FROM %s", tableName), "customer = 'Fred'", row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForPrestoAndHive("SELECT customer FROM " + tableName, "purchase = 'corn'", row("Debby"), row("Joe"));

            execute(deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForPrestoAndHive("SELECT purchase FROM " + tableName, "customer = 'Ann'", row("cards"), row("cereal"), row("chips"));

            execute(deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForPrestoAndHive("SELECT customer, purchase FROM " + tableName, "true", row("Fred", "limes"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testBucketedUnpartitionedDelete(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("bucketed_unpartitioned_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForPrestoAndHive(format("SELECT customer FROM %s", tableName), "purchase = 'lemons'", row("Ann"), row("Lou"));

            verifySelectForPrestoAndHive(format("SELECT purchase FROM %s", tableName), "customer = 'Fred'", row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForPrestoAndHive("SELECT customer FROM " + tableName, "purchase = 'corn'", row("Debby"), row("Joe"));

            execute(deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForPrestoAndHive("SELECT purchase FROM " + tableName, "customer = 'Ann'", row("cards"), row("cereal"), row("chips"));

            execute(deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForPrestoAndHive("SELECT customer, purchase FROM " + tableName, "true", row("Fred", "limes"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testCorrectSelectCountStar(HiveOrPresto inserter, HiveOrPresto deleter)
    {
        withTemporaryTable("select_count_star_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", tableName));

            execute(inserter, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c'), (4, 400, 'a'), (5, 500, 'b'), (6, 600, 'c')", tableName));
            execute(deleter, format("DELETE FROM %s WHERE col2 = 200", tableName));
            verifySelectForPrestoAndHive("SELECT COUNT(*) FROM " + tableName, "true", row(5));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "insertersProvider", timeOut = TEST_TIMEOUT)
    public void testInsertOnlyMultipleWriters(boolean bucketed, HiveOrPresto inserter1, HiveOrPresto inserter2)
    {
        log.info("testInsertOnlyMultipleWriters bucketed %s, inserter1 %s, inserter2 %s", bucketed, inserter1, inserter2);
        withTemporaryTable("insert_only_partitioned", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) %s STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
                    tableName, bucketed ? "CLUSTERED BY (col2) INTO 3 BUCKETS" : ""));

            execute(inserter1, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b')", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(1, 100, "a"), row(2, 200, "b"));

            execute(inserter2, format("INSERT INTO %s VALUES (3, 300, 'c'), (4, 400, 'a')", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"));

            execute(inserter1, format("INSERT INTO %s VALUES (5, 500, 'b'), (6, 600, 'c')", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "col2 > 300", row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"));

            execute(inserter2, format("INSERT INTO %s VALUES (7, 700, 'b'), (8, 800, 'c')", tableName));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "true", row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"), row(7, 700, "b"), row(8, 800, "c"));
            verifySelectForPrestoAndHive("SELECT * FROM " + tableName, "col3 = 'c'", row(3, 300, "c"), row(6, 600, "c"), row(8, 800, "c"));
        });
    }

    @DataProvider
    public Object[][] insertersProvider()
    {
        return new Object[][] {
                {false, HiveOrPresto.HIVE, HiveOrPresto.PRESTO},
                {false, HiveOrPresto.PRESTO, HiveOrPresto.PRESTO},
                {true, HiveOrPresto.HIVE, HiveOrPresto.PRESTO},
                {true, HiveOrPresto.PRESTO, HiveOrPresto.PRESTO},
        };
    }

    private enum HiveOrPresto
    {
        HIVE,
        PRESTO
    }

    private static QueryResult execute(HiveOrPresto hiveOrPresto, String sql, QueryExecutor.QueryParam... params)
    {
        return executorFor(hiveOrPresto).executeQuery(sql, params);
    }

    private static QueryExecutor executorFor(HiveOrPresto hiveOrPresto)
    {
        switch (hiveOrPresto) {
            case HIVE:
                return onHive();
            case PRESTO:
                return onPresto();
            default:
                throw new IllegalStateException("Unknown enum value " + hiveOrPresto);
        }
    }

    @DataProvider
    public Object[][] inserterAndDeleterProvider()
    {
        return new Object[][] {
                {HiveOrPresto.HIVE, HiveOrPresto.PRESTO},
                {HiveOrPresto.PRESTO, HiveOrPresto.PRESTO},
                {HiveOrPresto.PRESTO, HiveOrPresto.HIVE}
        };
    }

    void withTemporaryTable(String rootName, boolean transactional, boolean isPartitioned, BucketingType bucketingType, Consumer<String> testRunner)
    {
        if (transactional) {
            ensureTransactionalHive();
        }
        String tableName = null;
        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName(rootName, isPartitioned, bucketingType))) {
            tableName = table.getName();
            testRunner.accept(tableName);
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    @Flaky(issue = "https://github.com/prestosql/presto/issues/5463", match = "Expected row count to be <4>, but was <6>")
    public void testFilesForAbortedTransactionsIgnored()
            throws Exception
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_aborted_transaction_table";
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + " (col INT) " +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true')");

        ThriftHiveMetastoreClient client = testHiveMetastoreClientFactory.createMetastoreClient();
        try {
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + " ORDER BY COL";

            // Create `delta-A` file
            onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES (1),(2)");
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(1), row(2));

            String tableLocation = getTablePath(tableName);

            // Insert data to create a valid delta, which creates `delta-B`
            onHive().executeQuery("INSERT INTO TABLE " + tableName + " SELECT 3");

            // Simulate aborted transaction in Hive which has left behind a write directory and file (`delta-C` i.e `delta_0000003_0000003_0000`)
            long transaction = client.openTransaction("test");
            client.allocateTableWriteIds("default", tableName, Collections.singletonList(transaction)).get(0).getWriteId();
            client.abortTransaction(transaction);

            String deltaA = tableLocation + "/delta_0000001_0000001_0000";
            String deltaB = tableLocation + "/delta_0000002_0000002_0000";
            String deltaC = tableLocation + "/delta_0000003_0000003_0000";

            // Delete original `delta-B`, `delta-C`
            hdfsDeleteAll(deltaB);
            hdfsDeleteAll(deltaC);

            // Copy content of `delta-A` to `delta-B`
            hdfsCopyAll(deltaA, deltaB);

            // Verify that data from delta-A and delta-B is visible
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));

            // Copy content of `delta-A` to `delta-C` (which is an aborted transaction)
            hdfsCopyAll(deltaA, deltaC);

            // Verify that delta, corresponding to aborted transaction, is not getting read
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));
        }
        finally {
            client.close();
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    private void hdfsDeleteAll(String directory)
    {
        if (!hdfsClient.exist(directory)) {
            return;
        }
        for (String file : hdfsClient.listDirectory(directory)) {
            hdfsClient.delete(directory + "/" + file);
        }
    }

    private void hdfsCopyAll(String source, String target)
    {
        if (!hdfsClient.exist(target)) {
            hdfsClient.createDirectory(target);
        }
        for (String file : hdfsClient.listDirectory(source)) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            hdfsClient.loadFile(source + "/" + file, bos);
            hdfsClient.saveFile(target + "/" + file, new ByteArrayInputStream(bos.toByteArray()));
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
                .run(() -> tryCompactingTable(compactMode, tableName, partitionString, Duration.valueOf("2m")));
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

    private String makeInsertValues(int col1Value, int col2First, int col2Last)
    {
        checkArgument(col2First <= col2Last, "The first value %s must be less or equal to the last %s", col2First, col2Last);
        return IntStream.rangeClosed(col2First, col2Last).mapToObj(i -> format("(%s, %s)", col1Value, i)).collect(Collectors.joining(", "));
    }

    private void ensureTransactionalHive()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }
    }

    private void verifySelectForPrestoAndHive(String select, String whereClause, QueryAssert.Row... rows)
    {
        verifySelect(onPresto(), "selecting on Presto", select, whereClause, rows);
        verifySelect(onHive(), "selecting on Hive", select, whereClause, rows);
    }

    private void verifySelect(QueryExecutor executor, String description, String select, String whereClause, QueryAssert.Row... rows)
    {
        String fullQuery = format("%s WHERE %s", select, whereClause);

        assertThat(executor.executeQuery(fullQuery))
                .containsOnly(rows);
    }
}
