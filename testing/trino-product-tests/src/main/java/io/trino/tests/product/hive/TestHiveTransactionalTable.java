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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.hive.util.TemporaryHiveTable;
import org.assertj.core.api.Assertions;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_TRANSACTIONAL;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_V2;
import static io.trino.tests.product.hive.BucketingType.NONE;
import static io.trino.tests.product.hive.TestHiveTransactionalTable.CompactionMode.MAJOR;
import static io.trino.tests.product.hive.TestHiveTransactionalTable.CompactionMode.MINOR;
import static io.trino.tests.product.hive.TransactionalTableType.ACID;
import static io.trino.tests.product.hive.TransactionalTableType.INSERT_ONLY;
import static io.trino.tests.product.hive.util.TableLocationUtils.getTablePath;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHiveTransactionalTable.class);

    public static final int TEST_TIMEOUT = 15 * 60 * 1000;

    // Hive original file path end looks like /000000_0
    // New Trino original file path end looks like /000000_132574635756428963553891918669625313402
    // Older Trino path ends look like /20210416_190616_00000_fsymd_af6f0a3d-5449-4478-a53d-9f9f99c07ed9
    private static final Pattern ORIGINAL_FILE_MATCHER = Pattern.compile(".*/\\d+_\\d+(_[^/]+)?$");

    @Inject
    private TestHiveMetastoreClientFactory testHiveMetastoreClientFactory;

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadFullAcid()
    {
        doTestReadFullAcid(false, BucketingType.NONE);
    }

    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
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
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadFullAcidBucketedV1()
    {
        doTestReadFullAcid(false, BucketingType.BUCKETED_V1);
    }

    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
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
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(21, 1), row(22, 2));

            // test filtering
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (24, 4)");
            onHive().executeQuery("DELETE FROM " + tableName + " where fcol=4");

            // test filtering
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // test minor compacted data read
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            compactTableAndWait(MINOR, tableName, hivePartitionString, new Duration(6, MINUTES));
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(21, 1), row(22, 2));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(21, 1));

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(23, 1));

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            // test major compaction
            compactTableAndWait(MAJOR, tableName, hivePartitionString, new Duration(6, MINUTES));
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(23, 1));
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadInsertOnlyOrc(boolean isPartitioned, BucketingType bucketingType)
    {
        testReadInsertOnly(isPartitioned, bucketingType, "STORED AS ORC");
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "partitioningAndBucketingTypeSmokeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadInsertOnlyParquet(boolean isPartitioned, BucketingType bucketingType)
    {
        testReadInsertOnly(isPartitioned, bucketingType, "STORED AS PARQUET");
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "partitioningAndBucketingTypeSmokeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadInsertOnlyText(boolean isPartitioned, BucketingType bucketingType)
    {
        testReadInsertOnly(isPartitioned, bucketingType, "STORED AS TEXTFILE");
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadInsertOnlyTextWithCustomFormatProperties()
    {
        testReadInsertOnly(
                false,
                NONE,
                "  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
                        "  WITH SERDEPROPERTIES ('field.delim'=',', 'line.delim'='\\n', 'serialization.format'=',') " +
                        "  STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
                        "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
    }

    private void testReadInsertOnly(boolean isPartitioned, BucketingType bucketingType, String hiveTableFormatDefinition)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName("insert_only", isPartitioned, bucketingType))) {
            String tableName = table.getName();

            onHive().executeQuery("CREATE TABLE " + tableName + " (col INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("col", 4) + " " +
                    hiveTableFormatDefinition + " " +
                    hiveTableProperties(INSERT_ONLY, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 1");
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsOnly(row(1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 2");
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(1), row(2));

            assertThat(onTrino().executeQuery("SELECT col FROM " + tableName + " WHERE col=2")).containsExactlyInOrder(row(2));

            // test minor compacted data read
            compactTableAndWait(MINOR, tableName, hivePartitionString, new Duration(6, MINUTES));
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsExactlyInOrder(row(1), row(2));
            assertThat(onTrino().executeQuery("SELECT col FROM " + tableName + " WHERE col=2")).containsExactlyInOrder(row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsOnly(row(3));

            if (getHiveVersionMajor() >= 4) {
                // Major compaction on insert only table does not work prior to Hive 4:
                // https://issues.apache.org/jira/browse/HIVE-21280

                // test major compaction
                onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 4");
                compactTableAndWait(MAJOR, tableName, hivePartitionString, new Duration(6, MINUTES));
                assertThat(onTrino().executeQuery(selectFromOnePartitionsSql)).containsOnly(row(3), row(4));
            }
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadFullAcidWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Trino Hive transactional tables are supported with Hive version 3 or above");
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

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(tableName, "WHERE col = 21");

            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            // read with original files and insert delta
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1), row(22, 2));

            // read with original files and delete delta
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol = 2");
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1));

            // read with original files and insert+delete delta (UPDATE)
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE fcol = 1" + (isPartitioned ? " AND part_col = 2 " : ""));
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(23, 1));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testUpdateFullAcidWithOriginalFilesTrinoInserting(boolean isPartitioned, BucketingType bucketingType)
    {
        withTemporaryTable("trino_update_full_acid_acid_converted_table_read", true, isPartitioned, bucketingType, tableName -> {
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

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(tableName, "WHERE col = 21");

            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            if (isPartitioned) {
                onTrino().executeQuery("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 4, 2)");
            }
            else {
                onTrino().executeQuery("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 4)");
            }

            // read with original files and insert delta
            if (isPartitioned) {
                onTrino().executeQuery("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 3, 2)");
            }
            else {
                onTrino().executeQuery("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 3)");
            }

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1), row(22, 2));

            // read with original files and delete delta
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol = 2");

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1));
        });
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeleting(boolean isPartitioned, BucketingType bucketingType)
    {
        withTemporaryTable("trino_update_full_acid_acid_converted_table_read", true, isPartitioned, bucketingType, tableName -> {
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

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(tableName, "WHERE col = 10");

            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE col < 12")).containsOnly(row(10, 100), row(11, 110));

            String fields = isPartitioned ? "(col, fcol, part_col)" : "(col, fcol)";
            onTrino().executeQuery(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(30, 5, 2, isPartitioned, 3)));
            onTrino().executeQuery(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(40, 5, 2, isPartitioned, 3)));

            onTrino().executeQuery("DELETE FROM " + tableName + " WHERE col IN (11, 12)");
            onTrino().executeQuery("DELETE FROM " + tableName + " WHERE col IN (16, 17)");
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE fcol >= 100")).containsOnly(row(10, 100), row(13, 130), row(14, 140), row(15, 150), row(18, 180), row(19, 190));

            // read with original files and delete delta
            onTrino().executeQuery("DELETE FROM " + tableName + " WHERE col = 18 OR col = 14 OR (fcol = 2 AND (col / 2) * 2 = col)");

            assertThat(onHive().executeQuery("SELECT col, fcol FROM " + tableName))
                    .containsOnly(row(10, 100), row(13, 130), row(15, 150), row(19, 190), row(31, 2), row(33, 2), row(41, 2), row(43, 2));

            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName))
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
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4927", match = "Hive table .* is corrupt. Found sub-directory in bucket directory for partition")
    public void testReadInsertOnlyWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Trino Hive transactional tables are supported with Hive version 3 or above");
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

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(tableName, "WHERE col = 1");

            onHive().executeQuery("ALTER TABLE " + tableName + " SET " + hiveTableProperties(INSERT_ONLY, bucketingType));

            // read with original files
            assertThat(onTrino().executeQuery("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2));

            // read with original files and delta
            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (3)");
            assertThat(onTrino().executeQuery("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2), row(3));
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

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable("test_fail_acid_before_hive3_" + randomNameSuffix())) {
            String tableName = table.getName();
            onHive().executeQuery("" +
                    "CREATE TABLE " + tableName + "(a bigint) " +
                    "CLUSTERED BY(a) INTO 4 BUCKETS " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='true')");

            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM " + tableName))
                    .hasMessageContaining("Failed to open transaction. Transactional tables support requires Hive metastore version at least 3.0");
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

    @DataProvider
    public Object[][] partitioningAndBucketingTypeSmokeDataProvider()
    {
        return new Object[][] {
                {false, BucketingType.NONE},
                {true, BucketingType.BUCKETED_DEFAULT},
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "testCreateAcidTableDataProvider")
    public void testCtasAcidTable(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(format("ctas_transactional_%s", randomNameSuffix()))) {
            String tableName = table.getName();
            onTrino().executeQuery("CREATE TABLE " + tableName + " " +
                    trinoTableProperties(ACID, isPartitioned, bucketingType) +
                    " AS SELECT * FROM (VALUES (21, 1, 1), (22, 1, 2), (23, 2, 2)) t(col, fcol, partcol)");

            // can we query from Trino
            assertThat(onTrino().executeQuery("SELECT col, fcol FROM " + tableName + " WHERE partcol = 2 ORDER BY col"))
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
            onTrino().executeQuery("CREATE TABLE " + tableName + " (col INTEGER, fcol INTEGER, partcol INTEGER)" +
                    trinoTableProperties(ACID, isPartitioned, bucketingType));

            onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 2, 3)");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(row(1, 2, 3));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "acidFormatColumnNames")
    public void testAcidTableColumnNameConflict(String columnName)
    {
        withTemporaryTable("acid_column_name_conflict", true, true, NONE, tableName -> {
            onHive().executeQuery("CREATE TABLE " + tableName + " (`" + columnName + "` INTEGER, fcol INTEGER, partcol INTEGER) STORED AS ORC " + hiveTableProperties(ACID, NONE));
            onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 2, 3)");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).containsOnly(row(1, 2, 3));
        });
    }

    @DataProvider
    public Object[][] acidFormatColumnNames()
    {
        return new Object[][] {
                {"operation"},
                {"originalTransaction"},
                {"bucket"},
                {"rowId"},
                {"row"},
                {"currentTransaction"},
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testSimpleUnpartitionedTransactionalInsert()
    {
        withTemporaryTable("unpartitioned_transactional_insert", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));

            String insertQuery = format("INSERT INTO %s VALUES (11, 100), (12, 200), (13, 300)", tableName);

            // ensure that we treat ACID tables as implicitly bucketed on INSERT
            String explainOutput = (String) onTrino().executeQuery("EXPLAIN " + insertQuery).getOnlyValue();
            Assertions.assertThat(explainOutput).contains("Output partitioning: hive:HivePartitioningHandle{buckets=1");

            onTrino().executeQuery(insertQuery);

            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(11, 100L), row(12, 200L), row(13, 300L));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (14, 400), (15, 500), (16, 600)", tableName));

            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(11, 100L), row(12, 200L), row(13, 300L), row(14, 400L), row(15, 500L), row(16, 600L));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testTransactionalPartitionInsert()
    {
        withTemporaryTable("transactional_partition_insert", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));

            onTrino().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s WHERE column1 > 10", tableName), row(20));

            onTrino().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 30),
                    makeInsertValues(2, 21, 30)));

            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(20));

            onHive().executeQuery(format("DELETE FROM %s WHERE column1 > 15 AND column1 <= 25", tableName));

            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(0));

            onTrino().executeQuery(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 20, 23),
                    makeInsertValues(2, 20, 23)));

            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(8));
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

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive(format("SELECT customer FROM %s WHERE purchase = 'corn'", tableName), row("Debby"), row("Joe"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testTransactionalUnpartitionedDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("unpartitioned_delete", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INTEGER, column2 BIGINT) WITH (format = 'ORC', transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column2 = 100", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(2, 200), row(3, 300), row(4, 400), row(5, 500));

            execute(inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700)", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column1 = 4", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(2, 200), row(3, 300), row(5, 500), row(6, 600), row(7, 700));

            execute(deleter, format("DELETE FROM %s WHERE column1 <= 3 OR column1 = 6", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(5, 500), row(7, 700));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testMultiDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("unpartitioned_multi_delete", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000)", tableName));

            execute(deleter, format("DELETE FROM %s WHERE column1 = 9", tableName));
            execute(deleter, format("DELETE FROM %s WHERE column1 = 2 OR column1 = 3", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 100), row(4, 400), row(5, 500), row(6, 600), row(7, 700), row(8, 800), row(10, 1000));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testReadAfterMultiInsertAndDelete()
    {
        // Test reading from a table after Hive multi-insert. Multi-insert involves non-zero statement ID, encoded
        // within ORC-level bucketId field, while originalTransactionId and rowId can be the same for two different rows.
        //
        // This is a regression test to verify that Trino correctly takes into account the bucketId field, including encoded
        // statement id, when filtering out deleted rows.
        //
        // For more context see https://issues.apache.org/jira/browse/HIVE-16832
        withTemporaryTable("partitioned_multi_insert", true, true, BucketingType.BUCKETED_V1, tableName -> {
            withTemporaryTable("tmp_data_table", false, false, NONE, dataTableName -> {
                onTrino().executeQuery(format("CREATE TABLE %s (a int, b int, c varchar(5)) WITH " +
                        "(transactional = true, partitioned_by = ARRAY['c'], bucketed_by = ARRAY['a'], bucket_count = 2)", tableName));
                onTrino().executeQuery(format("CREATE TABLE %s (x int)", dataTableName));
                onTrino().executeQuery(format("INSERT INTO %s VALUES 1", dataTableName));

                // Perform a multi-insert
                onHive().executeQuery("SET hive.exec.dynamic.partition.mode = nonstrict");
                // Combine dynamic and static partitioning to trick Hive to insert two rows with same rowId but different statementId to a single partition.
                onHive().executeQuery(format("FROM %s INSERT INTO %s partition(c) SELECT 0, 0, 'c' || x INSERT INTO %2$s partition(c='c1') SELECT 0, 1",
                        dataTableName,
                        tableName));
                onHive().executeQuery(format("DELETE FROM %s WHERE b = 1", tableName));
                verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(0, 0, "c1"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testTransactionalMetadataDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("metadata_delete", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
            execute(inserter, format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            execute(deleter, format("DELETE FROM %s WHERE column2 = 1", tableName));
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM %s WHERE column2 = 1".formatted(tableName), row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testNonTransactionalMetadataDelete()
    {
        withTemporaryTable("non_transactional_metadata_delete", false, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column2 BIGINT, column1 INT) WITH (partitioned_by = ARRAY['column1'])", tableName));

            execute(Engine.TRINO, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 10),
                    makeInsertValues(2, 1, 10)));

            execute(Engine.TRINO, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 11, 20),
                    makeInsertValues(2, 11, 20)));

            execute(Engine.TRINO, format("DELETE FROM %s WHERE column1 = 1", tableName));
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM %s WHERE column1 = 1".formatted(tableName), row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testUnpartitionedDeleteAll(Engine inserter, Engine deleter)
    {
        withTemporaryTable("unpartitioned_delete_all", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(deleter, "DELETE FROM " + tableName);
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM " + tableName, row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testMultiColumnDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("multi_column_delete", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            String where = " WHERE column1 >= 2 AND column2 <= 400";
            execute(deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForTrinoAndHive("SELECT * FROM %s WHERE column1 IN (1, 5)".formatted(tableName), row(1, 100), row(5, 500));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testPartitionAndRowsDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("partition_and_rows_delete", true, true, NONE, tableName -> {
            onTrino().executeQuery("CREATE TABLE " + tableName +
                    " (column2 BIGINT, column1 INT) WITH (transactional = true, partitioned_by = ARRAY['column1'])");
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (1, 200), (2, 300), (2, 400), (2, 500)", tableName));
            String where = " WHERE column1 = 2 OR column2 = 200";
            execute(deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForTrinoAndHive("SELECT column1, column2 FROM " + tableName, row(1, 100));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testPartitionedInsertAndRowLevelDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("partitioned_row_level_delete", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column2 INT, column1 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column1'])", tableName));

            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));
            execute(inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 40),
                    makeInsertValues(2, 21, 40)));

            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30".formatted(tableName), row(40));

            execute(deleter, format("DELETE FROM %s WHERE column2 > 10 AND column2 <= 30", tableName));
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30".formatted(tableName), row(0));
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM " + tableName, row(40));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testBucketedPartitionedDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("bucketed_partitioned_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive("SELECT customer FROM %s WHERE purchase = 'corn'".formatted(tableName), row("Debby"), row("Joe"));

            execute(deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForTrinoAndHive("SELECT purchase FROM %s WHERE customer = 'Ann'".formatted(tableName), row("cards"), row("cereal"), row("chips"));

            execute(deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForTrinoAndHive("SELECT customer, purchase FROM " + tableName, row("Fred", "limes"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteAllRowsInPartition()
    {
        withTemporaryTable("bucketed_partitioned_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards'), ('Fred', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')", tableName));

            log.info("About to delete");
            onTrino().executeQuery(format("DELETE FROM %s WHERE customer = 'Fred'", tableName));

            log.info("About to verify");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row("lemons", "Ann"), row("chips", "Ann"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteAfterDelete()
    {
        withTemporaryTable("delete_after_delete", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id INT) WITH (transactional = true)", tableName));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (1), (2), (3)", tableName));

            onTrino().executeQuery(format("DELETE FROM %s WHERE id = 2", tableName));

            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1), row(3));

            onTrino().executeQuery("DELETE FROM " + tableName);

            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + tableName)).containsOnly(row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteAfterDeleteWithPredicate()
    {
        withTemporaryTable("delete_after_delete_predicate", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id INT) WITH (transactional = true)", tableName));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (1), (2), (3)", tableName));

            onTrino().executeQuery(format("DELETE FROM %s WHERE id = 2", tableName));

            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1), row(3));

            // A predicate sufficient to fool statistics-based optimization
            onTrino().executeQuery(format("DELETE FROM %s WHERE id != 2", tableName));

            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + tableName)).containsOnly(row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testBucketedUnpartitionedDelete(Engine inserter, Engine deleter)
    {
        withTemporaryTable("bucketed_unpartitioned_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive("SELECT customer FROM %s WHERE purchase = 'corn'".formatted(tableName), row("Debby"), row("Joe"));

            execute(deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForTrinoAndHive("SELECT purchase FROM %s WHERE customer = 'Ann'".formatted(tableName), row("cards"), row("cereal"), row("chips"));

            execute(deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForTrinoAndHive("SELECT customer, purchase FROM " + tableName, row("Fred", "limes"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteOverManySplits()
    {
        withTemporaryTable("delete_select", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf10.orders", tableName));

            log.info("About to delete selected rows");
            onTrino().executeQuery(format("DELETE FROM %s WHERE clerk = 'Clerk#000004942'", tableName));

            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM %s WHERE clerk = 'Clerk#000004942'".formatted(tableName), row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "inserterAndDeleterProvider", timeOut = TEST_TIMEOUT)
    public void testCorrectSelectCountStar(Engine inserter, Engine deleter)
    {
        withTemporaryTable("select_count_star_delete", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", tableName));

            execute(inserter, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c'), (4, 400, 'a'), (5, 500, 'b'), (6, 600, 'c')", tableName));
            execute(deleter, format("DELETE FROM %s WHERE col2 = 200", tableName));
            verifySelectForTrinoAndHive("SELECT COUNT(*) FROM " + tableName, row(5));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "insertersProvider", timeOut = TEST_TIMEOUT)
    public void testInsertOnlyMultipleWriters(boolean bucketed, Engine inserter1, Engine inserter2)
    {
        log.info("testInsertOnlyMultipleWriters bucketed %s, inserter1 %s, inserter2 %s", bucketed, inserter1, inserter2);
        withTemporaryTable("insert_only_partitioned", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) %s STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
                    tableName, bucketed ? "CLUSTERED BY (col2) INTO 3 BUCKETS" : ""));

            execute(inserter1, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 100, "a"), row(2, 200, "b"));

            execute(inserter2, format("INSERT INTO %s VALUES (3, 300, 'c'), (4, 400, 'a')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"));

            execute(inserter1, format("INSERT INTO %s VALUES (5, 500, 'b'), (6, 600, 'c')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"));
            verifySelectForTrinoAndHive("SELECT * FROM %s WHERE col2 > 300".formatted(tableName), row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"));

            execute(inserter2, format("INSERT INTO %s VALUES (7, 700, 'b'), (8, 800, 'c')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c"), row(4, 400, "a"), row(5, 500, "b"), row(6, 600, "c"), row(7, 700, "b"), row(8, 800, "c"));
            verifySelectForTrinoAndHive("SELECT * FROM %s WHERE col3 = 'c'".formatted(tableName), row(3, 300, "c"), row(6, 600, "c"), row(8, 800, "c"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testInsertFailsInExplicitTrinoTransaction()
    {
        withTemporaryTable("insert_fail_explicit_transaction", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            onTrino().executeQuery("START TRANSACTION");
            assertQueryFailure(() -> onTrino().executeQuery(format("INSERT INTO %s (a_string) VALUES ('Commander Bun Bun')", tableName)))
                    .hasMessageContaining("Inserting into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testUpdateFailsInExplicitTrinoTransaction()
    {
        withTemporaryTable("update_fail_explicit_transaction", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            onTrino().executeQuery("START TRANSACTION");
            assertQueryFailure(() -> onTrino().executeQuery(format("UPDATE %s SET a_string = 'Commander Bun Bun'", tableName)))
                    .hasMessageContaining("Merging into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testDeleteFailsInExplicitTrinoTransaction()
    {
        withTemporaryTable("delete_fail_explicit_transaction", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            onTrino().executeQuery("START TRANSACTION");
            assertQueryFailure(() -> onTrino().executeQuery(format("DELETE FROM %s WHERE a_string = 'Commander Bun Bun'", tableName)))
                    .hasMessageContaining("Merging into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "transactionModeProvider")
    public void testColumnRenamesOrcPartitioned(boolean transactional)
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_column_renames_partitioned", transactional, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR)" +
                    " WITH (format = 'ORC', transactional = %s, partitioned_by = ARRAY['old_state'])", tableName, transactional));
            testOrcColumnRenames(tableName);

            log.info("About to rename partition column old_state to new_state");
            assertQueryFailure(() -> onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN old_state TO new_state", tableName)))
                    .hasMessageContaining("Renaming partition columns is not supported");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "transactionModeProvider")
    public void testColumnRenamesOrcNotPartitioned(boolean transactional)
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_orc_column_renames_not_partitioned", transactional, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR)" +
                    " WITH (format = 'ORC', transactional = %s)", tableName, transactional));
            testOrcColumnRenames(tableName);
        });
    }

    private void testOrcColumnRenames(String tableName)
    {
        onTrino().executeQuery(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
        verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));

        onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a single rename");
        verifySelectForTrinoAndHive("SELECT age FROM %s WHERE new_name = 'Katy'".formatted(tableName), row(57));

        onTrino().executeQuery(format("INSERT INTO %s VALUES(333, 'Joan', 23, 'OR')", tableName));
        verifySelectForTrinoAndHive("SELECT age FROM %s WHERE new_name != 'Joe'".formatted(tableName), row(57), row(23));

        onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN new_name TO newer_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a double rename");
        verifySelectForTrinoAndHive("SELECT age FROM %s WHERE newer_name = 'Katy'".formatted(tableName), row(57));

        onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN newer_name TO old_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a rename back to the original name");
        verifySelectForTrinoAndHive("SELECT age FROM %s WHERE old_name = 'Katy'".formatted(tableName), row(57));
        verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"), row(333, "Joan", 23, "OR"));
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "transactionModeProvider")
    public void testOrcColumnSwap(boolean transactional)
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_orc_column_renames", transactional, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (name VARCHAR, state VARCHAR) WITH (format = 'ORC', transactional = %s)", tableName, transactional));
            onTrino().executeQuery(format("INSERT INTO %s VALUES ('Katy', 'CA'), ('Joe', 'WA')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row("Katy", "CA"), row("Joe", "WA"));

            onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN name TO new_name", tableName));
            onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN state TO name", tableName));
            onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN new_name TO state", tableName));
            log.info("This shows that Trino and Hive can still query old data, but because of the renames, columns are swapped!");
            verifySelectForTrinoAndHive("SELECT state, name FROM " + tableName, row("Katy", "CA"), row("Joe", "WA"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testBehaviorOnParquetColumnRenames()
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_parquet_column_renames", false, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR) WITH (format = 'PARQUET', transactional = false)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));

            onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (333, 'Fineas', 31, 'OR')", tableName));

            log.info("This shows that Hive and Trino do not see old data after a rename");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, null, 57, "CA"), row(222, null, 72, "WA"), row(333, "Fineas", 31, "OR"));

            onTrino().executeQuery(format("ALTER TABLE %s RENAME COLUMN new_name TO old_name", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (444, 'Gladys', 47, 'WA')", tableName));
            log.info("This shows that Trino and Hive both see data in old data files after renaming back to the original column name");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"), row(333, null, 31, "OR"), row(444, "Gladys", 47, "WA"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "transactionModeProvider")
    public void testOrcColumnDropAdd(boolean transactional)
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_orc_add_drop", transactional, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR) WITH (transactional = %s)", tableName, transactional));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));

            onTrino().executeQuery(format("ALTER TABLE %s DROP COLUMN old_state", tableName));
            log.info("This shows that neither Trino nor Hive see the old data after a column is dropped");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57), row(222, "Joe", 72));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (333, 'Kelly', 45)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57), row(222, "Joe", 72), row(333, "Kelly", 45));

            onTrino().executeQuery(format("ALTER TABLE %s ADD COLUMN new_state VARCHAR", tableName));
            log.info("This shows that for ORC, Trino and Hive both see data inserted into a dropped column when a column of the same type but different name is added");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"), row(333, "Kelly", 45, null));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, dataProvider = "transactionModeProvider")
    public void testOrcColumnTypeChange(boolean transactional)
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_orc_column_type_change", transactional, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id INT, old_name VARCHAR, age TINYINT, old_state VARCHAR) WITH (transactional = %s)", tableName, transactional));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));

            onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN age age INT", tableName));
            log.info("This shows that Hive see the old data after a column is widened");
            assertThat(onHive().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));
            log.info("This shows that Trino gets an exception trying to widen the type");
            assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM " + tableName))
                    .hasMessageMatching(".*Malformed ORC file. Cannot read SQL type 'integer' from ORC stream '.*.age' of type BYTE with attributes.*");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testParquetColumnDropAdd()
    {
        ensureSchemaEvolutionSupported();
        withTemporaryTable("test_parquet_add_drop", false, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, state VARCHAR) WITH (format = 'PARQUET')", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));

            onTrino().executeQuery(format("ALTER TABLE %s DROP COLUMN state", tableName));
            log.info("This shows that neither Trino nor Hive see the old data after a column is dropped");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57), row(222, "Joe", 72));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (333, 'Kelly', 45)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57), row(222, "Joe", 72), row(333, "Kelly", 45));

            onTrino().executeQuery(format("ALTER TABLE %s ADD COLUMN state VARCHAR", tableName));
            log.info("This shows that for Parquet, Trino and Hive both see data inserted into a dropped column when a column of the same name and type is added");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"), row(333, "Kelly", 45, null));

            onTrino().executeQuery(format("ALTER TABLE %s DROP COLUMN state", tableName));
            onTrino().executeQuery(format("ALTER TABLE %s ADD COLUMN new_state VARCHAR", tableName));

            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(111, "Katy", 57, null), row(222, "Joe", 72, null), row(333, "Kelly", 45, null));
        });
    }

    @DataProvider
    public Object[][] transactionModeProvider()
    {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateFailNonTransactional()
    {
        withTemporaryTable("update_fail_nontransactional", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail update");
            assertQueryFailure(() -> onTrino().executeQuery(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateFailInsertOnlyTable()
    {
        withTemporaryTable("update_fail_insert_only", true, false, NONE, tableName -> {
            onHive().executeQuery("CREATE TABLE " + tableName + " (customer STRING, purchase STRING) " +
                    "STORED AS ORC " +
                    hiveTableProperties(INSERT_ONLY, NONE));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail update");
            assertQueryFailure(() -> onTrino().executeQuery(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidDeleteFailNonTransactional()
    {
        withTemporaryTable("delete_fail_nontransactional", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail delete");
            assertQueryFailure(() -> onTrino().executeQuery(format("DELETE FROM %s WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidDeleteFailInsertOnlyTable()
    {
        withTemporaryTable("delete_fail_insert_only", true, false, NONE, tableName -> {
            onHive().executeQuery("CREATE TABLE " + tableName + " (customer STRING, purchase STRING) " +
                    "STORED AS ORC " +
                    hiveTableProperties(INSERT_ONLY, NONE));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail delete");
            assertQueryFailure(() -> onTrino().executeQuery(format("DELETE FROM %s WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSucceedUpdatingPartitionKey()
    {
        withTemporaryTable("fail_update_partition_key", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['col3'])", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3) VALUES (17, 'S1', 7)", tableName));

            log.info("About to succeed updating the partition key");
            onTrino().executeQuery(format("UPDATE %s SET col3 = 17 WHERE col3 = 7", tableName));

            log.info("Verify the update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(17, "S1", 17));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSucceedUpdatingBucketColumn()
    {
        withTemporaryTable("fail_update_bucket_column", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to succeed updating bucket column");
            onTrino().executeQuery(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName));

            log.info("Verifying update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row("Fred", "bread"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateFailOnIllegalCast()
    {
        withTemporaryTable("fail_update_on_illegal_cast", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true)", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3) VALUES (17, 'S1', 7)", tableName));

            log.info("About to fail update");
            assertQueryFailure(() -> onTrino().executeQuery(format("UPDATE %s SET col1 = col2 WHERE col3 = 7", tableName)))
                    .hasMessageContaining("UPDATE table column types don't match SET expressions: Table: [integer], Expressions: [varchar]");
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSimple()
    {
        withTemporaryTable("acid_update_simple", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (7, 'ONE', 1000, true, 101), (13, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col2 = 'DEUX', col3 = col3 + 20 + col1 + col5 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(7, "ONE", 1000, true, 101), row(13, "DEUX", 2235, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSelectedValues()
    {
        withTemporaryTable("acid_update_simple_selected", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (7, 'ONE', 1000, true, 101), (13, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update %s", tableName);
            onTrino().executeQuery(format("UPDATE %s SET col2 = 'DEUX', col3 = col3 + 20 + col1 + col5 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(7, "ONE", 1000, true, 101), row(13, "DEUX", 2235, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateCopyColumn()
    {
        withTemporaryTable("acid_update_copy_column", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 int, col2 int, col3 VARCHAR) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3) VALUES (7, 15, 'ONE'), (13, 17, 'DEUX')", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col1 = col2 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(7, 15, "ONE"), row(17, 17, "DEUX"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSomeLiteralNullColumnValues()
    {
        withTemporaryTable("update_some_literal_null_columns", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to run first update");
            onTrino().executeQuery(format("UPDATE %s SET col2 = NULL, col3 = NULL WHERE col1 = 2", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "ONE", 1000, true, 101), row(2, null, null, false, 202));
            log.info("About to run second update");
            onTrino().executeQuery(format("UPDATE %s SET col1 = NULL, col2 = NULL, col3 = NULL, col4 = NULL WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(null, null, null, null, 101), row(2, null, null, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateSomeComputedNullColumnValues()
    {
        withTemporaryTable("update_some_computed_null_columns", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to run first update");
            // Use IF(RAND()<0, NULL) as a way to compute null
            onTrino().executeQuery(format("UPDATE %s SET col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL) WHERE col1 = 2", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "ONE", 1000, true, 101), row(2, null, null, false, 202));
            log.info("About to run second update");
            onTrino().executeQuery(format("UPDATE %s SET col1 = IF(RAND()<0, NULL), col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL), col4 = IF(RAND()<0, NULL) WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(null, null, null, null, 101), row(2, null, null, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateAllLiteralNullColumnValues()
    {
        withTemporaryTable("update_all_literal_null_columns", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col1 = NULL, col2 = NULL, col3 = NULL, col4 = NULL, col5 = null WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(null, null, null, null, null), row(2, "TWO", 2000, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateAllComputedNullColumnValues()
    {
        withTemporaryTable("update_all_computed_null_columns", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            // Use IF(RAND()<0, NULL) as a way to compute null
            onTrino().executeQuery(format("UPDATE %s SET col1 = IF(RAND()<0, NULL), col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL), col4 = IF(RAND()<0, NULL), col5 = IF(RAND()<0, NULL) WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(null, null, null, null, null), row(2, "TWO", 2000, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateReversed()
    {
        withTemporaryTable("update_reversed", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col3 = col3 + 20 + col1 + col5, col1 = 3, col2 = 'DEUX' WHERE col1 = 2", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "ONE", 1000, true, 101), row(3, "DEUX", 2224, false, 202));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdatePermuted()
    {
        withTemporaryTable("update_permuted", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col5 = 303, col1 = 3, col3 = col3 + 20 + col1 + col5, col4 = true, col2 = 'DUO' WHERE col1 = 2", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "ONE", 1000, true, 101), row(3, "DUO", 2224, true, 303));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateAllColumnsSetAndDependencies()
    {
        withTemporaryTable("update_all_columns_set", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 INT, col3 BIGINT, col4 INT, col5 TINYINT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 2, 3, 4, 5), (21, 22, 23, 24, 25)", tableName));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 21", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(23, 21, 22, 25, 24));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdatePartitioned()
    {
        withTemporaryTable("update_partitioned", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['col3'])", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3) VALUES (13, 'T1', 3), (23, 'T2', 3), (17, 'S1', 7)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(13, "T1", 3), row(23, "T2", 3), row(17, "S1", 7));

            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET col1 = col1 + 1 WHERE col3 = 3 AND col1 > 15", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(13, "T1", 3), row(24, "T2", 3), row(17, "S1", 7));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateBucketed()
    {
        withTemporaryTable("update_bucketed", true, true, NONE, tableName -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards'), ('Fred', 'limes'), ('Ann', 'lemons')", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row("Fred", "cards"), row("Fred", "limes"), row("Ann", "lemons"));

            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Ann'", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row("Fred", "cards"), row("Fred", "limes"), row("Ann", "bread"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateMajorCompaction()
    {
        withTemporaryTable("schema_evolution_column_addition", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (11, 100)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (22, 200)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(11, 100L), row(22, 200L));
            log.info("About to compact");
            compactTableAndWait(MAJOR, tableName, "", new Duration(6, MINUTES));
            log.info("About to update");
            onTrino().executeQuery(format("UPDATE %s SET column1 = 33 WHERE column2 = 200", tableName));
            log.info("About to select");
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(11, 100L), row(33, 200L));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (44, 400), (55, 500)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(11, 100L), row(33, 200L), row(44, 400L), row(55, 500L));
            onTrino().executeQuery(format("DELETE FROM %s WHERE column2 IN (100, 500)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(33, 200L), row(44, 400L));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateWithSubqueryPredicate()
    {
        withTemporaryTable("test_update_subquery", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 varchar) WITH (transactional = true)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 'x')", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (2, 'y')", tableName));

            // WHERE with uncorrelated subquery
            onTrino().executeQuery(format("UPDATE %s SET column2 = 'row updated' WHERE column1 = (SELECT min(regionkey) + 1 FROM tpch.tiny.region)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "row updated"), row(2, "y"));

            withTemporaryTable("second_table", true, false, NONE, secondTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (regionkey bigint, name varchar(25), comment varchar(152)) WITH (transactional = true)", secondTable));
                onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM tpch.tiny.region", secondTable));

                // UPDATE while reading from another transactional table. Multiple transactional could interfere with ConnectorMetadata.beginQuery
                onTrino().executeQuery(format("UPDATE %s SET column2 = 'another row updated' WHERE column1 = (SELECT min(regionkey) + 2 FROM %s)", tableName, secondTable));
                verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "row updated"), row(2, "another row updated"));
            });

            // WHERE with correlated subquery
            onTrino().executeQuery(format("UPDATE %s SET column2 = 'row updated yet again' WHERE column2 = (SELECT name FROM tpch.tiny.region WHERE regionkey = column1)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "row updated"), row(2, "another row updated"));

            onTrino().executeQuery(format("UPDATE %s SET column2 = 'row updated yet again' WHERE column2 != (SELECT name FROM tpch.tiny.region WHERE regionkey = column1)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "row updated yet again"), row(2, "row updated yet again"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateWithSubqueryAssignment()
    {
        withTemporaryTable("test_update_subquery", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (column1 INT, column2 varchar) WITH (transactional = true)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 'x')", tableName));
            onTrino().executeQuery(format("INSERT INTO %s VALUES (2, 'y')", tableName));

            // SET with uncorrelated subquery
            onTrino().executeQuery(format("UPDATE %s SET column2 = (SELECT max(name) FROM tpch.tiny.region)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "MIDDLE EAST"), row(2, "MIDDLE EAST"));

            withTemporaryTable("second_table", true, false, NONE, secondTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (regionkey bigint, name varchar(25), comment varchar(152)) WITH (transactional = true)", secondTable));
                onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM tpch.tiny.region", secondTable));

                // UPDATE while reading from another transactional table. Multiple transactional could interfere with ConnectorMetadata.beginQuery
                onTrino().executeQuery(format("UPDATE %s SET column2 = (SELECT min(name) FROM %s)", tableName, secondTable));
                verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "AFRICA"), row(2, "AFRICA"));

                onTrino().executeQuery(format("UPDATE %s SET column2 = (SELECT name FROM %s WHERE column1 = regionkey + 1)", tableName, secondTable));
                verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "AFRICA"), row(2, "AMERICA"));
            });

            // SET with correlated subquery
            onTrino().executeQuery(format("UPDATE %s SET column2 = (SELECT name FROM tpch.tiny.region WHERE column1 = regionkey)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, "AMERICA"), row(2, "ASIA"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateDuplicateUpdateValue()
    {
        withTemporaryTable("test_update_bug", true, false, NONE, tableName -> {
            onTrino().executeQuery(
                    format("CREATE TABLE %s (", tableName) +
                            " yyyy integer," +
                            " week_number integer," +
                            " week_start_date date," +
                            " week_end_date date," +
                            " genre_summary_status smallint," +
                            " shop_summary_status smallint)" +
                            " WITH (transactional = true)");

            onTrino().executeQuery(format("INSERT INTO %s", tableName) +
                    "(yyyy, week_number, week_start_date, week_end_date, genre_summary_status, shop_summary_status) VALUES" +
                    "  (2021, 20,  DATE '2021-09-10', DATE '2021-09-10', 20, 20)" +
                    ", (2021, 30,  DATE '2021-09-09', DATE '2021-09-09', 30, 30)" +
                    ", (2021, 999, DATE '2018-12-24', DATE '2018-12-24', 999, 999)" +
                    ", (2021, 30,  DATE '2021-09-09', DATE '2021-09-10', 30, 30)");

            onTrino().executeQuery(format("UPDATE %s", tableName) +
                    " SET genre_summary_status = 1, shop_summary_status = 1" +
                    " WHERE week_start_date = DATE '2021-09-19' - (INTERVAL '08' DAY)" +
                    "    OR week_start_date = DATE '2021-09-19' - (INTERVAL '09' DAY)");

            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .contains(
                            row(2021, 20, Date.valueOf("2021-09-10"), Date.valueOf("2021-09-10"), 1, 1),
                            row(2021, 30, Date.valueOf("2021-09-09"), Date.valueOf("2021-09-09"), 30, 30),
                            row(2021, 999, Date.valueOf("2018-12-24"), Date.valueOf("2018-12-24"), 999, 999),
                            row(2021, 30, Date.valueOf("2021-09-09"), Date.valueOf("2021-09-10"), 30, 30));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testAcidUpdateMultipleDuplicateValues()
    {
        withTemporaryTable("test_update_multiple", true, false, NONE, tableName -> {
            onTrino().executeQuery(
                    format("CREATE TABLE %s (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int) WITH (transactional = true)", tableName));

            log.info("Inserting into table");
            onTrino().executeQuery(format("INSERT INTO %s (c1, c2, c3, c4, c5, c6) VALUES (1, 2, 3, 4, 5, 6)", tableName));

            log.info("Performing first update");
            onTrino().executeQuery(format("UPDATE %s SET c1 = 2, c2 = 4, c3 = 2, c4 = 4, c5 = 4, c6 = 2", tableName));

            log.info("Checking first results");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).contains(row(2, 4, 2, 4, 4, 2));

            log.info("Performing second update");
            onTrino().executeQuery(format("UPDATE %s SET c1 = c1 + c2, c2 = c3 + c4, c3 = c1 + c2, c4 = c4 + c3, c5 = c3 + c4, c6 = c4 + c3", tableName));

            log.info("Checking second results");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName)).contains(row(6, 6, 6, 6, 6, 6));

            log.info("Finished");
        });
    }

    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testInsertDeleteUpdateWithTrinoAndHive()
    {
        withTemporaryTable("update_insert_delete_trino_hive", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 INT, col3 BIGINT, col4 INT, col5 TINYINT) WITH (transactional = true)", tableName));

            log.info("Performing first insert on Trino");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 2, 3, 4, 5), (21, 22, 23, 24, 25)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(21, 22, 23, 24, 25));

            log.info("Performing first update on Trino");
            onTrino().executeQuery(format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 21", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(23, 21, 22, 25, 24));

            log.info("Performing second insert on Hive");
            onHive().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (31, 32, 33, 34, 35)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(23, 21, 22, 25, 24), row(31, 32, 33, 34, 35));

            log.info("Performing first delete on Trino");
            onTrino().executeQuery(format("DELETE FROM %s WHERE col1 = 23", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(31, 32, 33, 34, 35));

            log.info("Performing second update on Hive");
            onHive().executeQuery(format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 31", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(33, 31, 32, 35, 34));

            log.info("Performing more inserts on Trino");
            onTrino().executeQuery(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (41, 42, 43, 44, 45), (51, 52, 53, 54, 55)", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(1, 2, 3, 4, 5), row(33, 31, 32, 35, 34), row(41, 42, 43, 44, 45), row(51, 52, 53, 54, 55));

            log.info("Performing second delete on Hive");
            onHive().executeQuery(format("DELETE FROM %s WHERE col5 = 5", tableName));
            verifySelectForTrinoAndHive("SELECT * FROM " + tableName, row(33, 31, 32, 35, 34), row(41, 42, 43, 44, 45), row(51, 52, 53, 54, 55));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteFromOriginalFiles()
    {
        withTemporaryTable("delete_original_files", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive("SELECT count(*) FROM " + tableName, row(25));
            verifySelectForTrinoAndHive(format("SELECT nationkey, name FROM %s WHERE regionkey = 4", tableName), row(4, "EGYPT"), row(10, "IRAN"), row(11, "IRAQ"), row(13, "JORDAN"), row(20, "SAUDI ARABIA"));
            onTrino().executeQuery(format("DELETE FROM %s WHERE regionkey = 4 AND nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive("SELECT count(*) FROM " + tableName, row(24));
            verifySelectForTrinoAndHive(format("SELECT nationkey, name FROM %s WHERE regionkey = 4", tableName), row(4, "EGYPT"), row(10, "IRAN"), row(11, "IRAQ"), row(20, "SAUDI ARABIA"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteWholePartition()
    {
        testDeleteWholePartition(false);
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testDeleteWholePartitionWithOriginalFiles()
    {
        testDeleteWholePartition(true);
    }

    private void testDeleteWholePartition(boolean withOriginalFiles)
    {
        withTemporaryTable("delete_partitioned", true, true, NONE, tableName -> {
            if (withOriginalFiles) {
                onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                        " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
                verifyOriginalFiles(tableName, "WHERE regionkey = 4");
            }
            else {
                onTrino().executeQuery(format("CREATE TABLE %s (" +
                        "    nationkey bigint," +
                        "    name varchar(25)," +
                        "    regionkey bigint)" +
                        " WITH (transactional = true, partitioned_by = ARRAY['regionkey'])", tableName));
                onTrino().executeQuery(format("INSERT INTO %s SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            }

            verifySelectForTrinoAndHive("SELECT count(*) FROM " + tableName, row(25));

            // verify all partitions exist
            assertThat(onTrino().executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName)))
                    .containsOnly(row(0), row(1), row(2), row(3), row(4));

            // run delete and verify row count
            onTrino().executeQuery(format("DELETE FROM %s WHERE regionkey = 4", tableName));
            verifySelectForTrinoAndHive("SELECT count(*) FROM " + tableName, row(20));

            // verify all partitions still exist
            assertThat(onTrino().executeQuery(format("SELECT * FROM \"%s$partitions\"", tableName)))
                    .containsOnly(row(0), row(1), row(2), row(3), row(4));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testUpdateOriginalFilesPartitioned()
    {
        withTemporaryTable("update_original_files", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive("SELECT nationkey, name FROM %s WHERE regionkey = 4".formatted(tableName), row(4, "EGYPT"), row(10, "IRAN"), row(11, "IRAQ"), row(13, "JORDAN"), row(20, "SAUDI ARABIA"));
            onTrino().executeQuery(format("UPDATE %s SET nationkey = 100 WHERE regionkey = 4 AND nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive("SELECT nationkey, name FROM %s WHERE regionkey = 4".formatted(tableName), row(4, "EGYPT"), row(10, "IRAN"), row(11, "IRAQ"), row(100, "JORDAN"), row(20, "SAUDI ARABIA"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testUpdateOriginalFilesUnpartitioned()
    {
        withTemporaryTable("update_original_files", true, true, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true)" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive("SELECT nationkey, name, regionkey FROM %s WHERE nationkey %% 10 = 3".formatted(tableName), row(3, "CANADA", 1), row(13, "JORDAN", 4), row(23, "UNITED KINGDOM", 3));
            onTrino().executeQuery(format("UPDATE %s SET nationkey = nationkey + 100 WHERE nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive("SELECT nationkey, name, regionkey FROM %s WHERE nationkey %% 10 = 3".formatted(tableName), row(103, "CANADA", 1), row(113, "JORDAN", 4), row(123, "UNITED KINGDOM", 3));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testInsertRowIdCorrectness()
    {
        withTemporaryTable("test_insert_row_id_correctness", true, false, NONE, tableName -> {
            // We use tpch.tiny.supplier because it is the smallest table that
            // is written as multiple pages by the ORC writer. If it stops
            // being split into pages, this test won't detect issues arising
            // from row numbering across pages, which is its original purpose.
            onTrino().executeQuery(format(
                    "CREATE TABLE %s (" +
                            "  suppkey bigint," +
                            "  name varchar(25)," +
                            "  address varchar(40)," +
                            "  nationkey bigint," +
                            "  phone varchar(15)," +
                            "  acctbal double," +
                            "  comment varchar(101))" +
                            "WITH (transactional = true)", tableName));
            onTrino().executeQuery(format("INSERT INTO %s select * from tpch.tiny.supplier", tableName));

            int supplierRows = 100;
            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + tableName))
                    .containsOnly(row(supplierRows));

            String queryTarget = format(" FROM %s WHERE suppkey = 10", tableName);

            assertThat(onTrino().executeQuery("SELECT count(*)" + queryTarget))
                    .as("Only one matching row exists")
                    .containsOnly(row(1));
            assertThat(onTrino().executeQuery("DELETE" + queryTarget))
                    .as("Only one row is reported as deleted")
                    .containsOnly(row(1));

            assertThat(onTrino().executeQuery("SELECT count(*) FROM " + tableName))
                    .as("Only one row was actually deleted")
                    .containsOnly(row(supplierRows - 1));
        });
    }

    private void verifyOriginalFiles(String tableName, String whereClause)
    {
        QueryResult result = onTrino().executeQuery(format("SELECT DISTINCT \"$path\" FROM %s %s", tableName, whereClause));
        String path = (String) result.getOnlyValue();
        checkArgument(ORIGINAL_FILE_MATCHER.matcher(path).matches(), "Path should be original file path, but isn't, path: %s", path);
    }

    @DataProvider
    public Object[][] insertersProvider()
    {
        return new Object[][] {
                {false, Engine.HIVE, Engine.TRINO},
                {false, Engine.TRINO, Engine.TRINO},
                {true, Engine.HIVE, Engine.TRINO},
                {true, Engine.TRINO, Engine.TRINO},
        };
    }

    private static QueryResult execute(Engine engine, String sql, QueryExecutor.QueryParam... params)
    {
        return engine.queryExecutor().executeQuery(sql, params);
    }

    @DataProvider
    public Object[][] inserterAndDeleterProvider()
    {
        return new Object[][] {
                {Engine.HIVE, Engine.TRINO},
                {Engine.TRINO, Engine.TRINO},
                {Engine.TRINO, Engine.HIVE}
        };
    }

    void withTemporaryTable(String rootName, boolean transactional, boolean isPartitioned, BucketingType bucketingType, Consumer<String> testRunner)
    {
        if (transactional) {
            ensureTransactionalHive();
        }
        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName(rootName, isPartitioned, bucketingType) + randomNameSuffix())) {
            testRunner.accept(table.getName());
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    @Flaky(issue = "https://github.com/trinodb/trino/issues/5463", match = "Expected row count to be <4>, but was <6>")
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

        try (ThriftMetastoreClient client = testHiveMetastoreClientFactory.createMetastoreClient()) {
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + " ORDER BY COL";

            // Create `delta-A` file
            onHive().executeQuery("INSERT INTO TABLE " + tableName + " VALUES (1),(2)");
            QueryResult onePartitionQueryResult = onTrino().executeQuery(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactlyInOrder(row(1), row(2));

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
            onePartitionQueryResult = onTrino().executeQuery(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));

            // Copy content of `delta-A` to `delta-C` (which is an aborted transaction)
            hdfsCopyAll(deltaA, deltaC);

            // Verify that delta, corresponding to aborted transaction, is not getting read
            onePartitionQueryResult = onTrino().executeQuery(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testDoubleUpdateAndThenReadFromHive()
    {
        withTemporaryTable("test_double_update", true, false, NONE, tableName -> {
            onTrino().executeQuery(
                    "CREATE TABLE test_double_update ( " +
                            "column1 INT, " +
                            "column2 VARCHAR " +
                            ") " +
                            "WITH ( " +
                            "   transactional = true " +
                            ");");
            onTrino().executeQuery("INSERT INTO test_double_update VALUES(1, 'x')");
            onTrino().executeQuery("INSERT INTO test_double_update VALUES(2, 'y')");
            onTrino().executeQuery("UPDATE test_double_update SET column2 = 'xy1'");
            onTrino().executeQuery("UPDATE test_double_update SET column2 = 'xy2'");
            verifySelectForTrinoAndHive("SELECT * FROM test_double_update", row(1, "xy2"), row(2, "xy2"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testDeleteWithOriginalFiles()
    {
        withTemporaryTable("test_delete_with_original_files", true, false, NONE, tableName -> {
            // these 3 properties are necessary to make sure there is more than 1 original file created
            onTrino().executeQuery("SET SESSION scale_writers = true");
            onTrino().executeQuery("SET SESSION writer_min_size = '4kB'");
            onTrino().executeQuery("SET SESSION task_scale_writers_enabled = false");
            onTrino().executeQuery("SET SESSION task_writer_count = 2");
            onTrino().executeQuery(format(
                    "CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf1000.orders LIMIT 100000", tableName));

            verify(onTrino().executeQuery(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRowsCount() >= 2,
                    "There should be at least 2 files");
            validateFileIsDirectlyUnderTableLocation(tableName);

            onTrino().executeQuery(format("DELETE FROM %s", tableName));
            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s", tableName), row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testDeleteWithOriginalFilesWithWhereClause()
    {
        withTemporaryTable("test_delete_with_original_files_with_where_clause", true, false, NONE, tableName -> {
            // these 3 properties are necessary to make sure there is more than 1 original file created
            onTrino().executeQuery("SET SESSION scale_writers = true");
            onTrino().executeQuery("SET SESSION writer_min_size = '4kB'");
            onTrino().executeQuery("SET SESSION task_scale_writers_enabled = false");
            onTrino().executeQuery("SET SESSION task_writer_count = 2");
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf1000.orders LIMIT 100000", tableName));

            verify(onTrino().executeQuery(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRowsCount() >= 2,
                    "There should be at least 2 files");
            validateFileIsDirectlyUnderTableLocation(tableName);
            int sizeBeforeDeletion = onTrino().executeQuery(format("SELECT orderkey FROM %s", tableName)).rows().size();

            onTrino().executeQuery(format("DELETE FROM %s WHERE (orderkey %% 2) = 0", tableName));
            assertThat(onTrino().executeQuery(format("SELECT COUNT (orderkey) FROM %s WHERE orderkey %%2 = 0", tableName))).containsOnly(row(0));

            int sizeOnTrinoWithWhere = onTrino().executeQuery(format("SELECT orderkey FROM %s WHERE orderkey %%2 = 1", tableName)).rows().size();
            int sizeOnHiveWithWhere = onHive().executeQuery(format("SELECT orderkey FROM %s WHERE orderkey %%2 = 1", tableName)).rows().size();
            int sizeOnTrinoWithoutWhere = onTrino().executeQuery(format("SELECT orderkey FROM %s", tableName)).rows().size();

            verify(sizeOnHiveWithWhere == sizeOnTrinoWithWhere);
            verify(sizeOnTrinoWithWhere == sizeOnTrinoWithoutWhere);
            verify(sizeBeforeDeletion > sizeOnTrinoWithoutWhere);
        });
    }

    private void validateFileIsDirectlyUnderTableLocation(String tableName)
    {
        onTrino().executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName))
                .column(1)
                .forEach(path -> verify(path.toString().endsWith(tableName.toLowerCase(ENGLISH)),
                        "files in %s are not directly under table location", path));
    }

    @Test
    public void testDeleteAfterMajorCompaction()
    {
        withTemporaryTable("test_delete_after_major_compaction", true, false, NONE, tableName -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.tiny.nation", tableName));
            compactTableAndWait(MAJOR, tableName, "", new Duration(3, MINUTES));
            onTrino().executeQuery(format("DELETE FROM %s", tableName));
            verifySelectForTrinoAndHive(format("SELECT COUNT(*) FROM %s", tableName), row(0));
        });
    }

    @Test
    public void testUnbucketedPartitionedTransactionalTableWithTaskWriterCountGreaterThanOne()
    {
        unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(true);
    }

    @Test
    public void testUnbucketedTransactionalTableWithTaskWriterCountGreaterThanOne()
    {
        unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(false);
    }

    private void unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(boolean isPartitioned)
    {
        withTemporaryTable(format("test_unbucketed%s_transactional_table_with_task_writer_count_greater_than_one", isPartitioned ? "_partitioned" : ""), true, isPartitioned, NONE, tableName -> {
            onTrino().executeQuery(format(
                    "CREATE TABLE %s " +
                            "WITH (" +
                            "format='ORC', " +
                            "transactional=true " +
                            "%s" +
                            ") AS SELECT orderkey, orderstatus, totalprice, orderdate, clerk, shippriority, \"comment\", custkey, orderpriority " +
                            "FROM tpch.sf1000.orders LIMIT 0", tableName, isPartitioned ? ", partitioned_by = ARRAY['orderpriority']" : ""));
            onTrino().executeQuery("SET SESSION scale_writers = true");
            onTrino().executeQuery("SET SESSION writer_min_size = '4kB'");
            onTrino().executeQuery("SET SESSION task_scale_writers_enabled = false");
            onTrino().executeQuery("SET SESSION task_writer_count = 4");
            onTrino().executeQuery("SET SESSION task_partitioned_writer_count = 4");
            onTrino().executeQuery("SET SESSION hive.target_max_file_size = '1MB'");

            onTrino().executeQuery(
                    format(
                            "INSERT INTO %s SELECT orderkey, orderstatus, totalprice, orderdate, clerk, shippriority, \"comment\", custkey, orderpriority " +
                                    "FROM tpch.sf1000.orders LIMIT 100000", tableName));
            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s", tableName))).containsOnly(row(100000));
            int numberOfCreatedFiles = onTrino().executeQuery(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRowsCount();
            int expectedNumberOfPartitions = isPartitioned ? 5 : 1;
            assertEquals(numberOfCreatedFiles, expectedNumberOfPartitions, format("There should be only %s files created", expectedNumberOfPartitions));

            int sizeBeforeDeletion = onTrino().executeQuery(format("SELECT orderkey FROM %s", tableName)).rows().size();

            onTrino().executeQuery(format("DELETE FROM %s WHERE (orderkey %% 2) = 0", tableName));
            assertThat(onTrino().executeQuery(format("SELECT COUNT (orderkey) FROM %s WHERE orderkey %% 2 = 0", tableName))).containsOnly(row(0));

            int sizeOnTrinoWithWhere = onTrino().executeQuery(format("SELECT orderkey FROM %s WHERE orderkey %% 2 = 1", tableName)).rows().size();
            int sizeOnHiveWithWhere = onHive().executeQuery(format("SELECT orderkey FROM %s WHERE orderkey %% 2 = 1", tableName)).rows().size();
            int sizeOnTrinoWithoutWhere = onTrino().executeQuery(format("SELECT orderkey FROM %s", tableName)).rows().size();

            assertEquals(sizeOnHiveWithWhere, sizeOnTrinoWithWhere);
            assertEquals(sizeOnTrinoWithWhere, sizeOnTrinoWithoutWhere);
            assertTrue(sizeBeforeDeletion > sizeOnTrinoWithoutWhere);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testLargePartitionedDelete()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }
        withTemporaryTable("large_delete_" + "stage1", false, false, NONE, tableStage1 -> {
            onTrino().executeQuery("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 9001), SEQUENCE(1, 9001)) AS t(a, b)".formatted(tableStage1));
            withTemporaryTable("large_delete_" + "stage2", false, false, NONE, tableStage2 -> {
                onTrino().executeQuery("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 100), SEQUENCE(1, 100)) AS t(a, b)".formatted(tableStage2));
                withTemporaryTable("large_delete_" + "new", true, true, NONE, tableNew -> {
                    onTrino().executeQuery("""
                            CREATE TABLE %s WITH (transactional=true, partitioned_by=ARRAY['d'])
                            AS (SELECT stage1.a as a, stage1.b as b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d)
                            """.formatted(tableNew, tableStage1, tableStage2));
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100));
                    onTrino().executeQuery("DELETE FROM %s WHERE d = 20220101".formatted(tableNew));

                    // Verify no rows
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(0));

                    onTrino().executeQuery("INSERT INTO %s SELECT stage1.a AS a, stage1.b AS b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d".formatted(tableNew, tableStage1, tableStage2));

                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100));
                    onTrino().executeQuery("DELETE FROM %s WHERE d = 20220101".formatted(tableNew));

                    // Verify no rows
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(0));
                });
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL)
    public void testLargePartitionedUpdate()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }
        withTemporaryTable("large_update_" + "stage1", false, false, NONE, tableStage1 -> {
            onTrino().executeQuery("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 9001), SEQUENCE(1, 9001)) AS t(a, b)".formatted(tableStage1));
            withTemporaryTable("large_update_" + "stage2", false, false, NONE, tableStage2 -> {
                onTrino().executeQuery("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 100), SEQUENCE(1, 100)) AS t(a, b)".formatted(tableStage2));
                withTemporaryTable("large_update_" + "new", true, true, NONE, tableNew -> {
                    onTrino().executeQuery("""
                            CREATE TABLE %s WITH (transactional=true, partitioned_by=ARRAY['d'])
                            AS (SELECT stage1.a as a, stage1.b as b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d)
                            """.formatted(tableNew, tableStage1, tableStage2));
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100));
                    onTrino().executeQuery("UPDATE %s SET a = 0 WHERE d = 20220101".formatted(tableNew));

                    // Verify all rows updated
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE a = 0".formatted(tableNew), row(900100));
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100));

                    onTrino().executeQuery("INSERT INTO %s SELECT stage1.a AS a, stage1.b AS b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d".formatted(tableNew, tableStage1, tableStage2));

                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(1800200));
                    onTrino().executeQuery("UPDATE %s SET a = 0 WHERE d = 20220101".formatted(tableNew));

                    // Verify all matching rows updated
                    verifySelectForTrinoAndHive("SELECT count(1) FROM %s WHERE a = 0".formatted(tableNew), row(1800200));
                });
            });
        });
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

    private static String trinoTableProperties(TransactionalTableType transactionalTableType, boolean isPartitioned, BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.addAll(transactionalTableType.getTrinoTableProperties());
        tableProperties.addAll(bucketingType.getTrinoTableProperties("fcol", 4));
        if (isPartitioned) {
            tableProperties.add("partitioned_by = ARRAY['partcol']");
        }
        return tableProperties.build().stream().collect(joining(",", "WITH (", ")"));
    }

    private static void compactTableAndWait(CompactionMode compactMode, String tableName, String partitionString, Duration timeout)
    {
        log.info("Running %s compaction on %s", compactMode, tableName);

        Failsafe.with(
                RetryPolicy.builder()
                        .withMaxDuration(java.time.Duration.ofMillis(timeout.toMillis()))
                        .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                        .build())
                .onFailure(event -> {
                    throw new IllegalStateException(format("Could not compact table %s in %d retries", tableName, event.getAttemptCount()), event.getException());
                })
                .onSuccess(event -> log.info("Finished %s compaction on %s in %s (%d tries)", compactMode, tableName, event.getElapsedTime(), event.getAttemptCount()))
                .run(() -> tryCompactingTable(compactMode, tableName, partitionString, new Duration(2, MINUTES)));
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
                log.info("Compaction has not started yet. Existing compactions: %s", getTableCompactions(compactMode, tableName, Optional.empty()));
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

            rows.add(singleRow.buildOrThrow());
        }

        return rows.build().stream();
    }

    public static String tableName(String testName, boolean isPartitioned, BucketingType bucketingType)
    {
        return format("test_%s_%b_%s_%s", testName, isPartitioned, bucketingType.name(), randomNameSuffix());
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

    private void ensureSchemaEvolutionSupported()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive schema evolution requires Hive version 3 or above");
        }
    }

    public static void verifySelectForTrinoAndHive(String select, Row... rows)
    {
        verifySelect("onTrino", onTrino(), select, rows);
        verifySelect("onHive", onHive(), select, rows);
    }

    public static void verifySelect(String name, QueryExecutor executor, String select, Row... rows)
    {
        assertThat(executor.executeQuery(select))
                .describedAs(name)
                .containsOnly(rows);
    }
}
