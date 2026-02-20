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
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_DEFAULT;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_V2;
import static io.trino.tests.product.hive.BucketingType.NONE;
import static io.trino.tests.product.hive.TransactionalTableType.ACID;
import static io.trino.tests.product.hive.TransactionalTableType.INSERT_ONLY;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveTransactionalTable.
 * <p>
 * Tests Hive ACID/transactional table support in Trino.
 */
@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestHiveTransactionalTable
{
    private static final Logger log = Logger.get(TestHiveTransactionalTable.class);

    private static final int TEST_TIMEOUT = 15 * 60 * 1000;

    // Hive original file path end looks like /000000_0
    // New Trino original file path end looks like /000000_132574635756428963553891918669625313402
    // Older Trino path ends look like /20210416_190616_00000_fsymd_af6f0a3d-5449-4478-a53d-9f9f99c07ed9
    private static final Pattern ORIGINAL_FILE_MATCHER = Pattern.compile(".*/\\d+_\\d+(_[^/]+)?$");

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcid(HiveTransactionalEnvironment env)
    {
        doTestReadFullAcid(env, false, BucketingType.NONE);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidBucketed(HiveTransactionalEnvironment env)
    {
        doTestReadFullAcid(env, false, BucketingType.BUCKETED_DEFAULT);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidPartitioned(HiveTransactionalEnvironment env)
    {
        doTestReadFullAcid(env, true, BucketingType.NONE);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidPartitionedBucketed(HiveTransactionalEnvironment env)
    {
        doTestReadFullAcid(env, true, BucketingType.BUCKETED_DEFAULT);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidBucketedV1(HiveTransactionalEnvironment env)
    {
        assertThatThrownBy(() -> doTestReadFullAcid(env, false, BucketingType.BUCKETED_V1))
                .hasMessageContaining("Hive table is corrupt");
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidBucketedV2(HiveTransactionalEnvironment env)
    {
        doTestReadFullAcid(env, false, BucketingType.BUCKETED_V2);
    }

    private void doTestReadFullAcid(HiveTransactionalEnvironment env, boolean isPartitioned, BucketingType bucketingType)
    {
        String tableName = tableName("read_full_acid", isPartitioned, bucketingType);
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    hiveTableProperties(ACID, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            env.executeHiveUpdate("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(21, 1), row(22, 2));

            // test filtering
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (24, 4)");
            env.executeHiveUpdate("DELETE FROM " + tableName + " where fcol=4");

            // test filtering
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // test minor compacted data read
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            env.compactTableAndWait("minor", tableName, Duration.ofMinutes(6));
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(21, 1), row(22, 2));

            // delete a row
            env.executeHiveUpdate("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(21, 1));

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            env.executeHiveUpdate("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(23, 1));

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE col=20")).containsExactlyInOrder(row(20, 3));

            // test major compaction
            env.compactTableAndWait("major", tableName, Duration.ofMinutes(6));
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(20, 3), row(23, 1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testTwoMinorCompactions(HiveTransactionalEnvironment env)
    {
        String tableName = "test_two_minor_compactions_" + randomNameSuffix();
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    "STORED AS ORC " +
                    hiveTableProperties(ACID, NONE));

            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + " VALUES (22, 2)");
            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(22, 2));

            env.compactTableAndWait("minor", tableName, Duration.ofMinutes(6));
            env.compactTableAndWait("minor", tableName, Duration.ofMinutes(6));
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(22, 2));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    static Stream<Arguments> partitioningAndBucketingTypeDataProvider()
    {
        return Stream.of(
                Arguments.of(false, BucketingType.NONE),
                Arguments.of(false, BucketingType.BUCKETED_DEFAULT),
                Arguments.of(true, BucketingType.NONE),
                Arguments.of(true, BucketingType.BUCKETED_DEFAULT));
    }

    static Stream<Arguments> partitioningAndBucketingTypeSmokeDataProvider()
    {
        return Stream.of(
                Arguments.of(false, BucketingType.NONE),
                Arguments.of(true, BucketingType.BUCKETED_DEFAULT));
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadInsertOnlyOrc(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        testReadInsertOnly(env, isPartitioned, bucketingType, "STORED AS ORC");
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeSmokeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadInsertOnlyParquet(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        testReadInsertOnly(env, isPartitioned, bucketingType, "STORED AS PARQUET");
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeSmokeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadInsertOnlyText(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        testReadInsertOnly(env, isPartitioned, bucketingType, "STORED AS TEXTFILE");
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadInsertOnlyTextWithCustomFormatProperties(HiveTransactionalEnvironment env)
    {
        testReadInsertOnly(
                env,
                false,
                NONE,
                "  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
                        "  WITH SERDEPROPERTIES ('field.delim'=',', 'line.delim'='\\n', 'serialization.format'=',') " +
                        "  STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
                        "  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
    }

    private void testReadInsertOnly(HiveTransactionalEnvironment env, boolean isPartitioned, BucketingType bucketingType, String hiveTableFormatDefinition)
    {
        String tableName = tableName("insert_only", isPartitioned, bucketingType);
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("col", 4) + " " +
                    hiveTableFormatDefinition + " " +
                    hiveTableProperties(INSERT_ONLY, bucketingType));

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            env.executeHiveUpdate("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 1");
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsOnly(row(1));

            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 2");
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(1), row(2));

            assertThat(env.executeTrino("SELECT col FROM " + tableName + " WHERE col=2")).containsExactlyInOrder(row(2));

            // test minor compacted data read
            env.compactTableAndWait("minor", tableName, Duration.ofMinutes(6));
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsExactlyInOrder(row(1), row(2));
            assertThat(env.executeTrino("SELECT col FROM " + tableName + " WHERE col=2")).containsExactlyInOrder(row(2));

            env.executeHiveUpdate("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(env.executeTrino(selectFromOnePartitionsSql)).containsOnly(row(3));

            // Major compaction on insert only table does not work prior to Hive 4
            // We skip this part of the test as we'd need to check Hive version
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidWithOriginalFilesSmoke(HiveTransactionalEnvironment env)
    {
        testReadFullAcidWithOriginalFiles(env, true, BUCKETED_DEFAULT);
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadFullAcidWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        testReadFullAcidWithOriginalFiles(env, isPartitioned, bucketingType);
    }

    private void testReadFullAcidWithOriginalFiles(HiveTransactionalEnvironment env, boolean isPartitioned, BucketingType bucketingType)
    {
        String tableName = "test_full_acid_acid_converted_table_read_" + randomNameSuffix();
        verify(bucketingType.getHiveTableProperties().isEmpty());
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(env, tableName, "WHERE col = 21");

            env.executeHiveUpdate("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            // read with original files and insert delta
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (20, 3)");
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1), row(22, 2));

            // read with original files and delete delta
            env.executeHiveUpdate("DELETE FROM " + tableName + " WHERE fcol = 2");
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(21, 1));

            // read with original files and insert+delete delta (UPDATE)
            env.executeHiveUpdate("UPDATE " + tableName + " SET col = 23 WHERE fcol = 1" + (isPartitioned ? " AND part_col = 2 " : ""));
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(23, 1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUpdateFullAcidWithOriginalFilesTrinoInserting(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        String tableName = "trino_update_full_acid_acid_converted_table_read_" + randomNameSuffix();
        verify(bucketingType.getHiveTableProperties().isEmpty());
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(env, tableName, "WHERE col = 21");

            env.executeHiveUpdate("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(21, 1), row(22, 2));
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1")).containsOnly(row(21, 1));

            if (isPartitioned) {
                env.executeTrinoUpdate("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 4, 2)");
            }
            else {
                env.executeTrinoUpdate("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 4)");
            }

            // read with original files and insert delta
            if (isPartitioned) {
                env.executeTrinoUpdate("INSERT INTO " + tableName + "(col, fcol, part_col) VALUES (20, 3, 2)");
            }
            else {
                env.executeTrinoUpdate("INSERT INTO " + tableName + "(col, fcol) VALUES (20, 3)");
            }

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1), row(22, 2));

            // read with original files and delete delta
            env.executeHiveUpdate("DELETE FROM " + tableName + " WHERE fcol = 2");

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName)).containsOnly(row(20, 3), row(20, 4), row(21, 1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeletingSmoke(HiveTransactionalEnvironment env)
    {
        testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeleting(env, true, BUCKETED_DEFAULT);
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeleting(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeleting(env, isPartitioned, bucketingType);
    }

    private void testUpdateFullAcidWithOriginalFilesTrinoInsertingAndDeleting(HiveTransactionalEnvironment env, boolean isPartitioned, BucketingType bucketingType)
    {
        String tableName = "trino_update_full_acid_acid_converted_table_read_" + randomNameSuffix();
        verify(bucketingType.getHiveTableProperties().isEmpty());
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("fcol", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (10, 100), (11, 110), (12, 120), (13, 130), (14, 140)");
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (15, 150), (16, 160), (17, 170), (18, 180), (19, 190)");

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(env, tableName, "WHERE col = 10");

            env.executeHiveUpdate("ALTER TABLE " + tableName + " SET " + hiveTableProperties(ACID, bucketingType));

            // read with original files
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE col < 12")).containsOnly(row(10, 100), row(11, 110));

            String fields = isPartitioned ? "(col, fcol, part_col)" : "(col, fcol)";
            env.executeTrinoUpdate(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(30, 5, 2, isPartitioned, 3)));
            env.executeTrinoUpdate(format("INSERT INTO %s %s VALUES %s", tableName, fields, makeValues(40, 5, 2, isPartitioned, 3)));

            env.executeTrinoUpdate("DELETE FROM " + tableName + " WHERE col IN (11, 12)");
            env.executeTrinoUpdate("DELETE FROM " + tableName + " WHERE col IN (16, 17)");
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE fcol >= 100")).containsOnly(row(10, 100), row(13, 130), row(14, 140), row(15, 150), row(18, 180), row(19, 190));

            // read with original files and delete delta
            env.executeTrinoUpdate("DELETE FROM " + tableName + " WHERE col = 18 OR col = 14 OR (fcol = 2 AND (col / 2) * 2 = col)");

            assertThat(env.executeHive("SELECT col, fcol FROM " + tableName))
                    .containsOnly(row(10, 100), row(13, 130), row(15, 150), row(19, 190), row(31, 2), row(33, 2), row(41, 2), row(43, 2));

            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName))
                    .containsOnly(row(10, 100), row(13, 130), row(15, 150), row(19, 190), row(31, 2), row(33, 2), row(41, 2), row(43, 2));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("partitioningAndBucketingTypeDataProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadInsertOnlyWithOriginalFiles(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        String tableName = "test_insert_only_acid_converted_table_read_" + randomNameSuffix();
        verify(bucketingType.getHiveTableProperties().isEmpty());
        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col INT) " +
                    (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                    bucketingType.getHiveClustering("col", 4) + " " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='false')");

            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";

            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (1)");
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (2)");

            // verify that the existing rows are stored in original files
            verifyOriginalFiles(env, tableName, "WHERE col = 1");

            env.executeHiveUpdate("ALTER TABLE " + tableName + " SET " + hiveTableProperties(INSERT_ONLY, bucketingType));

            // read with original files
            assertThat(env.executeTrino("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2));

            // read with original files and delta
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (3)");
            assertThat(env.executeTrino("SELECT col FROM " + tableName + (isPartitioned ? " WHERE part_col = 2 " : "" + " ORDER BY col"))).containsOnly(row(1), row(2), row(3));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    static Stream<Arguments> testCreateAcidTableDataProvider()
    {
        return Stream.of(
                Arguments.of(false, BucketingType.NONE),
                Arguments.of(false, BucketingType.BUCKETED_DEFAULT),
                Arguments.of(false, BucketingType.BUCKETED_V1),
                Arguments.of(false, BucketingType.BUCKETED_V2),
                Arguments.of(true, BucketingType.NONE),
                Arguments.of(true, BucketingType.BUCKETED_DEFAULT));
    }

    @ParameterizedTest
    @MethodSource("testCreateAcidTableDataProvider")
    void testCtasAcidTable(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        String tableName = format("ctas_transactional_%s", randomNameSuffix());
        try {
            env.executeTrinoUpdate("CREATE TABLE " + tableName + " " +
                    trinoTableProperties(ACID, isPartitioned, bucketingType) +
                    " AS SELECT * FROM (VALUES (21, 1, 1), (22, 1, 2), (23, 2, 2)) t(col, fcol, partcol)");

            // can we query from Trino
            assertThat(env.executeTrino("SELECT col, fcol FROM " + tableName + " WHERE partcol = 2 ORDER BY col"))
                    .containsOnly(row(22, 1), row(23, 2));

            // can we query from Hive
            assertThat(env.executeHive("SELECT col, fcol FROM " + tableName + " WHERE partcol = 2 ORDER BY col"))
                    .containsOnly(row(22, 1), row(23, 2));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("testCreateAcidTableDataProvider")
    void testCreateAcidTable(boolean isPartitioned, BucketingType bucketingType, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "create_transactional", isPartitioned, bucketingType, tableName -> {
            env.executeTrinoUpdate("CREATE TABLE " + tableName + " (col INTEGER, fcol INTEGER, partcol INTEGER)" +
                    trinoTableProperties(ACID, isPartitioned, bucketingType));

            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3)");
            assertThat(env.executeTrino("SELECT * FROM " + tableName)).containsOnly(row(1, 2, 3));
        });
    }

    static Stream<Arguments> acidFormatColumnNames()
    {
        return Stream.of(
                Arguments.of("operation"),
                Arguments.of("originalTransaction"),
                Arguments.of("bucket"),
                Arguments.of("rowId"),
                Arguments.of("row"),
                Arguments.of("currentTransaction"));
    }

    @ParameterizedTest
    @MethodSource("acidFormatColumnNames")
    void testAcidTableColumnNameConflict(String columnName, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "acid_column_name_conflict", true, NONE, tableName -> {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (`" + columnName + "` INTEGER, fcol INTEGER, partcol INTEGER) STORED AS ORC " + hiveTableProperties(ACID, NONE));
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3)");
            assertThat(env.executeTrino("SELECT * FROM " + tableName)).containsOnly(row(1, 2, 3));
        });
    }

    @Test
    void testSimpleUnpartitionedTransactionalInsert(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "unpartitioned_transactional_insert", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));

            String insertQuery = format("INSERT INTO %s VALUES (11, 100), (12, 200), (13, 300)", tableName);

            // ensure that we treat ACID tables as implicitly bucketed on INSERT
            String explainOutput = (String) env.executeTrino("EXPLAIN " + insertQuery).getRows().get(0).getValue(0);
            assertThat(explainOutput).contains("Output partitioning: hive:HivePartitioningHandle{buckets=1");

            env.executeTrinoUpdate(insertQuery);

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(11, 100L), row(12, 200L), row(13, 300L));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (14, 400), (15, 500), (16, 600)", tableName));

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(11, 100L), row(12, 200L), row(13, 300L), row(14, 400L), row(15, 500L), row(16, 600L));
        });
    }

    @Test
    void testTransactionalPartitionInsert(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "transactional_partition_insert", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));

            env.executeTrinoUpdate(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s WHERE column1 > 10", tableName), row(20L));

            env.executeTrinoUpdate(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 30),
                    makeInsertValues(2, 21, 30)));

            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(20L));

            execute(env, "HIVE", format("DELETE FROM %s WHERE column1 > 15 AND column1 <= 25", tableName));

            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(0L));

            env.executeTrinoUpdate(format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 20, 23),
                    makeInsertValues(2, 20, 23)));

            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s WHERE column1 > 15 AND column1 <= 25", tableName), row(8L));
        });
    }

    @Test
    void testTransactionalBucketedPartitionedInsert(HiveTransactionalEnvironment env)
    {
        testTransactionalBucketedPartitioned(env, false);
    }

    @Test
    void testTransactionalBucketedPartitionedInsertOnly(HiveTransactionalEnvironment env)
    {
        testTransactionalBucketedPartitioned(env, true);
    }

    private void testTransactionalBucketedPartitioned(HiveTransactionalEnvironment env, boolean insertOnly)
    {
        withTemporaryTable(env, "bucketed_partitioned_insert_only", true, BUCKETED_V2, tableName -> {
            String insertOnlyProperty = insertOnly ? ", 'transactional_properties'='insert_only'" : "";
            env.executeHiveUpdate(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS" +
                            " STORED AS ORC TBLPROPERTIES ('transactional' = 'true'%s)",
                    tableName, insertOnlyProperty));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(env, format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(env, format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive(env, format("SELECT customer FROM %s WHERE purchase = 'corn'", tableName), row("Debby"), row("Joe"));
        });
    }

    static Stream<Arguments> inserterAndDeleterProvider()
    {
        return Stream.of(
                Arguments.of("HIVE", "TRINO"),
                Arguments.of("TRINO", "TRINO"),
                Arguments.of("TRINO", "HIVE"));
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testTransactionalUnpartitionedDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "unpartitioned_delete", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INTEGER, column2 BIGINT) WITH (format = 'ORC', transactional = true)", tableName));
            execute(env, inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(env, deleter, format("DELETE FROM %s WHERE column2 = 100", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(2, 200L), row(3, 300L), row(4, 400L), row(5, 500L));

            execute(env, inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700)", tableName));
            execute(env, deleter, format("DELETE FROM %s WHERE column1 = 4", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(2, 200L), row(3, 300L), row(5, 500L), row(6, 600L), row(7, 700L));

            execute(env, deleter, format("DELETE FROM %s WHERE column1 <= 3 OR column1 = 6", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(5, 500L), row(7, 700L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMultiDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "unpartitioned_multi_delete", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(env, inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(env, inserter, format("INSERT INTO %s VALUES (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000)", tableName));

            execute(env, deleter, format("DELETE FROM %s WHERE column1 = 9", tableName));
            execute(env, deleter, format("DELETE FROM %s WHERE column1 = 2 OR column1 = 3", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, 100L), row(4, 400L), row(5, 500L), row(6, 600L), row(7, 700L), row(8, 800L), row(10, 1000L));
        });
    }

    @Test
    void testInsertFailsInExplicitTrinoTransaction(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "insert_fail_explicit_transaction", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            env.executeTrinoUpdate("START TRANSACTION");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("INSERT INTO %s (a_string) VALUES ('Commander Bun Bun')", tableName)))
                    .hasMessageContaining("Inserting into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    @Test
    void testUpdateFailsInExplicitTrinoTransaction(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_fail_explicit_transaction", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            env.executeTrinoUpdate("START TRANSACTION");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("UPDATE %s SET a_string = 'Commander Bun Bun'", tableName)))
                    .hasMessageContaining("Merging into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    @Test
    void testDeleteFailsInExplicitTrinoTransaction(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_fail_explicit_transaction", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (a_string varchar) WITH (format = 'ORC', transactional = true)", tableName));
            env.executeTrinoUpdate("START TRANSACTION");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("DELETE FROM %s WHERE a_string = 'Commander Bun Bun'", tableName)))
                    .hasMessageContaining("Merging into Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        });
    }

    static Stream<Arguments> transactionModeProvider()
    {
        return Stream.of(Arguments.of(true), Arguments.of(false));
    }

    @ParameterizedTest
    @MethodSource("transactionModeProvider")
    void testColumnRenamesOrcPartitioned(boolean transactional, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_column_renames_partitioned", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR)" +
                    " WITH (format = 'ORC', transactional = %s, partitioned_by = ARRAY['old_state'])", tableName, transactional));
            testOrcColumnRenames(env, tableName);

            log.info("About to rename partition column old_state to new_state");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN old_state TO new_state", tableName)))
                    .hasMessageContaining("Renaming partition columns is not supported");
        });
    }

    @ParameterizedTest
    @MethodSource("transactionModeProvider")
    void testColumnRenamesOrcNotPartitioned(boolean transactional, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_orc_column_renames_not_partitioned", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR)" +
                    " WITH (format = 'ORC', transactional = %s)", tableName, transactional));
            testOrcColumnRenames(env, tableName);
        });
    }

    private void testOrcColumnRenames(HiveTransactionalEnvironment env, String tableName)
    {
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
        verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"));

        env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a single rename");
        verifySelectForTrino(env, "SELECT age FROM %s WHERE new_name = 'Katy'".formatted(tableName), row(57));

        env.executeTrinoUpdate(format("INSERT INTO %s VALUES(333, 'Joan', 23, 'OR')", tableName));
        verifySelectForTrino(env, "SELECT age FROM %s WHERE new_name != 'Joe'".formatted(tableName), row(57), row(23));

        env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN new_name TO newer_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a double rename");
        verifySelectForTrino(env, "SELECT age FROM %s WHERE newer_name = 'Katy'".formatted(tableName), row(57));

        env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN newer_name TO old_name", tableName));
        log.info("This shows that Trino and Hive can still query old data after a rename back to the original name");
        verifySelectForTrino(env, "SELECT age FROM %s WHERE old_name = 'Katy'".formatted(tableName), row(57));
        verifySelectForTrino(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"), row(333L, "Joan", 23, "OR"));
    }

    @ParameterizedTest
    @MethodSource("transactionModeProvider")
    void testOrcColumnSwap(boolean transactional, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_orc_column_renames", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (name VARCHAR, state VARCHAR) WITH (format = 'ORC', transactional = %s)", tableName, transactional));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('Katy', 'CA'), ('Joe', 'WA')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row("Katy", "CA"), row("Joe", "WA"));

            env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN name TO new_name", tableName));
            env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN state TO name", tableName));
            env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN new_name TO state", tableName));
            log.info("This shows that Trino and Hive can still query old data, but because of the renames, columns are swapped!");
            verifySelectForTrino(env, "SELECT state, name FROM " + tableName, row("Katy", "CA"), row("Joe", "WA"));
        });
    }

    @Test
    void testBehaviorOnParquetColumnRenames(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_parquet_column_renames", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR) WITH (format = 'PARQUET', transactional = false)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"));

            env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN old_name TO new_name", tableName));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (333, 'Fineas', 31, 'OR')", tableName));

            log.info("This shows that Hive and Trino do not see old data after a rename");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, null, 57, "CA"), row(222L, null, 72, "WA"), row(333L, "Fineas", 31, "OR"));

            env.executeTrinoUpdate(format("ALTER TABLE %s RENAME COLUMN new_name TO old_name", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (444, 'Gladys', 47, 'WA')", tableName));
            log.info("This shows that Trino and Hive both see data in old data files after renaming back to the original column name");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"), row(333L, null, 31, "OR"), row(444L, "Gladys", 47, "WA"));
        });
    }

    @ParameterizedTest
    @MethodSource("transactionModeProvider")
    void testOrcColumnDropAdd(boolean transactional, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_orc_add_drop", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, old_state VARCHAR) WITH (transactional = %s)", tableName, transactional));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"));

            env.executeTrinoUpdate(format("ALTER TABLE %s DROP COLUMN old_state", tableName));
            log.info("This shows that neither Trino nor Hive see the old data after a column is dropped");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57), row(222L, "Joe", 72));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (333, 'Kelly', 45)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57), row(222L, "Joe", 72), row(333L, "Kelly", 45));

            env.executeTrinoUpdate(format("ALTER TABLE %s ADD COLUMN new_state VARCHAR", tableName));
            log.info("This shows that for ORC, Trino and Hive both see data inserted into a dropped column when a column of the same type but different name is added");
            verifySelectForTrino(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"), row(333L, "Kelly", 45, null));
        });
    }

    @ParameterizedTest
    @MethodSource("transactionModeProvider")
    void testOrcColumnTypeChange(boolean transactional, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_orc_column_type_change", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id INT, old_name VARCHAR, age TINYINT, old_state VARCHAR) WITH (transactional = %s)", tableName, transactional));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111, "Katy", (byte) 57, "CA"), row(222, "Joe", (byte) 72, "WA"));

            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN age age INT", tableName));
            log.info("This shows that Hive see the old data after a column is widened");
            assertThat(env.executeHive("SELECT * FROM " + tableName))
                    .containsOnly(row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));
            log.info("This shows that Trino see the old data after a column is widened");
            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(row(111, "Katy", 57, "CA"), row(222, "Joe", 72, "WA"));
        });
    }

    @Test
    void testParquetColumnDropAdd(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_parquet_add_drop", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id BIGINT, old_name VARCHAR, age INT, state VARCHAR) WITH (format = 'PARQUET')", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (111, 'Katy', 57, 'CA'), (222, 'Joe', 72, 'WA')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"));

            env.executeTrinoUpdate(format("ALTER TABLE %s DROP COLUMN state", tableName));
            log.info("This shows that neither Trino nor Hive see the old data after a column is dropped");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57), row(222L, "Joe", 72));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (333, 'Kelly', 45)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57), row(222L, "Joe", 72), row(333L, "Kelly", 45));

            env.executeTrinoUpdate(format("ALTER TABLE %s ADD COLUMN state VARCHAR", tableName));
            log.info("This shows that for Parquet, Trino and Hive both see data inserted into a dropped column when a column of the same name and type is added");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, "CA"), row(222L, "Joe", 72, "WA"), row(333L, "Kelly", 45, null));

            env.executeTrinoUpdate(format("ALTER TABLE %s DROP COLUMN state", tableName));
            env.executeTrinoUpdate(format("ALTER TABLE %s ADD COLUMN new_state VARCHAR", tableName));

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(111L, "Katy", 57, null), row(222L, "Joe", 72, null), row(333L, "Kelly", 45, null));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateFailNonTransactional(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_fail_nontransactional", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail update");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateFailInsertOnlyTable(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_fail_insert_only", false, NONE, tableName -> {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (customer STRING, purchase STRING) " +
                    "STORED AS ORC " +
                    hiveTableProperties(INSERT_ONLY, NONE));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail update");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidDeleteFailNonTransactional(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_fail_nontransactional", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail delete");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("DELETE FROM %s WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidDeleteFailInsertOnlyTable(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_fail_insert_only", false, NONE, tableName -> {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (customer STRING, purchase STRING) " +
                    "STORED AS ORC " +
                    hiveTableProperties(INSERT_ONLY, NONE));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to fail delete");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("DELETE FROM %s WHERE customer = 'Fred'", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSimple(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "acid_update_simple", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (7, 'ONE', 1000, true, 101), (13, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col2 = 'DEUX', col3 = col3 + 20 + col1 + col5 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 7, "ONE", 1000L, true, 101), row((byte) 13, "DEUX", 2235L, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdatePartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_partitioned", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['col3'])", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3) VALUES (13, 'T1', 3), (23, 'T2', 3), (17, 'S1', 7)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(13, "T1", 3L), row(23, "T2", 3L), row(17, "S1", 7L));

            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = col1 + 1 WHERE col3 = 3 AND col1 > 15", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(13, "T1", 3L), row(24, "T2", 3L), row(17, "S1", 7L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateBucketed(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_bucketed", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards'), ('Fred', 'limes'), ('Ann', 'lemons')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row("Fred", "cards"), row("Fred", "limes"), row("Ann", "lemons"));

            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Ann'", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row("Fred", "cards"), row("Fred", "limes"), row("Ann", "bread"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteFromOriginalFiles(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_original_files", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(env, tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive(env, "SELECT count(*) FROM " + tableName, row(25L));
            verifySelectForTrinoAndHive(env, format("SELECT nationkey, name FROM %s WHERE regionkey = 4", tableName), row(4L, "EGYPT"), row(10L, "IRAN"), row(11L, "IRAQ"), row(13L, "JORDAN"), row(20L, "SAUDI ARABIA"));
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE regionkey = 4 AND nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive(env, "SELECT count(*) FROM " + tableName, row(24L));
            verifySelectForTrinoAndHive(env, format("SELECT nationkey, name FROM %s WHERE regionkey = 4", tableName), row(4L, "EGYPT"), row(10L, "IRAN"), row(11L, "IRAQ"), row(20L, "SAUDI ARABIA"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteWholePartition(HiveTransactionalEnvironment env)
    {
        testDeleteWholePartition(env, false);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteWholePartitionWithOriginalFiles(HiveTransactionalEnvironment env)
    {
        testDeleteWholePartition(env, true);
    }

    private void testDeleteWholePartition(HiveTransactionalEnvironment env, boolean withOriginalFiles)
    {
        withTemporaryTable(env, "delete_partitioned", true, NONE, tableName -> {
            if (withOriginalFiles) {
                env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                        " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
                verifyOriginalFiles(env, tableName, "WHERE regionkey = 4");
            }
            else {
                env.executeTrinoUpdate(format("CREATE TABLE %s (" +
                        "    nationkey bigint," +
                        "    name varchar(25)," +
                        "    regionkey bigint)" +
                        " WITH (transactional = true, partitioned_by = ARRAY['regionkey'])", tableName));
                env.executeTrinoUpdate(format("INSERT INTO %s SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            }

            verifySelectForTrinoAndHive(env, "SELECT count(*) FROM " + tableName, row(25L));

            // verify all partitions exist
            assertThat(env.executeTrino(format("SELECT * FROM \"%s$partitions\"", tableName)))
                    .containsOnly(row(0L), row(1L), row(2L), row(3L), row(4L));

            // run delete and verify row count
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE regionkey = 4", tableName));
            verifySelectForTrinoAndHive(env, "SELECT count(*) FROM " + tableName, row(20L));

            // verify all partitions still exist
            assertThat(env.executeTrino(format("SELECT * FROM \"%s$partitions\"", tableName)))
                    .containsOnly(row(0L), row(1L), row(2L), row(3L), row(4L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testUpdateOriginalFilesPartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_original_files", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true, partitioned_by = ARRAY['regionkey'])" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(env, tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive(env, "SELECT nationkey, name FROM %s WHERE regionkey = 4".formatted(tableName), row(4L, "EGYPT"), row(10L, "IRAN"), row(11L, "IRAQ"), row(13L, "JORDAN"), row(20L, "SAUDI ARABIA"));
            env.executeTrinoUpdate(format("UPDATE %s SET nationkey = 100 WHERE regionkey = 4 AND nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive(env, "SELECT nationkey, name FROM %s WHERE regionkey = 4".formatted(tableName), row(4L, "EGYPT"), row(10L, "IRAN"), row(11L, "IRAQ"), row(100L, "JORDAN"), row(20L, "SAUDI ARABIA"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testUpdateOriginalFilesUnpartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_original_files", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true)" +
                    " AS SELECT nationkey, name, regionkey FROM tpch.tiny.nation", tableName));
            verifyOriginalFiles(env, tableName, "WHERE regionkey = 4");
            verifySelectForTrinoAndHive(env, "SELECT nationkey, name, regionkey FROM %s WHERE nationkey %% 10 = 3".formatted(tableName), row(3L, "CANADA", 1L), row(13L, "JORDAN", 4L), row(23L, "UNITED KINGDOM", 3L));
            env.executeTrinoUpdate(format("UPDATE %s SET nationkey = nationkey + 100 WHERE nationkey %% 10 = 3", tableName));
            verifySelectForTrinoAndHive(env, "SELECT nationkey, name, regionkey FROM %s WHERE nationkey %% 10 = 3".formatted(tableName), row(103L, "CANADA", 1L), row(113L, "JORDAN", 4L), row(123L, "UNITED KINGDOM", 3L));
        });
    }

    @Test
    void testDoubleUpdateAndThenReadFromHive(HiveTransactionalEnvironment env)
    {
        String tableName = "test_double_update_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(
                    "CREATE TABLE " + tableName + " ( " +
                            "column1 INT, " +
                            "column2 VARCHAR " +
                            ") " +
                            "WITH ( " +
                            "   transactional = true " +
                            ")");
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES(1, 'x')");
            env.executeTrinoUpdate("INSERT INTO " + tableName + " VALUES(2, 'y')");
            env.executeTrinoUpdate("UPDATE " + tableName + " SET column2 = 'xy1'");
            env.executeTrinoUpdate("UPDATE " + tableName + " SET column2 = 'xy2'");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "xy2"), row(2, "xy2"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testDeleteAfterMajorCompaction(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_delete_after_major_compaction", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.tiny.nation", tableName));
            env.compactTableAndWait("major", tableName, Duration.ofMinutes(3));
            env.executeTrinoUpdate(format("DELETE FROM %s", tableName));
            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s", tableName), row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateDuplicateUpdateValue(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_update_bug", false, NONE, tableName -> {
            env.executeTrinoUpdate(
                    format("CREATE TABLE %s (", tableName) +
                            " yyyy integer," +
                            " week_number integer," +
                            " week_start_date date," +
                            " week_end_date date," +
                            " genre_summary_status smallint," +
                            " shop_summary_status smallint)" +
                            " WITH (transactional = true)");

            env.executeTrinoUpdate(format("INSERT INTO %s", tableName) +
                    "(yyyy, week_number, week_start_date, week_end_date, genre_summary_status, shop_summary_status) VALUES" +
                    "  (2021, 20,  DATE '2021-09-10', DATE '2021-09-10', 20, 20)" +
                    ", (2021, 30,  DATE '2021-09-09', DATE '2021-09-09', 30, 30)" +
                    ", (2021, 999, DATE '2018-12-24', DATE '2018-12-24', 999, 999)" +
                    ", (2021, 30,  DATE '2021-09-09', DATE '2021-09-10', 30, 30)");

            env.executeTrinoUpdate(format("UPDATE %s", tableName) +
                    " SET genre_summary_status = 1, shop_summary_status = 1" +
                    " WHERE week_start_date = DATE '2021-09-19' - (INTERVAL '08' DAY)" +
                    "    OR week_start_date = DATE '2021-09-19' - (INTERVAL '09' DAY)");

            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .contains(
                            row(2021, 20, Date.valueOf("2021-09-10"), Date.valueOf("2021-09-10"), (short) 1, (short) 1),
                            row(2021, 30, Date.valueOf("2021-09-09"), Date.valueOf("2021-09-09"), (short) 30, (short) 30),
                            row(2021, 999, Date.valueOf("2018-12-24"), Date.valueOf("2018-12-24"), (short) 999, (short) 999),
                            row(2021, 30, Date.valueOf("2021-09-09"), Date.valueOf("2021-09-10"), (short) 30, (short) 30));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateMultipleDuplicateValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_update_multiple", false, NONE, tableName -> {
            env.executeTrinoUpdate(
                    format("CREATE TABLE %s (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int) WITH (transactional = true)", tableName));

            log.info("Inserting into table");
            env.executeTrinoUpdate(format("INSERT INTO %s (c1, c2, c3, c4, c5, c6) VALUES (1, 2, 3, 4, 5, 6)", tableName));

            log.info("Performing first update");
            env.executeTrinoUpdate(format("UPDATE %s SET c1 = 2, c2 = 4, c3 = 2, c4 = 4, c5 = 4, c6 = 2", tableName));

            log.info("Checking first results");
            assertThat(env.executeTrino("SELECT * FROM " + tableName)).contains(row(2, 4, 2, 4, 4, 2));

            log.info("Performing second update");
            env.executeTrinoUpdate(format("UPDATE %s SET c1 = c1 + c2, c2 = c3 + c4, c3 = c1 + c2, c4 = c4 + c3, c5 = c3 + c4, c6 = c4 + c3", tableName));

            log.info("Checking second results");
            assertThat(env.executeTrino("SELECT * FROM " + tableName)).contains(row(6, 6, 6, 6, 6, 6));

            log.info("Finished");
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteWithOriginalFiles(HiveTransactionalEnvironment env)
            throws SQLException
    {
        withTemporaryTable(env, "test_delete_with_original_files", false, NONE, tableName -> {
            // Use a single connection to maintain session settings
            try (Connection conn = env.createTrinoConnection();
                    Statement stmt = conn.createStatement()) {
                // These properties are necessary to make sure there is more than 1 original file created
                stmt.execute("SET SESSION scale_writers = true");
                stmt.execute("SET SESSION writer_scaling_min_data_processed = '4kB'");
                stmt.execute("SET SESSION task_scale_writers_enabled = false");
                stmt.execute("SET SESSION task_min_writer_count = 2");
                stmt.execute(format(
                        "CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf1.orders LIMIT 100000", tableName));

                // Verify there are at least 2 files
                int fileCount = env.executeTrino(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRows().size();
                verify(fileCount >= 2, "There should be at least 2 files, but found: %s", fileCount);
                validateFileIsDirectlyUnderTableLocation(env, tableName);

                stmt.execute(format("DELETE FROM %s", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to execute test", e);
            }
            verifySelectForTrinoAndHive(env, format("SELECT COUNT(*) FROM %s", tableName), row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteWithOriginalFilesWithWhereClause(HiveTransactionalEnvironment env)
            throws SQLException
    {
        withTemporaryTable(env, "test_delete_with_original_files_with_where_clause", false, NONE, tableName -> {
            // Use a single connection to maintain session settings
            try (Connection conn = env.createTrinoConnection();
                    Statement stmt = conn.createStatement()) {
                // These properties are necessary to make sure there is more than 1 original file created
                stmt.execute("SET SESSION scale_writers = true");
                stmt.execute("SET SESSION writer_scaling_min_data_processed = '4kB'");
                stmt.execute("SET SESSION task_scale_writers_enabled = false");
                stmt.execute("SET SESSION task_min_writer_count = 2");
                stmt.execute(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf1.orders LIMIT 100000", tableName));

                // Verify there are at least 2 files
                int fileCount = env.executeTrino(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRows().size();
                verify(fileCount >= 2, "There should be at least 2 files, but found: %s", fileCount);
                validateFileIsDirectlyUnderTableLocation(env, tableName);
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to execute test setup", e);
            }

            long sizeBeforeDeletion = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s", tableName)).getRows().get(0).getValue(0);

            env.executeTrinoUpdate(format("DELETE FROM %s WHERE (orderkey %% 2) = 0", tableName));
            assertThat(env.executeTrino(format("SELECT COUNT(orderkey) FROM %s WHERE orderkey %% 2 = 0", tableName))).containsOnly(row(0L));

            long sizeOnTrinoWithWhere = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s WHERE orderkey %% 2 = 1", tableName)).getRows().get(0).getValue(0);
            long sizeOnHiveWithWhere = (long) env.executeHive(format("SELECT COUNT(*) FROM %s WHERE orderkey %% 2 = 1", tableName)).getRows().get(0).getValue(0);
            long sizeOnTrinoWithoutWhere = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s", tableName)).getRows().get(0).getValue(0);

            verify(sizeOnHiveWithWhere == sizeOnTrinoWithWhere, "Hive and Trino counts should match");
            verify(sizeOnTrinoWithWhere == sizeOnTrinoWithoutWhere, "Trino counts with and without WHERE should match");
            verify(sizeBeforeDeletion > sizeOnTrinoWithoutWhere, "Size before deletion should be greater than after");
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testUnbucketedPartitionedTransactionalTableWithTaskWriterCountGreaterThanOne(HiveTransactionalEnvironment env)
    {
        unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(env, true);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testUnbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(HiveTransactionalEnvironment env)
    {
        unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(env, false);
    }

    private void unbucketedTransactionalTableWithTaskWriterCountGreaterThanOne(HiveTransactionalEnvironment env, boolean isPartitioned)
    {
        String testNameSuffix = isPartitioned ? "_partitioned" : "";
        withTemporaryTable(env, format("test_unbucketed%s_transactional_table_with_task_writer_count_greater_than_one", testNameSuffix), isPartitioned, NONE, tableName -> {
            // Create the table first (empty)
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s " +
                            "WITH (" +
                            "format='ORC', " +
                            "transactional=true " +
                            "%s" +
                            ") AS SELECT orderkey, orderstatus, totalprice, orderdate, clerk, shippriority, \"comment\", custkey, orderpriority " +
                            "FROM tpch.sf1.orders LIMIT 0", tableName, isPartitioned ? ", partitioned_by = ARRAY['orderpriority']" : ""));

            // Use a single connection to maintain session settings for the insert
            try (Connection conn = env.createTrinoConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.execute("SET SESSION scale_writers = true");
                stmt.execute("SET SESSION writer_scaling_min_data_processed = '4kB'");
                stmt.execute("SET SESSION task_scale_writers_enabled = false");
                stmt.execute("SET SESSION task_min_writer_count = 4");
                stmt.execute("SET SESSION task_max_writer_count = 4");
                stmt.execute("SET SESSION hive.target_max_file_size = '1MB'");

                stmt.execute(
                        format(
                                "INSERT INTO %s SELECT orderkey, orderstatus, totalprice, orderdate, clerk, shippriority, \"comment\", custkey, orderpriority " +
                                        "FROM tpch.sf1.orders LIMIT 100000", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException("Failed to execute test insert", e);
            }

            assertThat(env.executeTrino(format("SELECT count(*) FROM %s", tableName))).containsOnly(row(100000L));
            int numberOfCreatedFiles = env.executeTrino(format("SELECT DISTINCT \"$path\" FROM %s", tableName)).getRows().size();
            int expectedNumberOfPartitions = isPartitioned ? 5 : 1;
            verify(numberOfCreatedFiles == expectedNumberOfPartitions,
                    "There should be only %s files created, but found %s",
                    expectedNumberOfPartitions,
                    numberOfCreatedFiles);

            long sizeBeforeDeletion = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s", tableName)).getRows().get(0).getValue(0);

            env.executeTrinoUpdate(format("DELETE FROM %s WHERE (orderkey %% 2) = 0", tableName));
            assertThat(env.executeTrino(format("SELECT COUNT(orderkey) FROM %s WHERE orderkey %% 2 = 0", tableName))).containsOnly(row(0L));

            long sizeOnTrinoWithWhere = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s WHERE orderkey %% 2 = 1", tableName)).getRows().get(0).getValue(0);
            long sizeOnHiveWithWhere = (long) env.executeHive(format("SELECT COUNT(*) FROM %s WHERE orderkey %% 2 = 1", tableName)).getRows().get(0).getValue(0);
            long sizeOnTrinoWithoutWhere = (long) env.executeTrino(format("SELECT COUNT(*) FROM %s", tableName)).getRows().get(0).getValue(0);

            verify(sizeOnHiveWithWhere == sizeOnTrinoWithWhere, "Hive and Trino counts should match");
            verify(sizeOnTrinoWithWhere == sizeOnTrinoWithoutWhere, "Trino counts with and without WHERE should match");
            verify(sizeBeforeDeletion > sizeOnTrinoWithoutWhere, "Size before deletion should be greater than after");
        });
    }

    @Test
    @Flaky(issue = "https://github.com/trinodb/trino/issues/5463", match = "Expected row count to be <4>, but was <6>")
    void testFilesForAbortedTransactionsIgnored(HiveTransactionalEnvironment env)
            throws Exception
    {
        String tableName = "test_aborted_transaction_table_" + randomNameSuffix();
        env.executeHiveUpdate("" +
                "CREATE TABLE " + tableName + " (col INT) " +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true')");

        try (ThriftMetastoreClient client = env.createThriftMetastoreClient()) {
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + " ORDER BY COL";

            // Create `delta-A` file
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + " VALUES (1),(2)");
            QueryResult onePartitionQueryResult = env.executeTrino(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactlyInOrder(row(1), row(2));

            String tableLocation = getTablePath(env, tableName);

            // Insert data to create a valid delta, which creates `delta-B`
            env.executeHiveUpdate("INSERT INTO TABLE " + tableName + " SELECT 3");

            // Simulate aborted transaction in Hive which has left behind a write directory and file (`delta-C` i.e `delta_0000003_0000003_0000`)
            long transaction = client.openTransaction("test");
            client.allocateTableWriteIds("default", tableName, Collections.singletonList(transaction)).get(0).getWriteId();
            client.abortTransaction(transaction);

            String deltaA = tableLocation + "/delta_0000001_0000001_0000";
            String deltaB = tableLocation + "/delta_0000002_0000002_0000";
            String deltaC = tableLocation + "/delta_0000003_0000003_0000";

            HdfsClient hdfs = env.createHdfsClient();

            // Delete original `delta-B`, `delta-C`
            hdfsDeleteAll(hdfs, deltaB);
            hdfsDeleteAll(hdfs, deltaC);

            // Copy content of `delta-A` to `delta-B`
            hdfsCopyAll(hdfs, deltaA, deltaB);

            // Verify that data from delta-A and delta-B is visible
            onePartitionQueryResult = env.executeTrino(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));

            // Copy content of `delta-A` to `delta-C` (which is an aborted transaction)
            hdfsCopyAll(hdfs, deltaA, deltaC);

            // Verify that delta, corresponding to aborted transaction, is not getting read
            onePartitionQueryResult = env.executeTrino(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1), row(1), row(2), row(2));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private String getTablePath(HiveTransactionalEnvironment env, String tableName)
    {
        QueryResult result = env.executeTrino(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName));
        String tablePath = URI.create((String) result.getRows().get(0).getValue(0)).getPath();
        int lastSlash = tablePath.lastIndexOf('/');
        if (lastSlash > 0) {
            String lastSegment = tablePath.substring(lastSlash + 1);
            if (lastSegment.startsWith("delta_") || lastSegment.startsWith("delete_delta_") || lastSegment.startsWith("base_")) {
                return tablePath.substring(0, lastSlash);
            }
        }
        return tablePath;
    }

    private void hdfsDeleteAll(HdfsClient hdfs, String path)
    {
        if (hdfs.exist(path)) {
            hdfs.delete(path);
        }
    }

    private void hdfsCopyAll(HdfsClient hdfs, String source, String destination)
    {
        // Create destination directory
        hdfs.createDirectory(destination);

        // Copy all files from source to destination
        for (String fileName : hdfs.listDirectory(source)) {
            String sourcePath = source + "/" + fileName;
            String destPath = destination + "/" + fileName;
            byte[] content = hdfs.readFile(sourcePath);
            hdfs.saveFile(destPath, content);
        }
    }

    private void validateFileIsDirectlyUnderTableLocation(HiveTransactionalEnvironment env, String tableName)
    {
        env.executeTrino(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName))
                .getRows()
                .forEach(row -> {
                    String path = (String) row.getValue(0);
                    verify(path.endsWith(tableName.toLowerCase(Locale.ENGLISH)),
                            "files in %s are not directly under table location", path);
                });
    }

    // Helper methods

    private void verifyOriginalFiles(HiveTransactionalEnvironment env, String tableName, String whereClause)
    {
        QueryResult result = env.executeTrino(format("SELECT DISTINCT \"$path\" FROM %s %s", tableName, whereClause));
        String path = (String) result.getRows().get(0).getValue(0);
        checkArgument(ORIGINAL_FILE_MATCHER.matcher(path).matches(), "Path should be original file path, but isn't, path: %s", path);
    }

    private static void execute(HiveTransactionalEnvironment env, String engine, String sql)
    {
        if ("TRINO".equals(engine)) {
            env.executeTrinoUpdate(sql);
        }
        else if ("HIVE".equals(engine)) {
            env.executeHiveUpdate(sql);
        }
        else {
            throw new IllegalArgumentException("Unknown engine: " + engine);
        }
    }

    private void withTemporaryTable(HiveTransactionalEnvironment env, String rootName, boolean isPartitioned, BucketingType bucketingType, Consumer<String> testRunner)
    {
        String tableName = tableName(rootName, isPartitioned, bucketingType) + randomNameSuffix();
        try {
            testRunner.accept(tableName);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private static String tableName(String testName, boolean isPartitioned, BucketingType bucketingType)
    {
        return format("test_%s_%b_%s_", testName, isPartitioned, bucketingType.name());
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

    private String makeInsertValues(int col1Value, int col2First, int col2Last)
    {
        checkArgument(col2First <= col2Last, "The first value %s must be less or equal to the last %s", col2First, col2Last);
        return IntStream.rangeClosed(col2First, col2Last).mapToObj(i -> format("(%s, %s)", col1Value, i)).collect(Collectors.joining(", "));
    }

    private String makeValues(int colStart, int colCount, int fcol, boolean isPartitioned, int partCol)
    {
        return IntStream.range(colStart, colStart + colCount - 1)
                .boxed()
                .map(n -> isPartitioned ? format("(%s, %s, %s)", n, fcol, partCol) : format("(%s, %s)", n, fcol))
                .collect(Collectors.joining(", "));
    }

    private static void verifySelectForTrinoAndHive(HiveTransactionalEnvironment env, String select, Row... rows)
    {
        verifySelect("onTrino", env.executeTrino(select), rows);
        verifySelect("onHive", env.executeHive(select), rows);
    }

    private static void verifySelectForTrino(HiveTransactionalEnvironment env, String select, Row... rows)
    {
        verifySelect("onTrino", env.executeTrino(select), rows);
    }

    private static void verifySelect(String name, QueryResult result, Row... rows)
    {
        assertThat(result)
                .describedAs(name)
                .containsOnly(rows);
    }

    // ========== Simple Update Methods ==========

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSelectedValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "acid_update_simple_selected", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (7, 'ONE', 1000, true, 101), (13, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update %s", tableName);
            env.executeTrinoUpdate(format("UPDATE %s SET col2 = 'DEUX', col3 = col3 + 20 + col1 + col5 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 7, "ONE", 1000L, true, 101), row((byte) 13, "DEUX", 2235L, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateCopyColumn(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "acid_update_copy_column", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 int, col2 int, col3 VARCHAR) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3) VALUES (7, 15, 'ONE'), (13, 17, 'DEUX')", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = col2 WHERE col1 = 13", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(7, 15, "ONE"), row(17, 17, "DEUX"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSomeLiteralNullColumnValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_some_literal_null_columns", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to run first update");
            env.executeTrinoUpdate(format("UPDATE %s SET col2 = NULL, col3 = NULL WHERE col1 = 2", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, "ONE", 1000L, true, 101), row((byte) 2, null, null, false, 202));
            log.info("About to run second update");
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = NULL, col2 = NULL, col3 = NULL, col4 = NULL WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(null, null, null, null, 101), row((byte) 2, null, null, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSomeComputedNullColumnValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_some_computed_null_columns", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to run first update");
            // Use IF(RAND()<0, NULL) as a way to compute null
            env.executeTrinoUpdate(format("UPDATE %s SET col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL) WHERE col1 = 2", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, "ONE", 1000L, true, 101), row((byte) 2, null, null, false, 202));
            log.info("About to run second update");
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = IF(RAND()<0, NULL), col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL), col4 = IF(RAND()<0, NULL) WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(null, null, null, null, 101), row((byte) 2, null, null, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateAllLiteralNullColumnValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_all_literal_null_columns", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = NULL, col2 = NULL, col3 = NULL, col4 = NULL, col5 = null WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(null, null, null, null, null), row((byte) 2, "TWO", 2000L, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateAllComputedNullColumnValues(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_all_computed_null_columns", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            // Use IF(RAND()<0, NULL) as a way to compute null
            env.executeTrinoUpdate(format("UPDATE %s SET col1 = IF(RAND()<0, NULL), col2 = IF(RAND()<0, NULL), col3 = IF(RAND()<0, NULL), col4 = IF(RAND()<0, NULL), col5 = IF(RAND()<0, NULL) WHERE col1 = 1", tableName));
            log.info("Finished first update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(null, null, null, null, null), row((byte) 2, "TWO", 2000L, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateReversed(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_reversed", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col3 = col3 + 20 + col1 + col5, col1 = 3, col2 = 'DEUX' WHERE col1 = 2", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, "ONE", 1000L, true, 101), row((byte) 3, "DEUX", 2224L, false, 202));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdatePermuted(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_permuted", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 VARCHAR, col3 BIGINT, col4 BOOLEAN, col5 INT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 'ONE', 1000, true, 101), (2, 'TWO', 2000, false, 202)", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col5 = 303, col1 = 3, col3 = col3 + 20 + col1 + col5, col4 = true, col2 = 'DUO' WHERE col1 = 2", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, "ONE", 1000L, true, 101), row((byte) 3, "DUO", 2224L, true, 303));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateAllColumnsSetAndDependencies(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_all_columns_set", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 INT, col3 BIGINT, col4 INT, col5 TINYINT) WITH (transactional = true)", tableName));
            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 2, 3, 4, 5), (21, 22, 23, 24, 25)", tableName));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 21", tableName));
            log.info("Finished update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 23, 21, 22L, 25, (byte) 24));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSucceedUpdatingPartitionKey(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "fail_update_partition_key", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['col3'])", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3) VALUES (17, 'S1', 7)", tableName));

            log.info("About to succeed updating the partition key");
            env.executeTrinoUpdate(format("UPDATE %s SET col3 = 17 WHERE col3 = 7", tableName));

            log.info("Verify the update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(17, "S1", 17L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateSucceedUpdatingBucketColumn(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "fail_update_bucket_column", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards')", tableName));

            log.info("About to succeed updating bucket column");
            env.executeTrinoUpdate(format("UPDATE %s SET purchase = 'bread' WHERE customer = 'Fred'", tableName));

            log.info("Verifying update");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row("Fred", "bread"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateFailOnIllegalCast(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "fail_update_on_illegal_cast", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 INT, col2 VARCHAR, col3 BIGINT) WITH (transactional = true)", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3) VALUES (17, 'S1', 7)", tableName));

            log.info("About to fail update");
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("UPDATE %s SET col1 = col2 WHERE col3 = 7", tableName)))
                    .hasMessageContaining("UPDATE table column types don't match SET expressions: Table: [integer], Expressions: [varchar]");
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateMajorCompaction(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "schema_evolution_column_addition", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (11, 100)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (22, 200)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(11, 100L), row(22, 200L));
            log.info("About to compact");
            env.compactTableAndWait("major", tableName, Duration.ofMinutes(6));
            log.info("About to update");
            env.executeTrinoUpdate(format("UPDATE %s SET column1 = 33 WHERE column2 = 200", tableName));
            log.info("About to select");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(11, 100L), row(33, 200L));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (44, 400), (55, 500)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(11, 100L), row(33, 200L), row(44, 400L), row(55, 500L));
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE column2 IN (100, 500)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(33, 200L), row(44, 400L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateWithSubqueryPredicate(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_update_subquery", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 varchar) WITH (transactional = true)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1, 'x')", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (2, 'y')", tableName));

            // WHERE with uncorrelated subquery
            env.executeTrinoUpdate(format("UPDATE %s SET column2 = 'row updated' WHERE column1 = (SELECT min(regionkey) + 1 FROM tpch.tiny.region)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "row updated"), row(2, "y"));

            withTemporaryTable(env, "second_table", false, NONE, secondTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (regionkey bigint, name varchar(25), comment varchar(152)) WITH (transactional = true)", secondTable));
                env.executeTrinoUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.region", secondTable));

                // UPDATE while reading from another transactional table. Multiple transactional could interfere with ConnectorMetadata.beginQuery
                env.executeTrinoUpdate(format("UPDATE %s SET column2 = 'another row updated' WHERE column1 = (SELECT min(regionkey) + 2 FROM %s)", tableName, secondTable));
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "row updated"), row(2, "another row updated"));
            });

            // WHERE with correlated subquery
            env.executeTrinoUpdate(format("UPDATE %s SET column2 = 'row updated yet again' WHERE column2 = (SELECT name FROM tpch.tiny.region WHERE regionkey = column1)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "row updated"), row(2, "another row updated"));

            env.executeTrinoUpdate(format("UPDATE %s SET column2 = 'row updated yet again' WHERE column2 != (SELECT name FROM tpch.tiny.region WHERE regionkey = column1)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "row updated yet again"), row(2, "row updated yet again"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testAcidUpdateWithSubqueryAssignment(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_update_subquery", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 varchar) WITH (transactional = true)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1, 'x')", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (2, 'y')", tableName));

            // SET with uncorrelated subquery
            env.executeTrinoUpdate(format("UPDATE %s SET column2 = (SELECT max(name) FROM tpch.tiny.region)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "MIDDLE EAST"), row(2, "MIDDLE EAST"));

            withTemporaryTable(env, "second_table", false, NONE, secondTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (regionkey bigint, name varchar(25), comment varchar(152)) WITH (transactional = true)", secondTable));
                env.executeTrinoUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.region", secondTable));

                // UPDATE while reading from another transactional table. Multiple transactional could interfere with ConnectorMetadata.beginQuery
                env.executeTrinoUpdate(format("UPDATE %s SET column2 = (SELECT min(name) FROM %s)", tableName, secondTable));
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "AFRICA"), row(2, "AFRICA"));

                env.executeTrinoUpdate(format("UPDATE %s SET column2 = (SELECT name FROM %s WHERE column1 = regionkey + 1)", tableName, secondTable));
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "AFRICA"), row(2, "AMERICA"));
            });

            // SET with correlated subquery
            env.executeTrinoUpdate(format("UPDATE %s SET column2 = (SELECT name FROM tpch.tiny.region WHERE column1 = regionkey)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, "AMERICA"), row(2, "ASIA"));
        });
    }

    // ========== Delete & Multi-Op Methods ==========

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteAllRowsInPartition(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "bucketed_partitioned_delete", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            log.info("About to insert");
            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Fred', 'cards'), ('Fred', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')", tableName));

            log.info("About to delete");
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE customer = 'Fred'", tableName));

            log.info("About to verify");
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row("lemons", "Ann"), row("chips", "Ann"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteAfterDelete(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_after_delete", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id INT) WITH (transactional = true)", tableName));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1), (2), (3)", tableName));

            env.executeTrinoUpdate(format("DELETE FROM %s WHERE id = 2", tableName));

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1), row(3));

            env.executeTrinoUpdate("DELETE FROM " + tableName);

            assertThat(env.executeTrino("SELECT count(*) FROM " + tableName)).containsOnly(row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteAfterDeleteWithPredicate(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_after_delete_predicate", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (id INT) WITH (transactional = true)", tableName));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1), (2), (3)", tableName));

            env.executeTrinoUpdate(format("DELETE FROM %s WHERE id = 2", tableName));

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1), row(3));

            // A predicate sufficient to fool statistics-based optimization
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE id != 2", tableName));

            assertThat(env.executeTrino("SELECT count(*) FROM " + tableName)).containsOnly(row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testReadAfterMultiInsertAndDelete(HiveTransactionalEnvironment env)
    {
        // Test reading from a table after Hive multi-insert. Multi-insert involves non-zero statement ID, encoded
        // within ORC-level bucketId field, while originalTransactionId and rowId can be the same for two different rows.
        //
        // This is a regression test to verify that Trino correctly takes into account the bucketId field, including encoded
        // statement id, when filtering out deleted rows.
        //
        // For more context see https://issues.apache.org/jira/browse/HIVE-16832
        withTemporaryTable(env, "partitioned_multi_insert", true, BucketingType.BUCKETED_V1, tableName -> {
            withTemporaryTable(env, "tmp_data_table", false, NONE, dataTableName -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (a int, b int, c varchar(5)) WITH " +
                        "(transactional = true, partitioned_by = ARRAY['c'], bucketed_by = ARRAY['a'], bucket_count = 2)", tableName));
                env.executeTrinoUpdate(format("CREATE TABLE %s (x int)", dataTableName));
                env.executeTrinoUpdate(format("INSERT INTO %s VALUES 1", dataTableName));

                // Perform a multi-insert
                env.executeHiveUpdate("SET hive.exec.dynamic.partition.mode = nonstrict");
                // Combine dynamic and static partitioning to trick Hive to insert two rows with same rowId but different statementId to a single partition.
                env.executeHiveUpdate(format("FROM %s INSERT INTO %s partition(c) SELECT 0, 0, 'c' || x INSERT INTO %2$s partition(c='c1') SELECT 0, 1",
                        dataTableName,
                        tableName));
                env.executeHiveUpdate(format("DELETE FROM %s WHERE b = 1", tableName));
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(0, 0, "c1"));
            });
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testInsertRowIdCorrectness(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_insert_row_id_correctness", false, NONE, tableName -> {
            // We use tpch.tiny.supplier because it is the smallest table that
            // is written as multiple pages by the ORC writer. If it stops
            // being split into pages, this test won't detect issues arising
            // from row numbering across pages, which is its original purpose.
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s (" +
                            "  suppkey bigint," +
                            "  name varchar(25)," +
                            "  address varchar(40)," +
                            "  nationkey bigint," +
                            "  phone varchar(15)," +
                            "  acctbal double," +
                            "  comment varchar(101))" +
                            "WITH (transactional = true)", tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s select * from tpch.tiny.supplier", tableName));

            int supplierRows = 100;
            assertThat(env.executeTrino("SELECT count(*) FROM " + tableName))
                    .containsOnly(row((long) supplierRows));

            String queryTarget = format(" FROM %s WHERE suppkey = 10", tableName);

            assertThat(env.executeTrino("SELECT count(*)" + queryTarget))
                    .describedAs("Only one matching row exists")
                    .containsOnly(row(1L));
            assertThat(env.executeTrino("DELETE" + queryTarget))
                    .describedAs("Only one row is reported as deleted")
                    .containsOnly(row(1L));

            assertThat(env.executeTrino("SELECT count(*) FROM " + tableName))
                    .describedAs("Only one row was actually deleted")
                    .containsOnly(row((long) (supplierRows - 1)));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testDeleteOverManySplits(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_select", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional = true) AS SELECT * FROM tpch.sf10.orders", tableName));

            log.info("About to delete selected rows");
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE clerk = 'Clerk#000004942'", tableName));

            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM %s WHERE clerk = 'Clerk#000004942'".formatted(tableName), row(0L));
        });
    }

    @Test
    void testLargePartitionedDelete(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "large_delete_stage1", false, NONE, tableStage1 -> {
            env.executeTrinoUpdate("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 9001), SEQUENCE(1, 9001)) AS t(a, b)".formatted(tableStage1));
            withTemporaryTable(env, "large_delete_stage2", false, NONE, tableStage2 -> {
                env.executeTrinoUpdate("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 100), SEQUENCE(1, 100)) AS t(a, b)".formatted(tableStage2));
                withTemporaryTable(env, "large_delete_new", true, NONE, tableNew -> {
                    env.executeTrinoUpdate(
                            """
                            CREATE TABLE %s WITH (transactional=true, partitioned_by=ARRAY['d'])
                            AS (SELECT stage1.a as a, stage1.b as b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d)
                            """.formatted(tableNew, tableStage1, tableStage2));
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100L));
                    env.executeTrinoUpdate("DELETE FROM %s WHERE d = 20220101".formatted(tableNew));

                    // Verify no rows
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(0L));

                    env.executeTrinoUpdate("INSERT INTO %s SELECT stage1.a AS a, stage1.b AS b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d".formatted(tableNew, tableStage1, tableStage2));

                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100L));
                    env.executeTrinoUpdate("DELETE FROM %s WHERE d = 20220101".formatted(tableNew));

                    // Verify no rows
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(0L));
                });
            });
        });
    }

    @Test
    void testLargePartitionedUpdate(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "large_update_stage1", false, NONE, tableStage1 -> {
            env.executeTrinoUpdate("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 9001), SEQUENCE(1, 9001)) AS t(a, b)".formatted(tableStage1));
            withTemporaryTable(env, "large_update_stage2", false, NONE, tableStage2 -> {
                env.executeTrinoUpdate("CREATE TABLE %s AS SELECT a, b, 20220101 AS d FROM UNNEST(SEQUENCE(1, 100), SEQUENCE(1, 100)) AS t(a, b)".formatted(tableStage2));
                withTemporaryTable(env, "large_update_new", true, NONE, tableNew -> {
                    env.executeTrinoUpdate(
                            """
                            CREATE TABLE %s WITH (transactional=true, partitioned_by=ARRAY['d'])
                            AS (SELECT stage1.a as a, stage1.b as b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d)
                            """.formatted(tableNew, tableStage1, tableStage2));
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100L));
                    env.executeTrinoUpdate("UPDATE %s SET a = 0 WHERE d = 20220101".formatted(tableNew));

                    // Verify all rows updated
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE a = 0".formatted(tableNew), row(900100L));
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(900100L));

                    env.executeTrinoUpdate("INSERT INTO %s SELECT stage1.a AS a, stage1.b AS b, stage1.d AS d FROM %s stage1, %s stage2 WHERE stage1.d = stage2.d".formatted(tableNew, tableStage1, tableStage2));

                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE d IS NOT NULL".formatted(tableNew), row(1800200L));
                    env.executeTrinoUpdate("UPDATE %s SET a = 0 WHERE d = 20220101".formatted(tableNew));

                    // Verify all matching rows updated
                    verifySelectForTrinoAndHive(env, "SELECT count(1) FROM %s WHERE a = 0".formatted(tableNew), row(1800200L));
                });
            });
        });
    }

    // ========== Engine-Parameterized Tests ==========

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testTransactionalMetadataDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "metadata_delete", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column2'])", tableName));
            execute(env, inserter, format("INSERT INTO %s (column2, column1) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));

            execute(env, deleter, format("DELETE FROM %s WHERE column2 = 1", tableName));
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM %s WHERE column2 = 1".formatted(tableName), row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testNonTransactionalMetadataDelete(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "non_transactional_metadata_delete", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column2 BIGINT, column1 INT) WITH (partitioned_by = ARRAY['column1'])", tableName));

            execute(env, "TRINO", format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 10),
                    makeInsertValues(2, 1, 10)));

            execute(env, "TRINO", format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 11, 20),
                    makeInsertValues(2, 11, 20)));

            execute(env, "TRINO", format("DELETE FROM %s WHERE column1 = 1", tableName));
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM %s WHERE column1 = 1".formatted(tableName), row(0L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testUnpartitionedDeleteAll(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "unpartitioned_delete_all", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(env, inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            execute(env, deleter, "DELETE FROM " + tableName);
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM " + tableName, row(0L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMultiColumnDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "multi_column_delete", false, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column1 INT, column2 BIGINT) WITH (transactional = true)", tableName));
            execute(env, inserter, format("INSERT INTO %s VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)", tableName));
            String where = " WHERE column1 >= 2 AND column2 <= 400";
            execute(env, deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForTrinoAndHive(env, "SELECT * FROM %s WHERE column1 IN (1, 5)".formatted(tableName), row(1, 100L), row(5, 500L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testPartitionAndRowsDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "partition_and_rows_delete", true, NONE, tableName -> {
            env.executeTrinoUpdate("CREATE TABLE " + tableName +
                    " (column2 BIGINT, column1 INT) WITH (transactional = true, partitioned_by = ARRAY['column1'])");
            execute(env, inserter, format("INSERT INTO %s (column1, column2) VALUES (1, 100), (1, 200), (2, 300), (2, 400), (2, 500)", tableName));
            String where = " WHERE column1 = 2 OR column2 = 200";
            execute(env, deleter, format("DELETE FROM %s %s", tableName, where));
            verifySelectForTrinoAndHive(env, "SELECT column1, column2 FROM " + tableName, row(1, 100L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testPartitionedInsertAndRowLevelDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "partitioned_row_level_delete", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (column2 INT, column1 BIGINT) WITH (transactional = true, partitioned_by = ARRAY['column1'])", tableName));

            execute(env, inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 1, 20),
                    makeInsertValues(2, 1, 20)));
            execute(env, inserter, format("INSERT INTO %s (column1, column2) VALUES %s, %s",
                    tableName,
                    makeInsertValues(1, 21, 40),
                    makeInsertValues(2, 21, 40)));

            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30".formatted(tableName), row(40L));

            execute(env, deleter, format("DELETE FROM %s WHERE column2 > 10 AND column2 <= 30", tableName));
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM %s WHERE column2 > 10 AND column2 <= 30".formatted(tableName), row(0L));
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM " + tableName, row(40L));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testBucketedPartitionedDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "bucketed_partitioned_delete", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (purchase STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(env, inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(env, format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(env, format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(env, inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive(env, "SELECT customer FROM %s WHERE purchase = 'corn'".formatted(tableName), row("Debby"), row("Joe"));

            execute(env, deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForTrinoAndHive(env, "SELECT purchase FROM %s WHERE customer = 'Ann'".formatted(tableName), row("cards"), row("cereal"), row("chips"));

            execute(env, deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT customer, purchase FROM " + tableName, row("Fred", "limes"));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testBucketedUnpartitionedDelete(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "bucketed_unpartitioned_delete", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (customer STRING, purchase STRING) CLUSTERED BY (purchase) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true')", tableName));

            execute(env, inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Fred', 'cards'), ('Fred', 'cereal'), ('Fred', 'limes'), ('Fred', 'chips')," +
                    " ('Ann', 'cards'), ('Ann', 'cereal'), ('Ann', 'lemons'), ('Ann', 'chips')," +
                    " ('Lou', 'cards'), ('Lou', 'cereal'), ('Lou', 'lemons'), ('Lou', 'chips')");

            verifySelectForTrinoAndHive(env, format("SELECT customer FROM %s WHERE purchase = 'lemons'", tableName), row("Ann"), row("Lou"));

            verifySelectForTrinoAndHive(env, format("SELECT purchase FROM %s WHERE customer = 'Fred'", tableName), row("cards"), row("cereal"), row("limes"), row("chips"));

            execute(env, inserter, format("INSERT INTO %s (customer, purchase) VALUES", tableName) +
                    " ('Ernie', 'cards'), ('Ernie', 'cereal')," +
                    " ('Debby', 'corn'), ('Debby', 'chips')," +
                    " ('Joe', 'corn'), ('Joe', 'lemons'), ('Joe', 'candy')");

            verifySelectForTrinoAndHive(env, "SELECT customer FROM %s WHERE purchase = 'corn'".formatted(tableName), row("Debby"), row("Joe"));

            execute(env, deleter, format("DELETE FROM %s WHERE purchase = 'lemons'", tableName));
            verifySelectForTrinoAndHive(env, "SELECT purchase FROM %s WHERE customer = 'Ann'".formatted(tableName), row("cards"), row("cereal"), row("chips"));

            execute(env, deleter, format("DELETE FROM %s WHERE purchase like('c%%')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT customer, purchase FROM " + tableName, row("Fred", "limes"));
        });
    }

    @ParameterizedTest
    @MethodSource("inserterAndDeleterProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testCorrectSelectCountStar(String inserter, String deleter, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "select_count_star_delete", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", tableName));

            execute(env, inserter, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c'), (4, 400, 'a'), (5, 500, 'b'), (6, 600, 'c')", tableName));
            execute(env, deleter, format("DELETE FROM %s WHERE col2 = 200", tableName));
            verifySelectForTrinoAndHive(env, "SELECT COUNT(*) FROM " + tableName, row(5L));
        });
    }

    static Stream<Arguments> insertOnlyMultipleWritersProvider()
    {
        return Stream.of(
                Arguments.of(false, "HIVE", "TRINO"),
                Arguments.of(false, "TRINO", "TRINO"),
                Arguments.of(true, "HIVE", "TRINO"),
                Arguments.of(true, "TRINO", "TRINO"));
    }

    @ParameterizedTest
    @MethodSource("insertOnlyMultipleWritersProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testInsertOnlyMultipleWriters(boolean bucketed, String inserter1, String inserter2, HiveTransactionalEnvironment env)
    {
        log.info("testInsertOnlyMultipleWriters bucketed %s, inserter1 %s, inserter2 %s", bucketed, inserter1, inserter2);
        withTemporaryTable(env, "insert_only_partitioned", true, NONE, tableName -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (col1 INT, col2 BIGINT) PARTITIONED BY (col3 STRING) %s STORED AS ORC TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
                    tableName, bucketed ? "CLUSTERED BY (col2) INTO 3 BUCKETS" : ""));

            execute(env, inserter1, format("INSERT INTO %s VALUES (1, 100, 'a'), (2, 200, 'b')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, 100L, "a"), row(2, 200L, "b"));

            execute(env, inserter2, format("INSERT INTO %s VALUES (3, 300, 'c'), (4, 400, 'a')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, 100L, "a"), row(2, 200L, "b"), row(3, 300L, "c"), row(4, 400L, "a"));

            execute(env, inserter1, format("INSERT INTO %s VALUES (5, 500, 'b'), (6, 600, 'c')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, 100L, "a"), row(2, 200L, "b"), row(3, 300L, "c"), row(4, 400L, "a"), row(5, 500L, "b"), row(6, 600L, "c"));
            verifySelectForTrinoAndHive(env, "SELECT * FROM %s WHERE col2 > 300".formatted(tableName), row(4, 400L, "a"), row(5, 500L, "b"), row(6, 600L, "c"));

            execute(env, inserter2, format("INSERT INTO %s VALUES (7, 700, 'b'), (8, 800, 'c')", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row(1, 100L, "a"), row(2, 200L, "b"), row(3, 300L, "c"), row(4, 400L, "a"), row(5, 500L, "b"), row(6, 600L, "c"), row(7, 700L, "b"), row(8, 800L, "c"));
            verifySelectForTrinoAndHive(env, "SELECT * FROM %s WHERE col3 = 'c'".formatted(tableName), row(3, 300L, "c"), row(6, 600L, "c"), row(8, 800L, "c"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertDeleteUpdateWithTrinoAndHive(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "update_insert_delete_trino_hive", true, NONE, tableName -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 INT, col3 BIGINT, col4 INT, col5 TINYINT) WITH (transactional = true)", tableName));

            log.info("Performing first insert on Trino");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (1, 2, 3, 4, 5), (21, 22, 23, 24, 25)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 21, 22, 23L, 24, (byte) 25));

            log.info("Performing first update on Trino");
            env.executeTrinoUpdate(format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 21", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 23, 21, 22L, 25, (byte) 24));

            log.info("Performing second insert on Hive");
            env.executeHiveUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (31, 32, 33, 34, 35)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 23, 21, 22L, 25, (byte) 24), row((byte) 31, 32, 33L, 34, (byte) 35));

            log.info("Performing first delete on Trino");
            env.executeTrinoUpdate(format("DELETE FROM %s WHERE col1 = 23", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 31, 32, 33L, 34, (byte) 35));

            log.info("Performing second update on Hive");
            execute(env, "HIVE", format("UPDATE %s SET col5 = col4, col1 = col3, col3 = col2, col4 = col5, col2 = col1 WHERE col1 = 31", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 33, 31, 32L, 35, (byte) 34));

            log.info("Performing more inserts on Trino");
            env.executeTrinoUpdate(format("INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (41, 42, 43, 44, 45), (51, 52, 53, 54, 55)", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 1, 2, 3L, 4, (byte) 5), row((byte) 33, 31, 32L, 35, (byte) 34), row((byte) 41, 42, 43L, 44, (byte) 45), row((byte) 51, 52, 53L, 54, (byte) 55));

            log.info("Performing second delete on Hive");
            execute(env, "HIVE", format("DELETE FROM %s WHERE col5 = 5", tableName));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + tableName, row((byte) 33, 31, 32L, 35, (byte) 34), row((byte) 41, 42, 43L, 44, (byte) 45), row((byte) 51, 52, 53L, 54, (byte) 55));
        });
    }
}
