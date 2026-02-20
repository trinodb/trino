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
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.cartesianProduct;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_DEFAULT;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_V1;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_V2;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveBucketedTables.
 * <p>
 * Tests bucketed table functionality in Hive connector.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveBucketedTables
{
    private static final Logger log = Logger.get(TestHiveBucketedTables.class);

    private static final String NATION_TABLE = "nation";

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectStar(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_" + randomNameSuffix();
        try {
            createBucketedNationTable(env, tableName);
            populateHiveTable(env, tableName, NATION_TABLE);

            QueryResult result = env.executeTrino("SELECT * FROM hive.default." + tableName);
            // Validate row count and content - all 25 nations with keys 0-24 and all 5 regions (0-4)
            assertThat(result).hasRowsCount(25);
            assertThat(env.executeTrino(format("SELECT count(DISTINCT n_nationkey) FROM hive.default.%s", tableName)))
                    .containsExactlyInOrder(row(25L));
            assertThat(env.executeTrino(format("SELECT n_regionkey, count(*) FROM hive.default.%s GROUP BY n_regionkey", tableName)))
                    .containsOnly(row(0L, 5L), row(1L, 5L), row(2L, 5L), row(3L, 5L), row(4L, 5L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testIgnorePartitionBucketingIfNotBucketed(HiveBasicEnvironment env)
    {
        String tableName = "bucketed_partitioned_nation_" + randomNameSuffix();
        try {
            createBucketedPartitionedNationTable(env, tableName);
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_1'");
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_2'");

            env.executeHiveUpdate(format("ALTER TABLE %s NOT CLUSTERED", tableName));

            assertThat(env.executeTrino(format("SELECT count(DISTINCT n_nationkey), count(*) FROM hive.default.%s", tableName)))
                    .hasRowsCount(1)
                    .contains(row(25L, 50L));

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_nationkey = 1", tableName)))
                    .containsExactlyInOrder(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testAllowMultipleFilesPerBucket(HiveBasicEnvironment env)
    {
        String tableName = "bucketed_partitioned_nation_" + randomNameSuffix();
        try {
            createBucketedPartitionedNationTable(env, tableName);
            for (int i = 0; i < 3; i++) {
                populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert'");
            }

            assertThat(env.executeTrino(format("SELECT count(DISTINCT n_nationkey), count(*) FROM hive.default.%s", tableName)))
                    .hasRowsCount(1)
                    .contains(row(25L, 75L));

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_nationkey = 1", tableName)))
                    .containsExactlyInOrder(row(3L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectAfterMultipleInserts(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_" + randomNameSuffix();
        try {
            createBucketedNationTable(env, tableName);
            populateHiveTable(env, tableName, NATION_TABLE);
            populateHiveTable(env, tableName, NATION_TABLE);

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_nationkey = 1", tableName)))
                    .containsExactlyInOrder(row(2L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey = 1", tableName)))
                    .containsExactlyInOrder(row(10L));
            assertThat(env.executeTrino(format("SELECT n_regionkey, count(*) FROM hive.default.%s GROUP BY n_regionkey", tableName)))
                    .containsOnly(row(0L, 10L), row(1L, 10L), row(2L, 10L), row(3L, 10L), row(4L, 10L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s n JOIN hive.default.%s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                    .containsExactlyInOrder(row(500L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectAfterMultipleInsertsForSortedTable(HiveBasicEnvironment env)
    {
        String tableName = "bucketed_sorted_nation_" + randomNameSuffix();
        try {
            createBucketedSortedNationTable(env, tableName);
            populateHiveTable(env, tableName, NATION_TABLE);
            populateHiveTable(env, tableName, NATION_TABLE);

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_nationkey = 1", tableName)))
                    .containsExactlyInOrder(row(2L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey = 1", tableName)))
                    .containsExactlyInOrder(row(10L));
            assertThat(env.executeTrino(format("SELECT n_regionkey, count(*) FROM hive.default.%s GROUP BY n_regionkey", tableName)))
                    .containsOnly(row(0L, 10L), row(1L, 10L), row(2L, 10L), row(3L, 10L), row(4L, 10L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s n JOIN hive.default.%s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                    .containsExactlyInOrder(row(500L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectAfterMultipleInsertsForPartitionedTable(HiveBasicEnvironment env)
    {
        String tableName = "bucketed_partitioned_nation_" + randomNameSuffix();
        try {
            createBucketedPartitionedNationTable(env, tableName);
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_1'");
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_2'");
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_1'");
            populateHivePartitionedTable(env, tableName, NATION_TABLE, "part_key = 'insert_2'");

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_nationkey = 1", tableName)))
                    .containsExactlyInOrder(row(4L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey = 1", tableName)))
                    .containsExactlyInOrder(row(20L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey = 1 AND part_key = 'insert_1'", tableName)))
                    .hasRowsCount(1)
                    .containsExactlyInOrder(row(10L));
            assertThat(env.executeTrino(format("SELECT n_regionkey, count(*) FROM hive.default.%s WHERE part_key = 'insert_2' GROUP BY n_regionkey", tableName)))
                    .containsOnly(row(0L, 10L), row(1L, 10L), row(2L, 10L), row(3L, 10L), row(4L, 10L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s n JOIN hive.default.%s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                    .containsExactlyInOrder(row(2000L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s n JOIN hive.default.%s n1 ON n.n_regionkey = n1.n_regionkey WHERE n.part_key = 'insert_1'", tableName, tableName)))
                    .containsExactlyInOrder(row(1000L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectFromEmptyBucketedTableEmptyTablesAllowed(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_" + randomNameSuffix();
        try {
            createBucketedNationTable(env, tableName);
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s", tableName)))
                    .containsExactlyInOrder(row(0L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectFromIncompleteBucketedTableEmptyTablesAllowed(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_" + randomNameSuffix();
        try {
            createBucketedNationTable(env, tableName);
            populateRowToHiveTable(env, tableName, ImmutableList.of("2", "'name'", "2", "'comment'"), Optional.empty());
            // insert one row into nation
            assertThat(env.executeTrino(format("SELECT count(*) from hive.default.%s", tableName)))
                    .containsExactlyInOrder(row(1L));
            assertThat(env.executeTrino(format("select n_nationkey from hive.default.%s where n_regionkey = 2", tableName)))
                    .containsExactlyInOrder(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertPartitionedBucketed(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_prepared_" + randomNameSuffix();
        try {
            String ctasQuery = "CREATE TABLE hive.default.%s WITH (bucket_count = 4, bucketed_by = ARRAY['n_regionkey'], partitioned_by = ARRAY['part_key']) " +
                    "AS SELECT n_nationkey, n_name, n_regionkey, n_comment, n_name as part_key FROM " + NATION_TABLE;
            env.executeTrinoUpdate(format(ctasQuery, tableName));

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s", tableName))).containsExactlyInOrder(row(25L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0", tableName))).containsExactlyInOrder(row(5L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE part_key='ALGERIA'", tableName))).containsExactlyInOrder(row(1L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0 AND part_key='ALGERIA'", tableName))).containsExactlyInOrder(row(1L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreatePartitionedBucketedTableAsSelect(HiveBasicEnvironment env)
    {
        String tableName = "bucketed_partitioned_nation_" + randomNameSuffix();
        try {
            createBucketedPartitionedNationTable(env, tableName);

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT n_nationkey, n_name, n_regionkey, n_comment, n_name FROM %s", tableName, NATION_TABLE));

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s", tableName))).containsExactlyInOrder(row(25L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0", tableName))).containsExactlyInOrder(row(5L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE part_key='ALGERIA'", tableName))).containsExactlyInOrder(row(1L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0 AND part_key='ALGERIA'", tableName))).containsExactlyInOrder(row(1L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertIntoBucketedTables(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_" + randomNameSuffix();
        try {
            createBucketedNationTable(env, tableName);

            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM %s", tableName, NATION_TABLE));
            // make sure that insert will not overwrite existing data
            env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT * FROM %s", tableName, NATION_TABLE));

            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s", tableName))).containsExactlyInOrder(row(50L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0", tableName))).containsExactlyInOrder(row(10L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreateBucketedTableAsSelect(HiveBasicEnvironment env)
    {
        String tableName = "bucket_nation_prepared_" + randomNameSuffix();
        try {
            // nations has 25 rows and NDV=5 for n_regionkey, setting bucket_count=10 will surely create empty buckets
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s WITH (bucket_count = 10, bucketed_by = ARRAY['n_regionkey']) AS SELECT * FROM %s", tableName, NATION_TABLE));

            QueryResult result = env.executeTrino(format("SELECT * FROM hive.default.%s", tableName));
            // Validate row count and content - all 25 nations with distinct keys and expected region distribution
            assertThat(result).hasRowsCount(25);
            assertThat(env.executeTrino(format("SELECT count(DISTINCT n_nationkey) FROM hive.default.%s", tableName)))
                    .containsExactlyInOrder(row(25L));
            assertThat(env.executeTrino(format("SELECT count(*) FROM hive.default.%s WHERE n_regionkey=0", tableName))).containsExactlyInOrder(row(5L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testBucketingVersion(HiveBasicEnvironment env)
    {
        String defaultValue = "Trino rocks";
        String hiveSelectValue = "ALGERIA";
        String bucketV1HiveSelect = "000001_0";
        String bucketV1TrinoInsert = "000002_0";
        String bucketV2Standard = "000001_0";
        String bucketV2DirectInsert = "bucket_00001";

        List<String> bucketV1HiveSelectNameOptions = ImmutableList.of(bucketV1HiveSelect);
        List<String> bucketV1TrinoInsertNameOptions = ImmutableList.of(bucketV1TrinoInsert);
        List<String> bucketV2NameOptions = ImmutableList.of(bucketV2Standard, bucketV2DirectInsert);

        testBucketingVersionInternal(env, BUCKETED_DEFAULT, defaultValue, true, bucketV2NameOptions);
        testBucketingVersionInternal(env, BUCKETED_V1, hiveSelectValue, false, bucketV1HiveSelectNameOptions);
        testBucketingVersionInternal(env, BUCKETED_V1, defaultValue, true, bucketV1TrinoInsertNameOptions);
        testBucketingVersionInternal(env, BUCKETED_V2, defaultValue, true, bucketV2NameOptions);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testBucketingVersion2HiveInsertValues(HiveBasicEnvironment env)
    {
        String tableName = "test_bucketing_v2_values_" + randomNameSuffix();
        String value = "Trino rocks";
        List<String> bucketV2NameOptions = ImmutableList.of("000001_0", "bucket_00001");
        try {
            createBucketingVersionTable(env, BUCKETED_V2, tableName);
            env.executeHiveUpdate("SET hive.enforce.bucketing = true");
            env.executeHiveUpdate(format("INSERT INTO %s(a) VALUES ('%s')", tableName, value));

            QueryResult result = env.executeTrino(format(
                    "SELECT a, regexp_extract(\"$path\", '^.*/([^_/]+_[^_/]+)(_[^/]+)?$', 1) FROM hive.default.%s",
                    tableName));
            assertThat(result).hasRowsCount(1);
            assertThat(result.rows().get(0).get(0)).isEqualTo(value);
            assertThat(bucketV2NameOptions).contains((String) result.rows().get(0).get(1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testBucketingVersion2HiveInsertSelectProducesCorruption(HiveBasicEnvironment env)
    {
        String tableName = "test_bucketing_v2_select_" + randomNameSuffix();
        String value = "Trino rocks";
        try {
            createBucketingVersionTable(env, BUCKETED_V2, tableName);
            env.executeHiveUpdate("SET hive.enforce.bucketing = true");
            env.executeHiveUpdate(format("INSERT INTO %s(a) SELECT '%s' FROM %s LIMIT 1", tableName, value, NATION_TABLE));

            // Hive 3.x has known bucketing-version / bucketed-insert issues (for example HIVE-21304, HIVE-22098,
            // HIVE-22429, HIVE-18157, HIVE-10151). In particular, INSERT ... SELECT into bucketing_version=2 can
            // write rows to files keyed by V1 hash. Once Hive fixes this behavior for our runtime, update this test.
            assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM hive.default.%s", tableName)))
                    .hasMessageContaining("Hive table is corrupt");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    static Stream<Arguments> testBucketingWithUnsupportedDataTypesDataProvider()
    {
        return cartesianProduct(
                ImmutableList.of(BUCKETED_DEFAULT, BUCKETED_V1, BUCKETED_V2),
                ImmutableList.<String>builder()
                        .add("n_decimal")
                        .add("n_timestamp")
                        .add("n_char")
                        .add("n_binary")
                        .add("n_union")
                        .add("n_struct")
                        .build()).stream()
                .map(list -> Arguments.of(list.get(0), list.get(1)));
    }

    @ParameterizedTest
    @MethodSource("testBucketingWithUnsupportedDataTypesDataProvider")
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testBucketingWithUnsupportedDataTypes(BucketingType bucketingType, String columnToBeBucketed, HiveBasicEnvironment env)
    {
        String tableName = "table_with_unsupported_bucketing_types_" + randomNameSuffix();
        try {
            env.executeHiveUpdate(format("CREATE TABLE %s (" +
                            "n_integer       INT," +
                            "n_decimal       DECIMAL(9, 2)," +
                            "n_timestamp     TIMESTAMP," +
                            "n_char          CHAR(10)," +
                            "n_binary        BINARY," +
                            "n_union         UNIONTYPE<INT,STRING>," +
                            "n_struct        STRUCT<field1:INT,field2:STRING>) " +
                            "CLUSTERED BY (%s) INTO 2 BUCKETS " +
                            "STORED AS ORC " +
                            "%s",
                    tableName,
                    columnToBeBucketed,
                    hiveTableProperties(bucketingType)));

            QueryResult showCreateTableResult = env.executeTrino("SHOW CREATE TABLE hive.default." + tableName);
            assertThat(showCreateTableResult)
                    .hasRowsCount(1);
            String createTableStatement = (String) showCreateTableResult.getOnlyValue();
            assertThat(createTableStatement)
                    .matches(Pattern.compile(format("\\QCREATE TABLE hive.default.%s (\n" +
                                    "   n_integer integer,\n" +
                                    "   n_decimal decimal(9, 2),\n" +
                                    "   n_timestamp timestamp(3),\n" +
                                    "   n_char char(10),\n" +
                                    "   n_binary varbinary,\n" +
                                    "   n_union ROW(tag tinyint, field0 integer, field1 varchar),\n" +
                                    "   n_struct ROW(field1 integer, field2 varchar)\n" +
                                    ")\n" +
                                    "WITH (\\E(?s:.*)" +
                                    "bucket_count = 2,\n(?s:.*)" +
                                    "bucketed_by = ARRAY\\['%s'\\],\n(?s:.*)" +
                                    "bucketing_version = %s,(?s:.*)",
                            tableName,
                            Pattern.quote(columnToBeBucketed),
                            getExpectedBucketVersion(bucketingType))));

            populateRowToHiveTable(
                    env,
                    tableName,
                    ImmutableList.<String>builder()
                            .add("1")
                            .add("CAST(1 AS DECIMAL(9, 2))")
                            .add("CAST('2015-01-01T00:01:00.15' AS TIMESTAMP)")
                            .add("'char value'")
                            .add("unhex('00010203')")
                            .add("create_union(0, 1, 'union value')")
                            .add("named_struct('field1', 1, 'field2', 'Field2')")
                            .build(),
                    Optional.empty());

            assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName)))
                    .hasRowsCount(1);

            assertThatThrownBy(() -> env.executeTrino("SELECT \"$bucket\" FROM hive.default." + tableName))
                    .hasMessageMatching(".*line 1:8: Column '\\$bucket' cannot be resolved.*");

            assertThatThrownBy(() -> env.executeTrinoUpdate(format("INSERT INTO hive.default.%s(n_integer) VALUES (1)", tableName)))
                    .hasMessageMatching(".*Cannot write to a table bucketed on an unsupported type.*");

            String newTableName = "new_" + tableName;

            assertThatThrownBy(() -> env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (LIKE hive.default.%s INCLUDING PROPERTIES)", newTableName, tableName)))
                    .hasMessageMatching(".*Cannot create a table bucketed on an unsupported type.*");

            assertThatThrownBy(() -> env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s (" +
                                    "n_integer       integer," +
                                    "n_decimal       decimal(9, 2)," +
                                    "n_timestamp     timestamp(3)," +
                                    "n_char          char(10)," +
                                    "n_binary        varbinary," +
                                    "n_union         ROW(tag tinyint, field0 integer, field1 varchar)," +
                                    "n_struct        ROW(field1 integer, field2 varchar)) " +
                                    "WITH (" +
                                    "   bucketed_by = ARRAY['%s']," +
                                    "   bucket_count = 2" +
                                    ")",
                            newTableName,
                            columnToBeBucketed)))
                    .hasMessageMatching(".*Cannot create a table bucketed on an unsupported type.*");

            assertThatThrownBy(() -> env.executeTrinoUpdate(format(
                            "CREATE TABLE hive.default.%s WITH (%s) AS SELECT * FROM hive.default.%s",
                            newTableName,
                            bucketingType.getTrinoTableProperties(columnToBeBucketed, 2).stream().collect(joining(",")),
                            tableName)))
                    .hasMessageMatching(".*Cannot create a table bucketed on an unsupported type.*");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS new_" + tableName);
        }
    }

    private void testBucketingVersionInternal(HiveBasicEnvironment env, BucketingType bucketingType, String value, boolean insertWithTrino, List<String> expectedFileNameOptions)
    {
        log.info("Testing with bucketingType=%s, value='%s', insertWithTrino=%s, expectedFileNamePossibilities=%s", bucketingType, value, insertWithTrino, expectedFileNameOptions);

        env.executeHiveUpdate("DROP TABLE IF EXISTS test_bucketing_version");
        createBucketingVersionTable(env, bucketingType, "test_bucketing_version");

        if (insertWithTrino) {
            env.executeTrinoUpdate("INSERT INTO hive.default.test_bucketing_version(a) VALUES ('" + value + "')");
        }
        else {
            env.executeHiveUpdate("SET hive.enforce.bucketing = true");
            env.executeHiveUpdate("INSERT INTO test_bucketing_version(a) SELECT n_name FROM " + NATION_TABLE + " WHERE n_nationkey = 0");
        }

        QueryResult result = env.executeTrino("SELECT a, regexp_extract(\"$path\", '^.*/([^_/]+_[^_/]+)(_[^/]+)?$', 1) FROM hive.default.test_bucketing_version");
        assertThat(result).hasRowsCount(1);
        List<Object> row = result.rows().get(0);
        assertThat(row.get(0)).isEqualTo(value);
        assertThat(expectedFileNameOptions).contains((String) row.get(1));

        env.executeHiveUpdate("DROP TABLE IF EXISTS test_bucketing_version");
    }

    private void createBucketingVersionTable(HiveBasicEnvironment env, BucketingType bucketingType, String tableName)
    {
        env.executeHiveUpdate("" +
                "CREATE TABLE " + tableName + "(a string) " +
                bucketingType.getHiveClustering("a", 4) + " " +
                "STORED AS ORC " +
                hiveTableProperties(bucketingType));
    }

    private String hiveTableProperties(BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.add("'transactional'='false'");
        tableProperties.addAll(bucketingType.getHiveTableProperties());
        return "TBLPROPERTIES(" + join(",", tableProperties.build()) + ")";
    }

    private String getExpectedBucketVersion(BucketingType bucketingType)
    {
        switch (bucketingType) {
            case BUCKETED_DEFAULT:
                return "2";
            case BUCKETED_V1:
                return "1";
            case BUCKETED_V2:
                return "2";
            default:
                throw new UnsupportedOperationException("Not supported for " + bucketingType);
        }
    }

    // Table creation helpers

    private static void createBucketedNationTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeHiveUpdate("CREATE TABLE " + tableName + "(" +
                "n_nationkey     BIGINT," +
                "n_name          STRING," +
                "n_regionkey     BIGINT," +
                "n_comment       STRING) " +
                "CLUSTERED BY (n_regionkey) " +
                "INTO 2 BUCKETS " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                "TBLPROPERTIES ('bucketing_version'='1')");
    }

    private static void createBucketedSortedNationTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeHiveUpdate("CREATE TABLE " + tableName + "(" +
                "n_nationkey     BIGINT," +
                "n_name          STRING," +
                "n_regionkey     BIGINT," +
                "n_comment       STRING) " +
                "CLUSTERED BY (n_regionkey) " +
                "SORTED BY (n_regionkey) " +
                "INTO 2 BUCKETS " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                "TBLPROPERTIES ('bucketing_version'='1')");
    }

    private static void createBucketedPartitionedNationTable(HiveBasicEnvironment env, String tableName)
    {
        env.executeHiveUpdate("CREATE TABLE " + tableName + "(" +
                "n_nationkey     BIGINT," +
                "n_name          STRING," +
                "n_regionkey     BIGINT," +
                "n_comment       STRING) " +
                "PARTITIONED BY (part_key STRING) " +
                "CLUSTERED BY (n_regionkey) " +
                "INTO 2 BUCKETS " +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                "TBLPROPERTIES ('bucketing_version'='1')");
    }

    // Data population helpers

    private static void populateRowToHiveTable(HiveBasicEnvironment env, String destination, List<String> values, Optional<String> partition)
    {
        String queryStatement = "INSERT INTO TABLE " + destination +
                (partition.isPresent() ? format(" PARTITION (%s) ", partition.get()) : " ") +
                "SELECT " + join(",", values) + " FROM " + NATION_TABLE + " LIMIT 1";

        env.executeHiveUpdate("set hive.enforce.bucketing = true");
        env.executeHiveUpdate("set hive.enforce.sorting = true");
        env.executeHiveUpdate(queryStatement);
    }

    private static void populateHivePartitionedTable(HiveBasicEnvironment env, String destination, String source, String partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s PARTITION (%s) SELECT * FROM %s", destination, partition, source);

        env.executeHiveUpdate("set hive.enforce.bucketing = true");
        env.executeHiveUpdate("set hive.enforce.sorting = true");
        env.executeHiveUpdate(queryStatement);
    }

    private static void populateHiveTable(HiveBasicEnvironment env, String destination, String source)
    {
        env.executeHiveUpdate("set hive.enforce.bucketing = true");
        env.executeHiveUpdate("set hive.enforce.sorting = true");
        env.executeHiveUpdate(format("INSERT INTO TABLE %s SELECT * FROM %s", destination, source));
    }
}
