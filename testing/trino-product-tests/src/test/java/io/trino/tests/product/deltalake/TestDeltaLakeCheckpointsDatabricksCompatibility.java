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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeCheckpointsDatabricksCompatibility
{
    private static final Pattern LOG_JSON_PATTERN = Pattern.compile("([0-9]{20})\\.json$");

    @Test
    @TestGroup.DeltaLakeDatabricks143
    @TestGroup.DeltaLakeDatabricks154
    @TestGroup.DeltaLakeDatabricks164
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksUsesCheckpointInterval(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        env.executeTrinoSql(format("CREATE TABLE delta.default.%s (a_number bigint, a_string varchar) " +
                "WITH (" +
                "      location = 's3://%s/%s'," +
                "      partitioned_by = ARRAY['a_number']," +
                "      checkpoint_interval = 3" +
                ")", tableName, env.getBucketName(), tableDirectory));

        try {
            assertThat((String) env.executeDatabricksSql("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("'delta.checkpointInterval' = '3'");

            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 'databricks')");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (2, 'databricks')");
            assertThat(listCheckpointFiles(env.getBucketName(), tableDirectory)).isEmpty();
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'databricks'"))
                    .hasNoRows();

            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, 'databricks')");
            assertThat(listCheckpointFiles(env.getBucketName(), tableDirectory)).hasSize(1);

            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (1, 'ala'), (2, 'kota')");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (5, 'osla'), (6, 'lwa')");
            assertThat(listCheckpointFiles(env.getBucketName(), tableDirectory)).hasSize(2);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoCheckpointMinMaxStatisticsForRowType(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_trino_" + randomNameSuffix();
        testCheckpointMinMaxStatisticsForRowType(env, sql -> env.executeTrinoSql(sql), tableName, "delta.default." + tableName);
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksCheckpointMinMaxStatisticsForRowType(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_databricks_" + randomNameSuffix();
        testCheckpointMinMaxStatisticsForRowType(env, sql -> env.executeDatabricksSql(sql), tableName, "default." + tableName);
    }

    private void testCheckpointMinMaxStatisticsForRowType(DeltaLakeDatabricksEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<Row> expectedRows = ImmutableList.of(
                row(1, "ala"),
                row(2, "kota"),
                row(3, "osla"),
                row(4, "zulu"));

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%s" +
                        "      (id INT, root STRUCT<entry_one : INT, entry_two : STRING>)" +
                        "      USING DELTA " +
                        "      LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "      TBLPROPERTIES (delta.checkpointInterval = 1)",
                tableName, env.getBucketName()));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, STRUCT(1,'ala')), (2, STRUCT(2, 'kota'))");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, STRUCT(3, 'osla'))");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (4, STRUCT(4, 'zulu'))");

            assertThat(listCheckpointFiles(env.getBucketName(), "databricks-compatibility-test-" + tableName)).hasSize(3);
            assertTransactionLogVersion(env.getBucketName(), tableName, 3);
            assertThat(env.executeDatabricksSql("SELECT DISTINCT root.entry_one, root.entry_two FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT DISTINCT root.entry_one, root.entry_two FROM delta.default." + tableName)).containsOnly(expectedRows);

            sqlExecutor.accept("DELETE FROM " + qualifiedTableName + " WHERE id = 4");
            assertLastEntryIsCheckpointed(env.getBucketName(), tableName);

            String explainSelectMax = getExplainPlanText(env.executeDatabricksSql("EXPLAIN SELECT max(root.entry_one) FROM default." + tableName));
            String column = "root.entry_one";
            assertThat(explainSelectMax).matches("== Physical Plan ==\\s*LocalTableScan \\[max\\(" + column + "\\).*]\\s*");

            List<Row> maxMin = ImmutableList.of(row(3, "ala"));
            assertThat(env.executeDatabricksSql("SELECT max(root.entry_one), min(root.entry_two) FROM default." + tableName)).containsOnly(maxMin);
            assertThat(env.executeTrinoSql("SELECT max(root.entry_one), min(root.entry_two) FROM delta.default." + tableName)).containsOnly(maxMin);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoCheckpointNullStatisticsForRowType(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_row_compat_trino_" + randomNameSuffix();
        testCheckpointNullStatisticsForRowType(env, sql -> env.executeTrinoSql(sql), tableName, "delta.default." + tableName);
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksCheckpointNullStatisticsForRowType(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_row_compat_databricks_" + randomNameSuffix();
        testCheckpointNullStatisticsForRowType(env, sql -> env.executeDatabricksSql(sql), tableName, "default." + tableName);
    }

    private void testCheckpointNullStatisticsForRowType(DeltaLakeDatabricksEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<Row> expectedRows = ImmutableList.of(
                row(1, "ala"),
                row(2, "kota"),
                row(null, null),
                row(4, "zulu"));

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%s" +
                        "      (id INT, root STRUCT<entry_one : INT, entry_two : STRING>)" +
                        "      USING DELTA " +
                        "      LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "      TBLPROPERTIES (delta.checkpointInterval = 1)",
                tableName, env.getBucketName()));
        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, STRUCT(1,'ala')), (2, STRUCT(2, 'kota'))");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, STRUCT(null, null))");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (4, STRUCT(4, 'zulu'))");

            assertThat(listCheckpointFiles(env.getBucketName(), "databricks-compatibility-test-" + tableName)).hasSize(3);
            assertTransactionLogVersion(env.getBucketName(), tableName, 3);
            assertThat(env.executeDatabricksSql("SELECT DISTINCT root.entry_one, root.entry_two FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT DISTINCT root.entry_one, root.entry_two FROM delta.default." + tableName)).containsOnly(expectedRows);

            sqlExecutor.accept("DELETE FROM " + qualifiedTableName + " WHERE id = 4");
            assertLastEntryIsCheckpointed(env.getBucketName(), tableName);

            String explainCountNotNull = getExplainPlanText(env.executeDatabricksSql("EXPLAIN SELECT count(root.entry_two) FROM default." + tableName));
            String column = "root.entry_two";
            assertThat(explainCountNotNull).matches("== Physical Plan ==\\s*LocalTableScan \\[count\\(" + column + "\\).*]\\s*");

            assertThat(env.executeDatabricksSql("SELECT count(root.entry_two) FROM default." + tableName)).containsOnly(row(2));
            assertThat(env.executeTrinoSql("SELECT count(root.entry_two) FROM delta.default." + tableName)).containsOnly(row(2));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @ParameterizedTest
    @MethodSource("testDatabricksCheckpointWriteStatsAsJsonDataProvider")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksWriteStatsAsJsonEnabled(String type, String inputValue, Double nullsFraction, Object statsValue, DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_enabled_databricks_" + randomNameSuffix();
        testWriteStatsAsJsonEnabled(env, sql -> env.executeDatabricksSql(sql), tableName, "default." + tableName, type, inputValue, null, null, nullsFraction, statsValue);
    }

    private void testWriteStatsAsJsonEnabled(DeltaLakeDatabricksEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, String type, String inputValue, Double dataSize, Double distinctValues, Double nullsFraction, Object statsValue)
    {
        String createTableSql = format(
                "CREATE TABLE default.%s" +
                        "(col %s) " +
                        "USING DELTA " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 2, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, type, env.getBucketName());

        env.executeDatabricksSql(createTableSql);

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            env.executeDatabricksSql("" +
                    "ALTER TABLE default." + tableName + " SET TBLPROPERTIES (" +
                    "'delta.checkpoint.writeStatsAsJson' = true, " +
                    "'delta.checkpoint.writeStatsAsStruct' = false)");

            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            assertThat(env.executeTrinoSql("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("col", dataSize, distinctValues, nullsFraction, null, statsValue, statsValue),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private String getExplainPlanText(QueryResult explainResult)
    {
        Row explainRow = explainResult.getRows().getFirst();
        return (String) explainRow.getValue(explainRow.getValues().size() - 1);
    }

    static Stream<Arguments> testDatabricksCheckpointWriteStatsAsJsonDataProvider()
    {
        return Stream.of(
                Arguments.of("boolean", "true", 0.0, null),
                Arguments.of("integer", "1", 0.0, "1"),
                Arguments.of("tinyint", "2", 0.0, "2"),
                Arguments.of("smallint", "3", 0.0, "3"),
                Arguments.of("bigint", "1000", 0.0, "1000"),
                Arguments.of("real", "0.1", 0.0, "0.1"),
                Arguments.of("double", "1.0", 0.0, "1.0"),
                Arguments.of("decimal(3,2)", "3.14", 0.0, "3.14"),
                Arguments.of("decimal(30,1)", "12345", 0.0, "12345.0"),
                Arguments.of("string", "'test'", 0.0, null),
                Arguments.of("binary", "X'65683F'", 0.0, null),
                Arguments.of("date", "date '2021-02-03'", 0.0, "2021-02-03"),
                Arguments.of("timestamp", "timestamp '2001-08-22 11:04:05.321 UTC'", 0.0, "2001-08-22 11:04:05.321 UTC"),
                Arguments.of("array<int>", "array(1)", 0.0, null),
                Arguments.of("map<string,int>", "map('key1', 1, 'key2', 2)", 0.0, null),
                Arguments.of("struct<x bigint>", "named_struct('x', 1)", null, null));
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksWriteStatsAsStructEnabled(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_enabled_databricks_" + randomNameSuffix();
        testWriteStatsAsStructEnabled(env, sql -> env.executeDatabricksSql(sql), tableName, "default." + tableName, null, null);
    }

    private void testWriteStatsAsStructEnabled(DeltaLakeDatabricksEnvironment env, Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, Double dataSize, Double distinctValues)
    {
        env.executeDatabricksSql(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 1, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, env.getBucketName()));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(env.executeTrinoSql("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, null, null),
                            row("a_string", dataSize, distinctValues, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private void assertTransactionLogVersion(String bucketName, String tableName, int expectedVersion)
    {
        String logDir = "databricks-compatibility-test-" + tableName + "/_delta_log";
        Optional<Integer> maxVersion = listS3Objects(bucketName, logDir).stream()
                .map(LOG_JSON_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> Integer.parseInt(matcher.group(1)))
                .max(Comparator.naturalOrder());
        assertThat(maxVersion).contains(expectedVersion);
    }

    private void assertLastEntryIsCheckpointed(String bucketName, String tableName)
    {
        String logDir = "databricks-compatibility-test-" + tableName + "/_delta_log";
        List<String> jsonEntries = listS3Objects(bucketName, logDir).stream()
                .filter(path -> path.endsWith(".json"))
                .toList();
        assertThat(jsonEntries).isNotEmpty();
        String lastJsonEntry = jsonEntries.stream().max(String::compareTo).orElseThrow();

        List<String> checkpointEntries = listCheckpointFiles(bucketName, "databricks-compatibility-test-" + tableName);
        assertThat(checkpointEntries).isNotEmpty();
        String lastCheckpointEntry = checkpointEntries.stream()
                .map(path -> path.substring(path.lastIndexOf('/') + 1))
                .max(String::compareTo)
                .orElseThrow();

        assertThat(lastJsonEntry.replace(".json", ""))
                .isEqualTo(lastCheckpointEntry.replace(".checkpoint.parquet", ""));
    }

    private List<String> listCheckpointFiles(String bucketName, String tableDirectory)
    {
        return listS3Objects(bucketName, tableDirectory + "/_delta_log").stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .toList();
    }

    private List<String> listS3Objects(String bucketName, String prefix)
    {
        try (S3Client s3 = createS3Client()) {
            return s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(prefix)
                            .build())
                    .contents().stream()
                    .map(item -> item.key().substring(prefix.endsWith("/") ? prefix.length() : prefix.length() + 1))
                    .toList();
        }
    }

    private static S3Client createS3Client()
    {
        String accessKey = requireEnv("TRINO_AWS_ACCESS_KEY_ID");
        String secretKey = requireEnv("TRINO_AWS_SECRET_ACCESS_KEY");
        Optional<String> sessionToken = Optional.ofNullable(System.getenv("TRINO_AWS_SESSION_TOKEN"))
                .filter(token -> !token.isBlank());

        StaticCredentialsProvider credentialsProvider;
        if (sessionToken.isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey, secretKey, sessionToken.orElseThrow()));
        }
        else {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }

        return S3Client.builder()
                .region(Region.of(requireEnv("AWS_REGION")))
                .credentialsProvider(credentialsProvider)
                .build();
    }
}
