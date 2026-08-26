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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.getDatabricksRuntimeVersion;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_122_RUNTIME_VERSION;
import static java.lang.String.format;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeWriteDatabricksCompatibilityDatabricks
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testUpdateCompatibility(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_update_compatibility_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (a int, b int, c int) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6), (5, 6, 7)");
            env.executeTrinoSql("UPDATE delta.default." + tableName + " SET b = b * 2 WHERE a % 2 = 1");

            List<Row> expectedRows = List.of(
                    row(1, 4, 3),
                    row(2, 3, 4),
                    row(3, 8, 5),
                    row(4, 5, 6),
                    row(5, 12, 7));

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDeleteCompatibility(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_delete_compatibility_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            env.executeTrinoSql("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<Row> expectedRows = List.of(row(1, 2), row(3, 4), row(5, 6));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDeleteOnPartitionedTableCompatibility(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_delete_on_partitioned_table_compatibility_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s' PARTITIONED BY (b)",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            env.executeTrinoSql("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<Row> expectedRows = List.of(row(1, 2), row(3, 4), row(5, 6));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDeleteOnPartitionKeyCompatibility(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_delete_on_partitioned_table_compatibility_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s' PARTITIONED BY (b)",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            env.executeTrinoSql("DELETE FROM delta.default." + tableName + " WHERE b % 2 = 0");

            List<Row> expectedRows = List.of(row(2, 3), row(4, 5));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"downpart", "UPPART"})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCaseUpdateInPartition(String partitionColumn, DeltaLakeDatabricksEnvironment env)
    {
        try (CaseTestTable table = new CaseTestTable(env, "update_case_compat", partitionColumn, List.of(
                testRow(1, 1, 0),
                testRow(2, 2, 0),
                testRow(3, 3, 1)))) {
            env.executeTrinoSql(format("UPDATE delta.default.%s SET upper = 0 WHERE lower = 1", table.name()));
            assertTable(env, table, table.rows().map(testRow -> testRow.lower() == 1 ? testRow.withUpper(0) : testRow));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"downpart", "UPPART"})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCaseUpdatePartitionColumnFails(String partitionColumn, DeltaLakeDatabricksEnvironment env)
    {
        try (CaseTestTable table = new CaseTestTable(env, "update_case_compat", partitionColumn, List.of(testRow(1, 1, 1)))) {
            env.executeTrinoSql(format("UPDATE delta.default.%s SET %s = 0 WHERE lower = 1", table.name(), partitionColumn));
            assertTable(env, table, table.rows().map(testRow -> testRow.withPartition(0)));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"downpart", "UPPART"})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCaseDeletePartialPartition(String partitionColumn, DeltaLakeDatabricksEnvironment env)
    {
        try (CaseTestTable table = new CaseTestTable(env, "delete_case_compat", partitionColumn, List.of(
                testRow(1, 1, 0),
                testRow(2, 2, 0),
                testRow(3, 3, 1)))) {
            env.executeTrinoSql(format("DELETE FROM delta.default.%s WHERE lower = 1", table.name()));
            assertTable(env, table, table.rows().filter(not(testRow -> testRow.lower() == 1)));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"downpart", "UPPART"})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCaseDeleteEntirePartition(String partitionColumn, DeltaLakeDatabricksEnvironment env)
    {
        try (CaseTestTable table = new CaseTestTable(env, "delete_case_compat", partitionColumn, List.of(
                testRow(1, 1, 0),
                testRow(2, 2, 0),
                testRow(3, 3, 1)))) {
            env.executeTrinoSql(format("DELETE FROM delta.default.%s WHERE %s = 0", table.name(), partitionColumn));
            assertTable(env, table, table.rows().filter(not(testRow -> testRow.partition() == 0)));
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoRespectsDatabricksSettingNonNullableColumn(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_databricks_table_with_nonnullable_columns_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (non_nullable_col INT NOT NULL, nullable_col INT) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1, 2)");
            assertThatThrownBy(() -> env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(row(1, 2));
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksRespectsTrinoSettingNonNullableColumn(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_trino_table_with_nonnullable_columns_" + randomNameSuffix();

        env.executeTrinoSql("CREATE TABLE delta.default.\"" + tableName + "\" " +
                "(non_nullable_col INT NOT NULL, nullable_col INT) " +
                "WITH (location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "')");

        try {
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            assertThatThrownBy(() -> env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(row(1, 2));
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2));
        }
        finally {
            env.executeTrinoSql("DROP TABLE delta.default." + tableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testInsertingIntoDatabricksTableWithAddedNotNullConstraint(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_databricks_table_altered_after_initial_write_" + randomNameSuffix();

        env.executeDatabricksSql(format(
                "CREATE TABLE default.%1$s (non_nullable_col INT, nullable_col INT) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation(env)));

        try {
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            env.executeDatabricksSql("ALTER TABLE default." + tableName + " ALTER COLUMN non_nullable_col SET NOT NULL");
            assertThatThrownBy(() -> env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName)).containsOnly(row(1, 2));
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName)).containsOnly(row(1, 2));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @Test
    @TestGroup.DeltaLakeExclude173
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testTrinoVacuumRemoveChangeDataFeedFiles(DeltaLakeDatabricksEnvironment env)
    {
        testVacuumRemoveChangeDataFeedFiles(env, tableName -> {
            env.executeTrinoSqlStatements(
                    "SET SESSION delta.vacuum_min_retention = '0s'",
                    "CALL delta.system.vacuum('default', '" + tableName + "', '0s')");
        });
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testDatabricksVacuumRemoveChangeDataFeedFiles(DeltaLakeDatabricksEnvironment env)
    {
        testVacuumRemoveChangeDataFeedFiles(env, tableName -> {
            env.executeDatabricksSqlStatements(
                    "SET spark.databricks.delta.retentionDurationCheck.enabled = false",
                    "VACUUM default." + tableName + " RETAIN 0 HOURS");
        });
    }

    private void testVacuumRemoveChangeDataFeedFiles(DeltaLakeDatabricksEnvironment env, Consumer<String> vacuumExecutor)
    {
        String tableName = "test_vacuum_ignore_cdf_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-" + tableName;
        String changeDataPrefix = directoryName + "/_change_data";

        env.executeDatabricksSql("CREATE TABLE default." + tableName + " (a INT) " +
                "USING DELTA " +
                "LOCATION 's3://" + env.getBucketName() + "/" + directoryName + "'" +
                "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

        try {
            env.executeDatabricksSql("INSERT INTO " + tableName + " VALUES (1)");
            env.executeDatabricksSql("UPDATE " + tableName + " SET a = 2");

            List<S3Object> initial = listObjects(env.getBucketName(), changeDataPrefix);
            assertThat(initial).hasSize(1);

            vacuumExecutor.accept(tableName);

            List<S3Object> summaries = listObjects(env.getBucketName(), changeDataPrefix);
            assertThat(summaries).hasSizeBetween(0, 1);
            if (!summaries.isEmpty()) {
                assertThat(getDatabricksRuntimeVersion(env).orElseThrow().isAtLeast(DATABRICKS_122_RUNTIME_VERSION)).isTrue();
                S3Object object = summaries.getFirst();
                assertThat(object.key()).endsWith(changeDataPrefix + "/");
                assertThat(object.size()).isEqualTo(0L);
            }
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    private static List<S3Object> listObjects(String bucketName, String prefix)
    {
        try (S3Client s3 = createS3Client()) {
            return s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(prefix)
                            .build())
                    .contents().stream()
                    .sorted(Comparator.comparing(S3Object::key))
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

    private static String getBaseLocation(DeltaLakeDatabricksEnvironment env)
    {
        return "s3://" + env.getBucketName() + "/databricks-compatibility-test-";
    }

    private static TestRow testRow(Integer lower, Integer upper, Integer partition)
    {
        return new TestRow(lower, upper, partition);
    }

    private static void assertTable(DeltaLakeDatabricksEnvironment env, CaseTestTable table, Stream<TestRow> expectedRows)
    {
        assertTable(env, table, expectedRows.map(TestRow::toRow).collect(toList()));
    }

    private static void assertTable(DeltaLakeDatabricksEnvironment env, CaseTestTable table, List<Row> expectedRows)
    {
        SoftAssertions softly = new SoftAssertions();

        softly.check(() ->
                assertThat(env.executeDatabricksSql("SHOW COLUMNS IN default." + table.name()))
                        .as("Correct columns after update")
                        .containsOnly(table.columns().stream().map(column -> row(column)).collect(toList())));

        softly.check(() ->
                assertThat(env.executeDatabricksSql("SELECT * FROM default." + table.name()))
                        .as("Data accessible via Databricks")
                        .containsOnly(expectedRows));

        softly.check(() ->
                assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + table.name()))
                        .as("Data accessible via Trino")
                        .containsOnly(expectedRows));

        softly.assertAll();
    }

    private static final class CaseTestTable
            implements AutoCloseable
    {
        private final DeltaLakeDatabricksEnvironment env;
        private final String name;
        private final List<String> columns;
        private final List<TestRow> rows;

        CaseTestTable(DeltaLakeDatabricksEnvironment env, String namePrefix, String partitionColumnName, Collection<TestRow> rows)
        {
            this.env = env;
            this.name = namePrefix + "_" + randomNameSuffix();
            this.columns = List.of("lower", "UPPER", partitionColumnName);
            this.rows = List.copyOf(rows);

            env.executeDatabricksSql(format(
                    "CREATE TABLE default.%1$s (lower int, UPPER int, %3$s int)\n" +
                            "USING DELTA\n" +
                            "PARTITIONED BY (%3$s)\n" +
                            "LOCATION '%2$s%1$s'\n",
                    name,
                    getBaseLocation(env),
                    partitionColumnName));

            env.executeDatabricksSql(format(
                    "INSERT INTO default.%s VALUES %s",
                    name,
                    rows.stream().map(TestRow::asValues).collect(joining(", "))));
        }

        String name()
        {
            return name;
        }

        List<String> columns()
        {
            return columns;
        }

        Stream<TestRow> rows()
        {
            return rows.stream();
        }

        @Override
        public void close()
        {
            dropDeltaTableWithRetry(env, "default." + name);
        }
    }

    private record TestRow(Integer lower, Integer upper, Integer partition)
    {
        TestRow withUpper(Integer newValue)
        {
            return new TestRow(lower, newValue, partition);
        }

        TestRow withPartition(Integer newValue)
        {
            return new TestRow(lower, upper, newValue);
        }

        String asValues()
        {
            return format("(%s, %s, %s)", lower, upper, partition);
        }

        Row toRow()
        {
            return row(lower, upper, partition);
        }
    }
}
