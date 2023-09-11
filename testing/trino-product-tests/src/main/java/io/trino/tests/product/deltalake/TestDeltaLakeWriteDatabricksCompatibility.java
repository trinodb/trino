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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_104;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_122_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeWriteDatabricksCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testUpdateCompatibility()
    {
        String tableName = "test_update_compatibility_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (a int, b int, c int) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation()));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6), (5, 6, 7)");
            onTrino().executeQuery("UPDATE delta.default." + tableName + " SET b = b * 2 WHERE a % 2 = 1");

            List<QueryAssert.Row> expectedRows = List.of(
                    row(1, 4, 3),
                    row(2, 3, 4),
                    row(3, 8, 5),
                    row(4, 5, 6),
                    row(5, 12, 7));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteCompatibility()
    {
        String tableName = "test_delete_compatibility_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation()));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<QueryAssert.Row> expectedRows = List.of(
                    row(1, 2),
                    row(3, 4),
                    row(5, 6));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteOnPartitionedTableCompatibility()
    {
        String tableName = "test_delete_on_partitioned_table_compatibility_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s' PARTITIONED BY (b)",
                tableName,
                getBaseLocation()));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a % 2 = 0");

            List<QueryAssert.Row> expectedRows = List.of(
                    row(1, 2),
                    row(3, 4),
                    row(5, 6));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDeleteOnPartitionKeyCompatibility()
    {
        String tableName = "test_delete_on_partitioned_table_compatibility_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (a int, b int) USING DELTA LOCATION '%2$s%1$s' PARTITIONED BY (b)",
                tableName,
                getBaseLocation()));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE b % 2 = 0");

            List<QueryAssert.Row> expectedRows = List.of(row(2, 3), row(4, 5));

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    // Test partition case sensitivity when updating
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "partition_column_names")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCaseUpdateInPartition(String partitionColumn)
    {
        try (CaseTestTable table = new CaseTestTable("update_case_compat", partitionColumn, List.of(
                row(1, 1, 0),
                row(2, 2, 0),
                row(3, 3, 1)))) {
            onTrino().executeQuery(format("UPDATE delta.default.%s SET upper = 0 WHERE lower = 1", table.name()));

            assertTable(table, table.rows().map(row -> row.lower() == 1 ? row.withUpper(0) : row));
        }
    }

    // Test that the correct error is generated when attempting to update the partition columns
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "partition_column_names")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCaseUpdatePartitionColumnFails(String partitionColumn)
    {
        try (CaseTestTable table = new CaseTestTable("update_case_compat", partitionColumn, List.of(row(1, 1, 1)))) {
            // TODO: The test fails for uppercase columns because the statement analyzer compares the column name case-sensitively.
            if (!partitionColumn.equals(partitionColumn.toLowerCase(ENGLISH))) {
                assertQueryFailure(() -> onTrino().executeQuery(format("UPDATE delta.default.%s SET %s = 0 WHERE lower = 1", table.name(), partitionColumn)))
                        .hasMessageMatching(".*The UPDATE SET target column .* doesn't exist");
            }
            else {
                onTrino().executeQuery(format("UPDATE delta.default.%s SET %s = 0 WHERE lower = 1", table.name(), partitionColumn));
                assertTable(table, table.rows().map(row -> row.withPartition(0)));
            }
        }
    }

    // Delete within a partition
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "partition_column_names")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCaseDeletePartialPartition(String partitionColumn)
    {
        try (CaseTestTable table = new CaseTestTable("delete_case_compat", partitionColumn, List.of(
                row(1, 1, 0),
                row(2, 2, 0),
                row(3, 3, 1)))) {
            onTrino().executeQuery(format("DELETE FROM delta.default.%s WHERE lower = 1", table.name()));
            assertTable(table, table.rows().filter(not(row -> row.lower() == 1)));
        }
    }

    // Delete an entire partition
    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "partition_column_names")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testCaseDeleteEntirePartition(String partitionColumn)
    {
        try (CaseTestTable table = new CaseTestTable("delete_case_compat", partitionColumn, List.of(
                row(1, 1, 0),
                row(2, 2, 0),
                row(3, 3, 1)))) {
            onTrino().executeQuery(format("DELETE FROM delta.default.%s WHERE %s = 0", table.name(), partitionColumn));
            assertTable(table, table.rows().filter(not(row -> row.partition() == 0)));
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoRespectsDatabricksSettingNonNullableColumn()
    {
        String tableName = "test_databricks_table_with_nonnullable_columns_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (non_nullable_col INT NOT NULL, nullable_col INT) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation()));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 2)");
            assertQueryFailure(() -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksRespectsTrinoSettingNonNullableColumn()
    {
        String tableName = "test_trino_table_with_nonnullable_columns_" + randomNameSuffix();

        onTrino().executeQuery("CREATE TABLE delta.default.\"" + tableName + "\" " +
                "(non_nullable_col INT NOT NULL, nullable_col INT) " +
                "WITH (location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "')");

        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            assertQueryFailure(() -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
        }
        finally {
            onTrino().executeQuery("DROP TABLE delta.default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testInsertingIntoDatabricksTableWithAddedNotNullConstraint()
    {
        String tableName = "test_databricks_table_altered_after_initial_write_" + randomNameSuffix();

        onDelta().executeQuery(format(
                "CREATE TABLE default.%1$s (non_nullable_col INT, nullable_col INT) USING DELTA LOCATION '%2$s%1$s'",
                tableName,
                getBaseLocation()));

        try {
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)");
            onDelta().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN non_nullable_col SET NOT NULL");
            assertQueryFailure(() -> onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (null, 4)"))
                    .hasMessageContaining("NOT NULL constraint violated for column: non_nullable_col");
            assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (null, 5)"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column: non_nullable_col");

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(row(1, 2));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(row(1, 2));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoVacuumRemoveChangeDataFeedFiles()
    {
        testVacuumRemoveChangeDataFeedFiles(tableName -> {
            onTrino().executeQuery("SET SESSION delta.vacuum_min_retention = '0s'");
            onTrino().executeQuery("CALL delta.system.vacuum('default', '" + tableName + "', '0s')");
        });
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksVacuumRemoveChangeDataFeedFiles()
    {
        testVacuumRemoveChangeDataFeedFiles(tableName -> {
            onDelta().executeQuery("SET spark.databricks.delta.retentionDurationCheck.enabled = false");
            onDelta().executeQuery("VACUUM default." + tableName + " RETAIN 0 HOURS");
        });
    }

    private void testVacuumRemoveChangeDataFeedFiles(Consumer<String> vacuumExecutor)
    {
        String tableName = "test_vacuum_ignore_cdf_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-" + tableName;
        String changeDataPrefix = directoryName + "/_change_data";

        onDelta().executeQuery("CREATE TABLE default." + tableName + " (a INT) " +
                "USING DELTA " +
                "LOCATION '" + ("s3://" + bucketName + "/" + directoryName) + "'" +
                "TBLPROPERTIES (delta.enableChangeDataFeed = true)");

        try {
            // Executing some statements to create _change_data directory
            onDelta().executeQuery("INSERT INTO " + tableName + " VALUES (1)");
            onDelta().executeQuery("UPDATE " + tableName + " SET a = 2");

            assertThat(s3.listObjectsV2(bucketName, changeDataPrefix).getObjectSummaries()).hasSize(1);

            // Vacuum procedure should remove files in _change_data directory
            // https://docs.delta.io/2.1.0/delta-change-data-feed.html#change-data-storage
            vacuumExecutor.accept(tableName);

            List<S3ObjectSummary> summaries = s3.listObjectsV2(bucketName, changeDataPrefix).getObjectSummaries();
            assertThat(summaries).hasSizeBetween(0, 1);
            if (!summaries.isEmpty()) {
                // Databricks version >= 12.2 keep an empty _change_data directory
                DatabricksVersion databricksRuntimeVersion = getDatabricksRuntimeVersion().orElseThrow();
                assertThat(databricksRuntimeVersion.isAtLeast(DATABRICKS_122_RUNTIME_VERSION)).isTrue();
                S3ObjectSummary summary = summaries.get(0);
                assertThat(summary.getKey()).endsWith(changeDataPrefix + "/");
                assertThat(summary.getSize()).isEqualTo(0);
            }
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_EXCLUDE_104, DELTA_LAKE_EXCLUDE_113, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testVacuumUnsupportedWriterVersion()
    {
        String tableName = "test_vacuum_unsupported_writer_version_" + randomNameSuffix();
        String directoryName = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(a INT)" +
                "USING DELTA " +
                "LOCATION '" + ("s3://" + bucketName + "/" + directoryName) + "'" +
                "TBLPROPERTIES ('delta.minWriterVersion'='7')");
        try {
            assertThatThrownBy(() -> onTrino().executeQuery("CALL delta.system.vacuum('default', '" + tableName + "', '7d')"))
                    .hasMessageContaining("Cannot execute vacuum procedure with 7 writer version");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider(name = "partition_column_names")
    public static Object[][] partitionColumns()
    {
        return new Object[][] {{"downpart"}, {"UPPART"}};
    }

    private static QueryAssert.Row row(Integer a, Integer b)
    {
        return QueryAssert.Row.row(a, b);
    }

    private static TestRow row(Integer lower, Integer upper, Integer partition)
    {
        return new TestRow(lower, upper, partition);
    }

    private static void assertTable(CaseTestTable table, Stream<? extends QueryAssert.Row> expectedRows)
    {
        assertTable(table, expectedRows.collect(toList()));
    }

    private static void assertTable(CaseTestTable table, List<QueryAssert.Row> expectedRows)
    {
        SoftAssertions softly = new SoftAssertions();

        softly.check(() ->
                assertThat(onDelta().executeQuery("SHOW COLUMNS IN " + table.name()))
                        .as("Correct columns after update")
                        .containsOnly(table.columns().stream().map(QueryAssert.Row::row).collect(toList())));

        softly.check(() ->
                assertThat(onDelta().executeQuery("SELECT * FROM default." + table.name()))
                        .as("Data accessible via Databricks")
                        .containsOnly(expectedRows));

        softly.check(() ->
                assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + table.name()))
                        .as("Data accessible via Trino")
                        .containsOnly(expectedRows));

        softly.assertAll();
    }

    private String getBaseLocation()
    {
        return "s3://" + bucketName + "/databricks-compatibility-test-";
    }

    /**
     * Creates a test table with three integer columns.
     *
     * <p>The first column is named {@code lower}, the second {@code UPPER},
     * and the third column is named according to the {@code partitionColumnName}
     * parameter. The table is partitioned on the third column.
     */
    private class CaseTestTable
            implements AutoCloseable
    {
        private final String name;
        private final List<String> columns;
        private final Collection<TestRow> rows;

        CaseTestTable(String namePrefix, String partitionColumnName, Collection<TestRow> rows)
        {
            this.name = namePrefix + "_" + randomNameSuffix();
            this.columns = List.of("lower", "UPPER", partitionColumnName);
            this.rows = List.copyOf(rows);

            onDelta().executeQuery(format(
                    "CREATE TABLE default.%1$s (lower int, UPPER int, %3$s int)\n"
                            + "USING DELTA\n"
                            + "PARTITIONED BY (%3$s)\n"
                            + "LOCATION '%2$s%1$s'\n",
                    name,
                    getBaseLocation(),
                    partitionColumnName));

            onDelta().executeQuery(format(
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
            dropDeltaTableWithRetry("default." + name);
        }
    }

    private static class TestRow
            extends QueryAssert.Row
    {
        private Integer lower;
        private Integer upper;
        private Integer partition;

        private TestRow(Integer lower, Integer upper, Integer partition)
        {
            super(List.of(lower, upper, partition));
            this.lower = lower;
            this.upper = upper;
            this.partition = partition;
        }

        public Integer lower()
        {
            return lower;
        }

        public Integer upper()
        {
            return upper;
        }

        public Integer partition()
        {
            return partition;
        }

        public TestRow withLower(Integer newValue)
        {
            return new TestRow(newValue, upper, partition);
        }

        public TestRow withUpper(Integer newValue)
        {
            return new TestRow(lower, newValue, partition);
        }

        public TestRow withPartition(Integer newValue)
        {
            return new TestRow(lower, upper, newValue);
        }

        public String asValues()
        {
            return format("(%s, %s, %s)", lower(), upper(), partition());
        }
    }
}
