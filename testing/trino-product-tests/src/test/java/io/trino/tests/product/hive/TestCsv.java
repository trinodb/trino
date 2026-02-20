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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;

/**
 * Tests for CSV table format support in Hive connector.
 * <p>
 * Ported from the Tempto-based TestCsv.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
@TestGroup.HmsOnly
class TestCsv
{
    @Test
    void testInsertIntoCsvTable(HiveStorageFormatsEnvironment env)
    {
        testInsertIntoCsvTable(env, "storage_formats_test_insert_into_csv", "");
    }

    @Test
    void testInsertIntoCsvTableWithCustomProperties(HiveStorageFormatsEnvironment env)
    {
        testInsertIntoCsvTable(env, "storage_formats_test_insert_into_csv_with_custom_properties", ", csv_escape = 'e', csv_separator='s', csv_quote='q'");
    }

    private void testInsertIntoCsvTable(HiveStorageFormatsEnvironment env, String tableName, String additionalTableProperties)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s(" +
                        "  name varchar, " +
                        "  comment varchar " +
                        ") WITH (format='CSV' %s)",
                tableName, additionalTableProperties));

        env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT name, comment FROM tpch.tiny.nation", tableName));

        assertSelect(env, "SELECT max(name), max(comment) FROM %s", tableName);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreateCsvTableAs(HiveStorageFormatsEnvironment env)
    {
        testCreateCsvTableAs(env, "");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreateCsvTableAsWithCustomProperties(HiveStorageFormatsEnvironment env)
    {
        testCreateCsvTableAs(env, ", csv_escape = 'e', csv_separator = 's', csv_quote = 'q'");
    }

    private void testCreateCsvTableAs(HiveStorageFormatsEnvironment env, String additionalParameters)
    {
        String tableName = "test_csv_table";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s WITH (format='CSV' %s) AS " +
                        "SELECT " +
                        "CAST(nationkey AS varchar) AS nationkey, CAST(name AS varchar) AS name, CAST(comment AS varchar) AS comment " +
                        "FROM tpch.tiny.nation",
                tableName,
                additionalParameters));

        assertSelect(env, "SELECT max(name), max(comment) FROM %s", tableName);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertIntoPartitionedCsvTable(HiveStorageFormatsEnvironment env)
    {
        testInsertIntoPartitionedCsvTable(env, "test_partitioned_csv_table", "");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertIntoPartitionedCsvTableWithCustomProperties(HiveStorageFormatsEnvironment env)
    {
        testInsertIntoPartitionedCsvTable(env, "test_partitioned_csv_table_with_custom_parameters", ", csv_escape = 'e', csv_separator = 's', csv_quote = 'q'");
    }

    private void testInsertIntoPartitionedCsvTable(HiveStorageFormatsEnvironment env, String tableName, String additionalParameters)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s(" +
                        "  name varchar, " +
                        "  comment varchar, " +
                        "  regionkey bigint " +
                        ") WITH (format='CSV' %s, partitioned_by = ARRAY['regionkey'])",
                tableName,
                additionalParameters));

        env.executeTrinoUpdate(format("INSERT INTO hive.default.%s SELECT name, comment, regionkey FROM tpch.tiny.nation", tableName));

        assertSelectPartitioned(env, "SELECT max(name), max(comment), max(regionkey) FROM %s", tableName);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreatePartitionedCsvTableAs(HiveStorageFormatsEnvironment env)
    {
        testCreatePartitionedCsvTableAs(env, "storage_formats_test_create_table_as_select_partitioned_csv", "");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreatePartitionedCsvTableAsWithCustomParamters(HiveStorageFormatsEnvironment env)
    {
        testCreatePartitionedCsvTableAs(
                env,
                "storage_formats_test_create_table_as_select_partitioned_csv_with_custom_parameters",
                ", csv_escape = 'e', csv_separator='s', csv_quote='q'");
    }

    private void testCreatePartitionedCsvTableAs(HiveStorageFormatsEnvironment env, String tableName, String additionalParameters)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s WITH (format='CSV', partitioned_by = ARRAY['regionkey'] %s) AS " +
                        "SELECT cast(nationkey AS varchar) AS nationkey, cast(name AS varchar) AS name, regionkey FROM tpch.tiny.nation",
                tableName,
                additionalParameters));

        assertSelectPartitionedNameRegion(env, "SELECT max(name), max(regionkey) FROM %s", tableName);

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    private static void assertSelect(HiveStorageFormatsEnvironment env, String query, String tableName)
    {
        QueryResult expected = env.executeTrino(format(query, "tpch.tiny.nation"));
        List<Row> expectedRows = expected.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = env.executeTrino(format(query, "hive.default." + tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsOnly(expectedRows);
    }

    private static void assertSelectPartitioned(HiveStorageFormatsEnvironment env, String query, String tableName)
    {
        QueryResult expected = env.executeTrino(format(query, "tpch.tiny.nation"));
        List<Row> expectedRows = expected.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = env.executeTrino(format(query, "hive.default." + tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsOnly(expectedRows);
    }

    private static void assertSelectPartitionedNameRegion(HiveStorageFormatsEnvironment env, String query, String tableName)
    {
        QueryResult expected = env.executeTrino(format(query, "tpch.tiny.nation"));
        List<Row> expectedRows = expected.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = env.executeTrino(format(query, "hive.default." + tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsOnly(expectedRows);
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testReadCsvTableWithMultiCharProperties(HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_read_csv_table_with_multi_char_properties";
        env.executeHiveUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        env.executeHiveUpdate(format(
                "CREATE TABLE %s(" +
                        "   a  string," +
                        "   b  string," +
                        "   c  string" +
                        ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                        "WITH SERDEPROPERTIES ('escapeChar'='ee','separatorChar'='ss','quoteChar'='qq') " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
                tableName));

        env.executeHiveUpdate(format(
                "INSERT INTO %s(a, b, c) VALUES " +
                        "('1', 'a', 'A'), " +
                        "('2', 'b', 'B'), " +
                        "('3', 'c', 'C')",
                tableName));

        assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName)))
                .containsOnly(
                        row("1", "a", "A"),
                        row("2", "b", "B"),
                        row("3", "c", "C"));
        env.executeHiveUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testWriteCsvTableWithMultiCharProperties(HiveStorageFormatsEnvironment env)
    {
        String tableName = "storage_formats_test_write_csv_table_with_multi_char_properties";
        env.executeHiveUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        env.executeHiveUpdate(format(
                "CREATE TABLE %s(" +
                        "   a  string," +
                        "   b  string," +
                        "   c  string" +
                        ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                        "WITH SERDEPROPERTIES ('escapeChar'='ee','separatorChar'='ss','quoteChar'='qq') " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
                tableName));

        env.executeTrinoUpdate(format(
                "INSERT INTO hive.default.%s(a, b, c) VALUES " +
                        "('1', 'a', 'A'), " +
                        "('2', 'b', 'B'), " +
                        "('3', 'c', 'C')",
                tableName));
        assertThat(env.executeTrino(format("SELECT * FROM hive.default.%s", tableName)))
                .containsOnly(
                        row("1", "a", "A"),
                        row("2", "b", "B"),
                        row("3", "c", "C"));
        env.executeHiveUpdate(format("DROP TABLE %s", tableName));
    }
}
