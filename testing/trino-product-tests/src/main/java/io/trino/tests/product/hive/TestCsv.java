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

import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCsv
        extends ProductTest
{
    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    public void testInsertIntoCsvTable()
    {
        testInsertIntoCsvTable("storage_formats_test_insert_into_csv", "");
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    public void testInsertIntoCsvTableWithCustomProperties()
    {
        testInsertIntoCsvTable("storage_formats_test_insert_into_csv_with_custom_properties", ", csv_escape = 'e', csv_separator='s', csv_quote='q'");
    }

    private void testInsertIntoCsvTable(String tableName, String additionalTableProperties)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s(" +
                        "  name varchar, " +
                        "  comment varchar " +
                        ") WITH (format='CSV' %s)",
                tableName, additionalTableProperties));

        onTrino().executeQuery(format("INSERT INTO %s SELECT name, comment FROM tpch.tiny.nation", tableName));

        assertSelect("SELECT max(name), max(comment) FROM %s", tableName);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testCreateCsvTableAs()
    {
        testCreateCsvTableAs("");
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testCreateCsvTableAsWithCustomProperties()
    {
        testCreateCsvTableAs(", csv_escape = 'e', csv_separator = 's', csv_quote = 'q'");
    }

    private void testCreateCsvTableAs(String additionalParameters)
    {
        String tableName = "test_csv_table";
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s WITH (format='CSV' %s) AS " +
                        "SELECT " +
                        "CAST(nationkey AS varchar) AS nationkey, CAST(name AS varchar) AS name, CAST(comment AS varchar) AS comment " +
                        "FROM tpch.tiny.nation",
                tableName,
                additionalParameters));

        assertSelect("SELECT max(name), max(comment) FROM %s", tableName);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testInsertIntoPartitionedCsvTable()
    {
        testInsertIntoPartitionedCsvTable("test_partitioned_csv_table", "");
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testInsertIntoPartitionedCsvTableWithCustomProperties()
    {
        testInsertIntoPartitionedCsvTable("test_partitioned_csv_table_with_custom_parameters", ", csv_escape = 'e', csv_separator = 's', csv_quote = 'q'");
    }

    private void testInsertIntoPartitionedCsvTable(String tableName, String additionalParameters)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s(" +
                        "  name varchar, " +
                        "  comment varchar, " +
                        "  regionkey bigint " +
                        ") WITH (format='CSV' %s, partitioned_by = ARRAY['regionkey'])",
                tableName,
                additionalParameters));

        onTrino().executeQuery(format("INSERT INTO %s SELECT name, comment, regionkey FROM tpch.tiny.nation", tableName));

        assertSelect("SELECT max(name), max(comment), max(regionkey) FROM %s", tableName);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testCreatePartitionedCsvTableAs()
    {
        testCreatePartitionedCsvTableAs("storage_formats_test_create_table_as_select_partitioned_csv", "");
    }

    @Test(groups = {STORAGE_FORMATS, HMS_ONLY})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testCreatePartitionedCsvTableAsWithCustomParamters()
    {
        testCreatePartitionedCsvTableAs(
                "storage_formats_test_create_table_as_select_partitioned_csv_with_custom_parameters",
                ", csv_escape = 'e', csv_separator='s', csv_quote='q'");
    }

    private void testCreatePartitionedCsvTableAs(String tableName, String additionalParameters)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onTrino().executeQuery(format(
                "CREATE TABLE %s WITH (format='CSV', partitioned_by = ARRAY['regionkey'] %s) AS " +
                        "SELECT cast(nationkey AS varchar) AS nationkey, cast(name AS varchar) AS name, regionkey FROM tpch.tiny.nation",
                tableName,
                additionalParameters));

        assertSelect("SELECT max(name), max(regionkey) FROM %s", tableName);

        onTrino().executeQuery("DROP TABLE " + tableName);
    }

    private static void assertSelect(String query, String tableName)
    {
        QueryResult expected = onTrino().executeQuery(format(query, "tpch.tiny.nation"));
        List<Row> expectedRows = expected.rows().stream()
                .map(columns -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = onTrino().executeQuery(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsOnly(expectedRows);
    }

    @Test(groups = STORAGE_FORMATS)
    public void testReadCsvTableWithMultiCharProperties()
    {
        String tableName = "storage_formats_test_read_csv_table_with_multi_char_properties";
        onHive().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onHive().executeQuery(format(
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

        onHive().executeQuery(format(
                "INSERT INTO %s(a, b, c) VALUES " +
                        "('1', 'a', 'A'), " +
                        "('2', 'b', 'B'), " +
                        "('3', 'c', 'C')",
                tableName));

        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .containsOnly(
                        row("1", "a", "A"),
                        row("2", "b", "B"),
                        row("3", "c", "C"));
        onHive().executeQuery(format("DROP TABLE %s", tableName));
    }

    @Test(groups = STORAGE_FORMATS)
    public void testWriteCsvTableWithMultiCharProperties()
    {
        String tableName = "storage_formats_test_write_csv_table_with_multi_char_properties";
        onHive().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        onHive().executeQuery(format(
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

        onTrino().executeQuery(format(
                "INSERT INTO %s(a, b, c) VALUES " +
                        "('1', 'a', 'A'), " +
                        "('2', 'b', 'B'), " +
                        "('3', 'c', 'C')",
                tableName));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", tableName)))
                .containsOnly(
                        row("1", "a", "A"),
                        row("2", "b", "B"),
                        row("3", "c", "C"));
        onHive().executeQuery(format("DROP TABLE %s", tableName));
    }
}
