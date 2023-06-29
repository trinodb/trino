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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.AVRO;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestAvroSchemaEvolution
        extends ProductTest
{
    // TODO move Avro schema files to classpath and use tempto SshClient to upload them
    private static final String ORIGINAL_SCHEMA = "file:///docker/presto-product-tests/avro/original_schema.avsc";
    private static final String RENAMED_COLUMN_SCHEMA = "file:///docker/presto-product-tests/avro/rename_column_schema.avsc";
    private static final String REMOVED_COLUMN_SCHEMA = "file:///docker/presto-product-tests/avro/remove_column_schema.avsc";
    private static final String ADDED_COLUMN_SCHEMA = "file:///docker/presto-product-tests/avro/add_column_schema.avsc";
    private static final String CHANGE_COLUMN_TYPE_SCHEMA = "file:///docker/presto-product-tests/avro/change_column_type_schema.avsc";
    private static final String INCOMPATIBLE_TYPE_SCHEMA = "file:///docker/presto-product-tests/avro/incompatible_type_schema.avsc";

    private final String tableWithSchemaUrl;
    private final String tableWithSchemaLiteral;
    private final String columnsInTableStatement;
    private final String selectStarStatement;
    private final List<String> varcharPartitionColumns;

    protected BaseTestAvroSchemaEvolution(String tableName, String... varcharPartitionColumns)
    {
        this.tableWithSchemaLiteral = tableName + "_with_schema_literal";
        this.tableWithSchemaUrl = tableName + "_with_schema_url";
        this.columnsInTableStatement = "SHOW COLUMNS IN %s";
        this.selectStarStatement = "SELECT * FROM %s";
        this.varcharPartitionColumns = ImmutableList.copyOf(varcharPartitionColumns);
    }

    @BeforeMethodWithContext
    public void createAndLoadTable()
            throws IOException
    {
        onTrino().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  dummy_col VARCHAR" +
                        (varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(partitionColumns -> partitionColumns + " varchar")) +
                        ")" +
                        "WITH (" +
                        "  format='AVRO', " +
                        "  avro_schema_url='%s'" +
                        (varcharPartitionColumns.isEmpty() ? "" : ", partitioned_by=ARRAY[" + getPartitionsAsListString(partitionColumns -> "'" + partitionColumns + "'") + "]") +
                        ")",
                tableWithSchemaUrl,
                ORIGINAL_SCHEMA));
        insertData(tableWithSchemaUrl, 0, "'stringA0'", "0");
        onTrino().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  dummy_col VARCHAR" +
                        (varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(partitionColumns -> partitionColumns + " varchar")) +
                        ")" +
                        "WITH (" +
                        "  format='AVRO', " +
                        "  avro_schema_literal='%s'" +
                        (varcharPartitionColumns.isEmpty() ? "" : ", partitioned_by=ARRAY[" + getPartitionsAsListString(partitionColumns -> "'" + partitionColumns + "'") + "]") +
                        ")",
                tableWithSchemaLiteral,
                readSchemaLiteralFromUrl(ORIGINAL_SCHEMA)));
        insertData(tableWithSchemaLiteral, 0, "'stringA0'", "0");
    }

    @AfterMethodWithContext
    public void dropTestTable()
    {
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableWithSchemaUrl));
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableWithSchemaLiteral));
    }

    @Test(groups = AVRO)
    public void testSelectTable()
    {
        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", tableWithSchemaUrl)))
                .containsExactlyInOrder(row("stringA0"));
        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", tableWithSchemaLiteral)))
                .containsExactlyInOrder(row("stringA0"));
    }

    @Test(groups = AVRO)
    public void testInsertAfterSchemaEvolution()
            throws IOException
    {
        assertUnmodified();

        alterTableSchemaUrl(ADDED_COLUMN_SCHEMA);
        alterTableSchemaLiteral(readSchemaLiteralFromUrl(ADDED_COLUMN_SCHEMA));

        insertData(tableWithSchemaUrl, 1, "'stringA1'", "1", "101");
        insertData(tableWithSchemaLiteral, 1, "'stringA1'", "1", "101");

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsOnly(
                        createRow(0, "stringA0", 0, 100),
                        createRow(1, "stringA1", 1, 101));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsOnly(
                        createRow(0, "stringA0", 0, 100),
                        createRow(1, "stringA1", 1, 101));
    }

    @Test(groups = AVRO)
    public void testSchemaEvolutionWithIncompatibleType()
            throws IOException
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertUnmodified();

        alterTableSchemaUrl(INCOMPATIBLE_TYPE_SCHEMA);
        alterTableSchemaLiteral(readSchemaLiteralFromUrl(INCOMPATIBLE_TYPE_SCHEMA));

        assertQueryFailure(() -> onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .hasStackTraceContaining("Found int, expecting string");
        assertQueryFailure(() -> onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .hasStackTraceContaining("Found int, expecting string");
    }

    @Test(groups = AVRO)
    public void testSchemaEvolutionWithUrl()
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaUrl(CHANGE_COLUMN_TYPE_SCHEMA);
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "bigint", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaUrl(ADDED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", ""),
                                row("int_col_added", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0, 100));

        alterTableSchemaUrl(REMOVED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, 0));

        alterTableSchemaUrl(RENAMED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col_renamed", "integer", "", "")));

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", null));
    }

    @Test(groups = AVRO)
    public void testSchemaEvolutionWithLiteral()
            throws IOException
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaLiteral(readSchemaLiteralFromUrl(CHANGE_COLUMN_TYPE_SCHEMA));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "bigint", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaLiteral(readSchemaLiteralFromUrl(ADDED_COLUMN_SCHEMA));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", ""),
                                row("int_col_added", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0, 100));

        alterTableSchemaLiteral(readSchemaLiteralFromUrl(REMOVED_COLUMN_SCHEMA));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, 0));

        alterTableSchemaLiteral(readSchemaLiteralFromUrl(RENAMED_COLUMN_SCHEMA));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col_renamed", "integer", "", "")));

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", null));
    }

    @Test(groups = AVRO)
    public void testSchemaWhenUrlIsUnset()
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        onHive().executeQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.url')", tableWithSchemaUrl));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("dummy_col", "varchar", "", "")));
    }

    @Test(groups = AVRO)
    public void testSchemaWhenLiteralIsUnset()
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        onHive().executeQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.literal')", tableWithSchemaLiteral));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("dummy_col", "varchar", "", "")));
    }

    @Test(groups = AVRO)
    public void testCreateTableLike()
    {
        String createTableLikeWithSchemaUrl = tableWithSchemaUrl + "_avro_like";
        String createTableLikeWithSchemaLiteral = tableWithSchemaLiteral + "_avro_like";

        onTrino().executeQuery(format(
                "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                createTableLikeWithSchemaUrl,
                tableWithSchemaUrl));

        onTrino().executeQuery(format(
                "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                createTableLikeWithSchemaLiteral,
                tableWithSchemaLiteral));

        insertData(createTableLikeWithSchemaUrl, 0, "'stringA0'", "0");
        insertData(createTableLikeWithSchemaLiteral, 0, "'stringA0'", "0");

        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", createTableLikeWithSchemaUrl)))
                .containsExactlyInOrder(row("stringA0"));
        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", createTableLikeWithSchemaLiteral)))
                .containsExactlyInOrder(row("stringA0"));

        onTrino().executeQuery("DROP TABLE IF EXISTS " + createTableLikeWithSchemaUrl);
        onTrino().executeQuery("DROP TABLE IF EXISTS " + createTableLikeWithSchemaLiteral);
    }

    private void assertUnmodified()
    {
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(format(columnsInTableStatement, tableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaUrl)))
                .containsOnly(createRow(0, "stringA0", 0));
        assertThat(onTrino().executeQuery(format(selectStarStatement, tableWithSchemaLiteral)))
                .containsOnly(createRow(0, "stringA0", 0));
    }

    private void alterTableSchemaUrl(String newUrl)
    {
        onHive().executeQuery(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.url'='%s')", tableWithSchemaUrl, newUrl));
    }

    private void alterTableSchemaLiteral(String newLiteral)
    {
        onHive().executeQuery(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.literal'='%s')", tableWithSchemaLiteral, newLiteral));
    }

    private void insertData(String tableName, int rowNumber, String... sqlValues)
    {
        String columnValues = Joiner.on(", ").join(sqlValues) +
                (varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(partitionColumn -> "'" + partitionColumn + "_" + rowNumber + "'"));
        onTrino().executeQuery(format("INSERT INTO %s VALUES (%s)", tableName, columnValues));
    }

    private Row createRow(int rowNumber, Object... data)
    {
        List<Object> rowData = new ArrayList<>(Arrays.asList(data));
        varcharPartitionColumns.forEach(partition -> rowData.add(partition + "_" + rowNumber));
        return new Row(rowData);
    }

    private List<Row> prepareShowColumnsResultRows(Row... rows)
    {
        ImmutableList.Builder<Row> rowsWithPartitionsBuilder = ImmutableList.builder();
        rowsWithPartitionsBuilder.add(rows);
        varcharPartitionColumns.stream()
                .map(partition -> row(partition, "varchar", "partition key", ""))
                .forEach(rowsWithPartitionsBuilder::add);
        return rowsWithPartitionsBuilder.build();
    }

    private String getPartitionsAsListString(Function<String, String> partitionMapper)
    {
        return varcharPartitionColumns.stream()
                .map(partitionMapper)
                .collect(Collectors.joining(", "));
    }

    private static String readSchemaLiteralFromUrl(String url)
            throws IOException
    {
        return Files.readString(Path.of(URI.create(url)));
    }
}
