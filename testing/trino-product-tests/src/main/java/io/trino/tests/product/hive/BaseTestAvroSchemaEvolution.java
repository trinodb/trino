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
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert.Row;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.AVRO;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

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

    private final String tableName;
    private final String columnsInTableStatement;
    private final String selectStarStatement;
    private final List<String> varcharPartitionColumns;

    protected BaseTestAvroSchemaEvolution(String tableName, String... varcharPartitionColumns)
    {
        this.tableName = tableName;
        this.columnsInTableStatement = "SHOW COLUMNS IN " + tableName;
        this.selectStarStatement = "SELECT * FROM " + tableName;
        this.varcharPartitionColumns = ImmutableList.copyOf(varcharPartitionColumns);
    }

    @BeforeTestWithContext
    public void createAndLoadTable()
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
                tableName,
                ORIGINAL_SCHEMA));
        insertData(tableName, 0, "'stringA0'", "0");
    }

    @AfterTestWithContext
    public void dropTestTable()
    {
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = AVRO)
    public void testSelectTable()
    {
        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", tableName)))
                .containsExactlyInOrder(row("stringA0"));
    }

    @Test(groups = AVRO)
    public void testInsertAfterSchemaEvolution()
    {
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsOnly(
                        createRow(0, "stringA0", 0));

        alterTableSchemaTo(ADDED_COLUMN_SCHEMA);
        insertData(tableName, 1, "'stringA1'", "1", "101");
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsOnly(
                        createRow(0, "stringA0", 0, 100),
                        createRow(1, "stringA1", 1, 101));
    }

    @Test(groups = AVRO)
    public void testSchemaEvolutionWithIncompatibleType()
    {
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaTo(INCOMPATIBLE_TYPE_SCHEMA);
        assertQueryFailure(() -> onTrino().executeQuery(selectStarStatement))
                .hasMessageContaining("Found int, expecting string");
    }

    @Test(groups = AVRO)
    public void testSchemaEvolution()
    {
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaTo(CHANGE_COLUMN_TYPE_SCHEMA);
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "bigint", "", "")));
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaTo(ADDED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", ""),
                                row("int_col_added", "integer", "", "")));
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0, 100));

        alterTableSchemaTo(REMOVED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, 0));

        alterTableSchemaTo(RENAMED_COLUMN_SCHEMA);
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col_renamed", "integer", "", "")));

        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", null));
    }

    @Test(groups = AVRO)
    public void testSchemaWhenUrlIsUnset()
    {
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(onTrino().executeQuery(selectStarStatement))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        onHive().executeQuery(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.url')", tableName));
        assertThat(onTrino().executeQuery(columnsInTableStatement))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("dummy_col", "varchar", "", "")));
    }

    @Test(groups = AVRO)
    public void testCreateTableLike()
    {
        String createTableLikeName = tableName + "_avro_like";
        onTrino().executeQuery(format(
                "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                createTableLikeName,
                tableName));

        insertData(createTableLikeName, 0, "'stringA0'", "0");

        assertThat(onTrino().executeQuery(format("SELECT string_col FROM %s", createTableLikeName)))
                .containsExactlyInOrder(row("stringA0"));
        onTrino().executeQuery("DROP TABLE IF EXISTS " + createTableLikeName);
    }

    private void alterTableSchemaTo(String schema)
    {
        onHive().executeQuery(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.url'='%s')", tableName, schema));
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
        ImmutableList.Builder<Row> rowsWithPartitionsBuilder = new ImmutableList.Builder<>();
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
}
