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
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Base class for Avro schema evolution tests.
 */
@ProductTest
abstract class BaseTestAvroSchemaEvolutionJunit
{
    private static final String WAREHOUSE_PATH = "/user/hive/warehouse/TestAvroSchemaEvolution";
    private static final String SCHEMAS_PATH = WAREHOUSE_PATH + "/schemas";

    // Avro schema content embedded as strings
    private static final String ORIGINAL_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"int" }
              ]
            }""";

    private static final String ADDED_COLUMN_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"int" },
                { "name":"int_col_added", "type":"int", "default": 100 }
              ]
            }""";

    private static final String CHANGE_COLUMN_TYPE_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"long"}
              ]
            }""";

    private static final String REMOVED_COLUMN_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"int_col", "type":"int" }
              ]
            }""";

    private static final String RENAMED_COLUMN_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col_renamed", "type":["null", "int"], "default": null }
              ]
            }""";

    private static final String INCOMPATIBLE_TYPE_SCHEMA = """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"string"}
              ]
            }""";

    // Fully qualified table name for Trino (hive.default.tablename)
    private final String trinoTableWithSchemaUrl;
    private final String trinoTableWithSchemaLiteral;
    // Short table name for Hive (default database is implicit)
    private final String hiveTableWithSchemaUrl;
    private final String hiveTableWithSchemaLiteral;
    private final List<String> varcharPartitionColumns;

    protected BaseTestAvroSchemaEvolutionJunit(String tableName, String... varcharPartitionColumns)
    {
        this.hiveTableWithSchemaLiteral = tableName + "_with_schema_literal";
        this.hiveTableWithSchemaUrl = tableName + "_with_schema_url";
        this.trinoTableWithSchemaLiteral = "hive.default." + hiveTableWithSchemaLiteral;
        this.trinoTableWithSchemaUrl = "hive.default." + hiveTableWithSchemaUrl;
        this.varcharPartitionColumns = ImmutableList.copyOf(varcharPartitionColumns);
    }

    @BeforeEach
    void setup(HiveStorageFormatsEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();

        // Upload schema files to HDFS
        hdfsClient.createDirectory(SCHEMAS_PATH);
        hdfsClient.saveFile(SCHEMAS_PATH + "/original_schema.avsc", ORIGINAL_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/add_column_schema.avsc", ADDED_COLUMN_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/change_column_type_schema.avsc", CHANGE_COLUMN_TYPE_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/remove_column_schema.avsc", REMOVED_COLUMN_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/rename_column_schema.avsc", RENAMED_COLUMN_SCHEMA);
        hdfsClient.saveFile(SCHEMAS_PATH + "/incompatible_type_schema.avsc", INCOMPATIBLE_TYPE_SCHEMA);

        // Create table with schema URL
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (" +
                        "  dummy_col VARCHAR" +
                        "%s" +
                        ")" +
                        "WITH (" +
                        "  format='AVRO', " +
                        "  avro_schema_url='%s'" +
                        "%s" +
                        ")",
                trinoTableWithSchemaUrl,
                varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(col -> col + " varchar"),
                SCHEMAS_PATH + "/original_schema.avsc",
                varcharPartitionColumns.isEmpty() ? "" : ", partitioned_by=ARRAY[" + getPartitionsAsListString(col -> "'" + col + "'") + "]"));
        insertData(env, trinoTableWithSchemaUrl, 0, "'stringA0'", "0");

        // Create table with schema literal
        env.executeTrinoUpdate(format(
                "CREATE TABLE %s (" +
                        "  dummy_col VARCHAR" +
                        "%s" +
                        ")" +
                        "WITH (" +
                        "  format='AVRO', " +
                        "  avro_schema_literal='%s'" +
                        "%s" +
                        ")",
                trinoTableWithSchemaLiteral,
                varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(col -> col + " varchar"),
                escapeSchemaLiteral(ORIGINAL_SCHEMA),
                varcharPartitionColumns.isEmpty() ? "" : ", partitioned_by=ARRAY[" + getPartitionsAsListString(col -> "'" + col + "'") + "]"));
        insertData(env, trinoTableWithSchemaLiteral, 0, "'stringA0'", "0");
    }

    @AfterEach
    void cleanup(HiveStorageFormatsEnvironment env)
    {
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", trinoTableWithSchemaUrl));
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", trinoTableWithSchemaLiteral));
        try {
            env.createHdfsClient().delete(WAREHOUSE_PATH);
        }
        catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectTable(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SELECT string_col FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(row("stringA0"));
        assertThat(env.executeTrino(format("SELECT string_col FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(row("stringA0"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testInsertAfterSchemaEvolution(HiveStorageFormatsEnvironment env)
    {
        assertUnmodified(env);

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/add_column_schema.avsc");
        alterTableSchemaLiteral(env, escapeSchemaLiteral(ADDED_COLUMN_SCHEMA));

        insertData(env, trinoTableWithSchemaUrl, 1, "'stringA1'", "1", "101");
        insertData(env, trinoTableWithSchemaLiteral, 1, "'stringA1'", "1", "101");

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsOnly(
                        createRow(0, "stringA0", 0, 100),
                        createRow(1, "stringA1", 1, 101));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsOnly(
                        createRow(0, "stringA0", 0, 100),
                        createRow(1, "stringA1", 1, 101));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSchemaEvolutionWithIncompatibleType(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertUnmodified(env);

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/incompatible_type_schema.avsc");
        alterTableSchemaLiteral(env, escapeSchemaLiteral(INCOMPATIBLE_TYPE_SCHEMA));

        assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .hasStackTraceContaining("Found int, expecting string");
        assertThatThrownBy(() -> env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .hasStackTraceContaining("Found int, expecting string");
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSchemaEvolutionWithUrl(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/change_column_type_schema.avsc");
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "bigint", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0L));

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/add_column_schema.avsc");
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", ""),
                                row("int_col_added", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0, 100));

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/remove_column_schema.avsc");
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, 0));

        alterTableSchemaUrl(env, SCHEMAS_PATH + "/rename_column_schema.avsc");
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col_renamed", "integer", "", "")));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", null));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSchemaEvolutionWithLiteral(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        alterTableSchemaLiteral(env, escapeSchemaLiteral(CHANGE_COLUMN_TYPE_SCHEMA));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "bigint", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0L));

        alterTableSchemaLiteral(env, escapeSchemaLiteral(ADDED_COLUMN_SCHEMA));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", ""),
                                row("int_col_added", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0, 100));

        alterTableSchemaLiteral(env, escapeSchemaLiteral(REMOVED_COLUMN_SCHEMA));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, 0));

        alterTableSchemaLiteral(env, escapeSchemaLiteral(RENAMED_COLUMN_SCHEMA));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col_renamed", "integer", "", "")));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", null));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSchemaWhenUrlIsUnset(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        env.executeHiveUpdate(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.url')", hiveTableWithSchemaUrl));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("dummy_col", "varchar", "", "")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSchemaWhenLiteralIsUnset(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        createRow(0, "stringA0", 0));

        env.executeHiveUpdate(format("ALTER TABLE %s UNSET TBLPROPERTIES('avro.schema.literal')", hiveTableWithSchemaLiteral));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("dummy_col", "varchar", "", "")));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testCreateTableLike(HiveStorageFormatsEnvironment env)
    {
        String trinoCreateTableLikeWithSchemaUrl = trinoTableWithSchemaUrl + "_avro_like";
        String trinoCreateTableLikeWithSchemaLiteral = trinoTableWithSchemaLiteral + "_avro_like";

        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                    trinoCreateTableLikeWithSchemaUrl,
                    trinoTableWithSchemaUrl));

            env.executeTrinoUpdate(format(
                    "CREATE TABLE %s (LIKE %s INCLUDING PROPERTIES)",
                    trinoCreateTableLikeWithSchemaLiteral,
                    trinoTableWithSchemaLiteral));

            insertData(env, trinoCreateTableLikeWithSchemaUrl, 0, "'stringA0'", "0");
            insertData(env, trinoCreateTableLikeWithSchemaLiteral, 0, "'stringA0'", "0");

            assertThat(env.executeTrino(format("SELECT string_col FROM %s", trinoCreateTableLikeWithSchemaUrl)))
                    .containsExactlyInOrder(row("stringA0"));
            assertThat(env.executeTrino(format("SELECT string_col FROM %s", trinoCreateTableLikeWithSchemaLiteral)))
                    .containsExactlyInOrder(row("stringA0"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoCreateTableLikeWithSchemaUrl);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoCreateTableLikeWithSchemaLiteral);
        }
    }

    private void assertUnmodified(HiveStorageFormatsEnvironment env)
    {
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaUrl)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));
        assertThat(env.executeTrino(format("SHOW COLUMNS IN %s", trinoTableWithSchemaLiteral)))
                .containsExactlyInOrder(
                        prepareShowColumnsResultRows(
                                row("string_col", "varchar", "", ""),
                                row("int_col", "integer", "", "")));

        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaUrl)))
                .containsOnly(createRow(0, "stringA0", 0));
        assertThat(env.executeTrino(format("SELECT * FROM %s", trinoTableWithSchemaLiteral)))
                .containsOnly(createRow(0, "stringA0", 0));
    }

    private void alterTableSchemaUrl(HiveStorageFormatsEnvironment env, String newUrl)
    {
        env.executeHiveUpdate(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.url'='%s')", hiveTableWithSchemaUrl, newUrl));
    }

    private void alterTableSchemaLiteral(HiveStorageFormatsEnvironment env, String newLiteral)
    {
        env.executeHiveUpdate(format("ALTER TABLE %s SET TBLPROPERTIES('avro.schema.literal'='%s')", hiveTableWithSchemaLiteral, newLiteral));
    }

    private void insertData(HiveStorageFormatsEnvironment env, String tableName, int rowNumber, String... sqlValues)
    {
        String columnValues = String.join(", ", sqlValues) +
                (varcharPartitionColumns.isEmpty() ? "" : ", " + getPartitionsAsListString(partitionColumn -> "'" + partitionColumn + "_" + rowNumber + "'"));
        env.executeTrinoUpdate(format("INSERT INTO %s VALUES (%s)", tableName, columnValues));
    }

    private Row createRow(int rowNumber, Object... data)
    {
        List<Object> rowData = new ArrayList<>(Arrays.asList(data));
        varcharPartitionColumns.forEach(partition -> rowData.add(partition + "_" + rowNumber));
        return Row.fromList(rowData);
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

    private static String escapeSchemaLiteral(String schema)
    {
        // Escape single quotes for SQL and remove newlines for compact representation
        return schema.replace("'", "''").replace("\n", "").replace("  ", " ");
    }
}
