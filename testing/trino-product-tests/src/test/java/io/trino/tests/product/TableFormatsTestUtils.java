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
package io.trino.tests.product;

import io.trino.testing.containers.environment.Row;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

public final class TableFormatsTestUtils
{
    private TableFormatsTestUtils() {}

    public static void verifyCreateAndSelectNationTable(TableFormatsTestEnvironment environment, String catalog)
    {
        String tableName = "nation_" + randomNameSuffix();
        String tableLocation = environment.getWarehouseDirectory() + "/" + tableName;
        try {
            environment.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.default.%2$s WITH (location = '%3$s/%2$s') AS SELECT * FROM tpch.tiny.nation",
                    catalog,
                    tableName,
                    tableLocation));
            assertThat(environment.executeTrino(format("SELECT count(*) FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(25L));
        }
        finally {
            environment.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.default.%2$s", catalog, tableName));
        }
    }

    public static void verifyBasicWriteOperations(TableFormatsTestEnvironment environment, String catalog)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        String tableLocation = environment.getWarehouseDirectory() + "/" + tableName;

        try {
            environment.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.default.%2$s (a_bigint bigint, a_varchar varchar) WITH (location = '%3$s/%2$s')",
                    catalog,
                    tableName,
                    tableLocation));

            environment.executeTrinoUpdate(format("INSERT INTO %1$s.default.%2$s VALUES (1, 'hello world')", catalog, tableName));
            assertThat(environment.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(1L, "hello world"));

            environment.executeTrinoUpdate(format("UPDATE %1$s.default.%2$s SET a_varchar = 'hallo Welt' WHERE a_bigint = 1", catalog, tableName));
            assertThat(environment.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(1L, "hallo Welt"));

            environment.executeTrinoUpdate(format("DELETE FROM %1$s.default.%2$s WHERE a_bigint = 1", catalog, tableName));
            assertThat(environment.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).hasNoRows();
        }
        finally {
            environment.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.default.%2$s", catalog, tableName));
        }
    }

    public static void verifyCreateAndInsertTable(TableFormatsTestEnvironment environment, String catalog)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        environment.executeTrinoUpdate(format("CREATE SCHEMA %s.test WITH (location = '%s')", catalog, environment.getWarehouseDirectory()));
        try {
            environment.executeTrinoUpdate(format("CREATE TABLE %s.test.%s (a_bigint bigint, a_varchar varchar)", catalog, tableName));
            environment.executeTrinoUpdate(format("INSERT INTO %s.test.%s VALUES (1, 'hello world')", catalog, tableName));
            assertThat(environment.executeTrino(format("SELECT * FROM %s.test.%s", catalog, tableName))).containsOnly(row(1L, "hello world"));
        }
        finally {
            environment.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %s.test CASCADE", catalog));
        }
    }

    public static void verifyPathContainsSpecialCharacter(
            TableFormatsTestEnvironment environment,
            String catalog,
            String partitioningPropertyName,
            String sparkCatalog)
    {
        String tableName = "test_path_special_character_" + randomNameSuffix();
        try {
            environment.executeTrinoUpdate(format("CREATE SCHEMA %1$s.test WITH (location = '%2$s')", catalog, environment.getWarehouseDirectory()));
            environment.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.test.%2$s (id bigint, part varchar) WITH (%3$s = ARRAY['part'])",
                    catalog,
                    tableName,
                    partitioningPropertyName));

            environment.executeTrinoUpdate(
                    """
                    INSERT INTO %s.test.%s VALUES
                    (1, 'with-hyphen'),
                    (2, 'with.dot'),
                    (3, 'with:colon'),
                    (4, 'with/slash'),
                    (5, 'with\\\\backslashes'),
                    (6, 'with\\backslash'),
                    (7, 'with=equal'),
                    (8, 'with?question'),
                    (9, 'with!exclamation'),
                    (10, 'with%%percent'),
                    (11, 'with%%%%percents'),
                    (12, 'with space')
                    """.formatted(catalog, tableName));

            List<Row> expectedRows = List.of(
                    row(1L, "with-hyphen"),
                    row(2L, "with.dot"),
                    row(3L, "with:colon"),
                    row(4L, "with/slash"),
                    row(5L, "with\\\\backslashes"),
                    row(6L, "with\\backslash"),
                    row(7L, "with=equal"),
                    row(8L, "with?question"),
                    row(9L, "with!exclamation"),
                    row(10L, "with%percent"),
                    row(11L, "with%%percents"),
                    row(12L, "with space"));

            assertThat(environment.executeTrino(format("SELECT * FROM %1$s.test.%2$s", catalog, tableName)))
                    .containsOnly(expectedRows.toArray(Row[]::new));
            if (sparkCatalog != null) {
                assertThat(environment.executeSpark(format("SELECT * FROM %1$s.test.%2$s", sparkCatalog, tableName)))
                        .containsOnly(expectedRows.toArray(Row[]::new));
            }
        }
        finally {
            environment.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.test.%2$s", catalog, tableName));
            environment.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %1$s.test", catalog));
        }
    }

    public static void verifySparkCompatibilityOnTrinoCreatedTable(TableFormatsTestEnvironment environment, String catalog, String sparkCatalog)
    {
        String baseTableName = "trino_created_table_using_parquet_" + randomNameSuffix();
        String sparkTableName = format("%s.test_compat.%s", sparkCatalog, baseTableName);
        String trinoTableName = format("%s.test_compat.%s", catalog, baseTableName);
        try {
            environment.executeTrinoUpdate(format("CREATE SCHEMA %s.test_compat WITH (location = '%s')", catalog, environment.getWarehouseDirectory()));

            environment.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " (a_boolean boolean, a_varchar varchar) WITH (format = 'PARQUET')");
            environment.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (true, 'test data')");

            List<Row> expected = List.of(row(true, "test data"));
            assertThat(environment.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected.toArray(Row[]::new));
            assertThat(environment.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected.toArray(Row[]::new));
        }
        finally {
            environment.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
            environment.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %s.test_compat", catalog));
        }
    }
}
