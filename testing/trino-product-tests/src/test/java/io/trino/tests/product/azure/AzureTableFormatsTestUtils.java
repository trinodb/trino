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
package io.trino.tests.product.azure;

import io.trino.testing.containers.environment.Row;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

public final class AzureTableFormatsTestUtils
{
    private AzureTableFormatsTestUtils() {}

    public static void testCreateAndSelectNationTable(AzureEnvironment env, String catalog)
    {
        String tableName = "nation_" + randomNameSuffix();
        String tableLocation = env.getSchemaLocation() + "/" + tableName;
        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.default.%2$s WITH (location = '%3$s/%2$s') AS SELECT * FROM tpch.tiny.nation",
                    catalog,
                    tableName,
                    tableLocation));
            assertThat(env.executeTrino(format("SELECT count(*) FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(25L));
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.default.%2$s", catalog, tableName));
        }
    }

    public static void testBasicWriteOperations(AzureEnvironment env, String catalog)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        String tableLocation = env.getSchemaLocation() + "/" + tableName;

        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.default.%2$s (a_bigint bigint, a_varchar varchar) WITH (location = '%3$s/%2$s')",
                    catalog,
                    tableName,
                    tableLocation));

            env.executeTrinoUpdate(format("INSERT INTO %1$s.default.%2$s VALUES (1, 'hello world')", catalog, tableName));
            assertThat(env.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(1L, "hello world"));

            env.executeTrinoUpdate(format("UPDATE %1$s.default.%2$s SET a_varchar = 'hallo Welt' WHERE a_bigint = 1", catalog, tableName));
            assertThat(env.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).containsOnly(row(1L, "hallo Welt"));

            env.executeTrinoUpdate(format("DELETE FROM %1$s.default.%2$s WHERE a_bigint = 1", catalog, tableName));
            assertThat(env.executeTrino(format("SELECT * FROM %1$s.default.%2$s", catalog, tableName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.default.%2$s", catalog, tableName));
        }
    }

    public static void testCreateAndInsertTable(AzureEnvironment env, String catalog)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        env.executeTrinoUpdate(format("CREATE SCHEMA %s.test WITH (location = '%s')", catalog, env.getSchemaLocation()));
        try {
            env.executeTrinoUpdate(format("CREATE TABLE %s.test.%s (a_bigint bigint, a_varchar varchar)", catalog, tableName));
            env.executeTrinoUpdate(format("INSERT INTO %s.test.%s VALUES (1, 'hello world')", catalog, tableName));
            assertThat(env.executeTrino(format("SELECT * FROM %s.test.%s", catalog, tableName))).containsOnly(row(1L, "hello world"));
        }
        finally {
            env.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %s.test CASCADE", catalog));
        }
    }

    public static void testPathContainsSpecialCharacter(AzureEnvironment env, String catalog, String partitioningPropertyName, boolean verifyWithSpark, String sparkCatalog)
    {
        String tableName = "test_path_special_character_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(format("CREATE SCHEMA %1$s.test WITH (location = '%2$s')", catalog, env.getSchemaLocation()));
            env.executeTrinoUpdate(format(
                    "CREATE TABLE %1$s.test.%2$s (id bigint, part varchar) WITH (%3$s = ARRAY['part'])",
                    catalog,
                    tableName,
                    partitioningPropertyName));

            env.executeTrinoUpdate("INSERT INTO " + catalog + ".test." + tableName + " VALUES " +
                    "(1, 'with-hyphen')," +
                    "(2, 'with.dot')," +
                    "(3, 'with:colon')," +
                    "(4, 'with/slash')," +
                    "(5, 'with\\\\backslashes')," +
                    "(6, 'with\\backslash')," +
                    "(7, 'with=equal')," +
                    "(8, 'with?question')," +
                    "(9, 'with!exclamation')," +
                    "(10, 'with%percent')," +
                    "(11, 'with%%percents')," +
                    "(12, 'with space')");

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

            assertThat(env.executeTrino(format("SELECT * FROM %1$s.test.%2$s", catalog, tableName)))
                    .containsOnly(expectedRows.toArray(Row[]::new));
            if (verifyWithSpark) {
                assertThat(env.executeSpark(format("SELECT * FROM %1$s.test.%2$s", sparkCatalog, tableName)))
                        .containsOnly(expectedRows.toArray(Row[]::new));
            }
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %1$s.test.%2$s", catalog, tableName));
            env.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %1$s.test", catalog));
        }
    }

    public static void testSparkCompatibilityOnTrinoCreatedTable(AzureEnvironment env, String catalog, String sparkCatalog)
    {
        String baseTableName = "trino_created_table_using_parquet_" + randomNameSuffix();
        String sparkTableName = format("%s.test_compat.%s", sparkCatalog, baseTableName);
        String trinoTableName = format("%s.test_compat.%s", catalog, baseTableName);
        try {
            env.executeTrinoUpdate(format("CREATE SCHEMA %s.test_compat WITH (location = '%s')", catalog, env.getSchemaLocation()));
            env.executeTrinoUpdate("CREATE TABLE " + trinoTableName + " (a_boolean boolean, a_varchar varchar) WITH (format = 'PARQUET')");
            env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (true, 'test data')");

            List<Row> expected = List.of(row(true, "test data"));
            assertThat(env.executeTrino("SELECT * FROM " + trinoTableName)).containsOnly(expected.toArray(Row[]::new));
            assertThat(env.executeSpark("SELECT * FROM " + sparkTableName)).containsOnly(expected.toArray(Row[]::new));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
            env.executeTrinoUpdate(format("DROP SCHEMA IF EXISTS %s.test_compat", catalog));
        }
    }
}
