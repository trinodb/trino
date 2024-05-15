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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestTableFormats
        extends ProductTest
{
    protected abstract String getCatalogName();

    protected void testCreateAndSelectNationTable(String schemaLocation)
    {
        String tableName = "nation_" + randomNameSuffix();
        String tableLocation = schemaLocation + "/" + tableName;
        onTrino().executeQuery(format(
                "CREATE TABLE %1$s.default.%2$s WITH (location = '%3$s/%2$s') AS SELECT * FROM tpch.tiny.nation",
                getCatalogName(),
                tableName,
                tableLocation));

        assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %1$s.default.%2$s", getCatalogName(), tableName))).containsOnly(row(25));
        onTrino().executeQuery(format("DROP TABLE %1$s.default.%2$s", getCatalogName(), tableName));
    }

    protected void testBasicWriteOperations(String schemaLocation)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        String tableLocation = schemaLocation + "/" + tableName;

        onTrino().executeQuery(format(
                "CREATE TABLE %1$s.default.%2$s (a_bigint bigint, a_varchar varchar) WITH (location = '%3$s/%2$s')",
                getCatalogName(),
                tableName,
                tableLocation));

        onTrino().executeQuery(format("INSERT INTO %1$s.default.%2$s VALUES (1, 'hello world')".formatted(getCatalogName(), tableName)));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %1$s.default.%2$s", getCatalogName(), tableName))).containsOnly(row(1L, "hello world"));

        onTrino().executeQuery(format("UPDATE %1$s.default.%2$s SET a_varchar = 'hallo Welt' WHERE a_bigint = 1".formatted(getCatalogName(), tableName)));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %1$s.default.%2$s", getCatalogName(), tableName))).containsOnly(row(1L, "hallo Welt"));

        onTrino().executeQuery(format("DELETE FROM %1$s.default.%2$s WHERE a_bigint = 1".formatted(getCatalogName(), tableName)));
        assertThat(onTrino().executeQuery(format("SELECT * FROM %1$s.default.%2$s", getCatalogName(), tableName))).hasNoRows();
        onTrino().executeQuery(format("DROP TABLE %1$s.default.%2$s", getCatalogName(), tableName));
    }

    protected void testCreateAndInsertTable(String schemaLocation)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        onTrino().executeQuery(format("CREATE SCHEMA %s.test WITH (location = '%s')", getCatalogName(), schemaLocation));
        try {
            onTrino().executeQuery(format("CREATE TABLE %s.test.%s (a_bigint bigint, a_varchar varchar)", getCatalogName(), tableName));

            onTrino().executeQuery(format("INSERT INTO %s.test.%s VALUES (1, 'hello world')".formatted(getCatalogName(), tableName)));
            assertThat(onTrino().executeQuery("SELECT * FROM %s.test.%s".formatted(getCatalogName(), tableName))).containsOnly(row(1L, "hello world"));
        }
        finally {
            onTrino().executeQuery("DROP SCHEMA %s.test CASCADE".formatted(getCatalogName()));
        }
    }

    protected void testPathContainsSpecialCharacter(String schemaLocation, String partitioningPropertyName)
    {
        String tableName = "test_path_special_character" + randomNameSuffix();
        try {
            onTrino().executeQuery(format("CREATE SCHEMA %1$s.test WITH (location = '%2$s')", getCatalogName(), schemaLocation));
            onTrino().executeQuery(format(
                    "CREATE TABLE %1$s.test.%2$s (id bigint, part varchar) WITH (%3$s = ARRAY['part'])",
                    getCatalogName(),
                    tableName,
                    partitioningPropertyName));

            onTrino().executeQuery("INSERT INTO " + getCatalogName() + ".test." + tableName + " VALUES " +
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

            List<QueryAssert.Row> expectedRows = ImmutableList.of(
                    row(1, "with-hyphen"),
                    row(2, "with.dot"),
                    row(3, "with:colon"),
                    row(4, "with/slash"),
                    row(5, "with\\\\backslashes"),
                    row(6, "with\\backslash"),
                    row(7, "with=equal"),
                    row(8, "with?question"),
                    row(9, "with!exclamation"),
                    row(10, "with%percent"),
                    row(11, "with%%percents"),
                    row(12, "with space"));
            assertThat(onTrino().executeQuery(format("SELECT * FROM %1$s.test.%2$s", getCatalogName(), tableName))).containsOnly(expectedRows);
        }
        finally {
            onTrino().executeQuery("DROP TABLE %1$s.test.%2$s".formatted(getCatalogName(), tableName));
            onTrino().executeQuery("DROP SCHEMA %1$s.test".formatted(getCatalogName()));
        }
    }

    protected void testSparkCompatibilityOnTrinoCreatedTable(String schemaLocation)
    {
        String baseTableName = "trino_created_table_using_parquet_" + randomNameSuffix();
        String sparkTableName = format("%s.test_compat.%s", getSparkCatalog(), baseTableName);
        String trinoTableName = format("%s.test_compat.%s", getCatalogName(), baseTableName);
        try {
            onTrino().executeQuery(format("CREATE SCHEMA %s.test_compat WITH (location = '%s')", getCatalogName(), schemaLocation));

            onTrino().executeQuery("CREATE TABLE " + trinoTableName + "(a_boolean boolean, a_varchar varchar) WITH (format = 'PARQUET')");
            onTrino().executeQuery("INSERT INTO " + trinoTableName + " VALUES (true, 'test data')");

            List<QueryAssert.Row> expected = List.of(row(true, "test data"));
            assertThat(onTrino().executeQuery("SELECT * FROM " + trinoTableName)).containsOnly(expected);
            assertThat(onSpark().executeQuery("SELECT * FROM " + sparkTableName)).containsOnly(expected);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
            onTrino().executeQuery("DROP SCHEMA IF EXISTS %s.test_compat".formatted(getCatalogName()));
        }
    }

    protected String getSparkCatalog()
    {
        return "spark_catalog";
    }
}
