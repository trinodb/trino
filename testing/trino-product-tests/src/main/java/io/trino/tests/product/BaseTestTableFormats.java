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
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestTableFormats
        extends ProductTest
{
    protected abstract String getCatalogName();

    protected void testCreateAndSelectNationTable(String schemaLocation)
    {
        String tableName = "nation_" + randomNameSuffix();
        String tableLocation = schemaLocation + "/" + tableName;
        onTrino().executeQuery("CREATE TABLE %1$s.default.%2$s WITH (location = '%3$s/%2$s') AS SELECT * FROM tpch.tiny.nation".formatted(
                getCatalogName(),
                tableName,
                tableLocation));

        assertThat(onTrino().executeQuery("SELECT count(*) FROM %1$s.default.%2$s".formatted(getCatalogName(), tableName))).containsOnly(row(25));
        onTrino().executeQuery("DROP TABLE %1$s.default.%2$s".formatted(getCatalogName(), tableName));
    }

    protected void testBasicWriteOperations(String schemaLocation)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        String tableLocation = schemaLocation + "/" + tableName;

        onTrino().executeQuery("CREATE TABLE %1$s.default.%2$s (a_bigint bigint, a_varchar varchar) WITH (location = '%3$s/%2$s')".formatted(
                getCatalogName(),
                tableName,
                tableLocation));

        onTrino().executeQuery("INSERT INTO %1$s.default.%2$s VALUES (1, 'hello world')".formatted(getCatalogName(), tableName).formatted());
        assertThat(onTrino().executeQuery("SELECT * FROM %1$s.default.%2$s".formatted(getCatalogName(), tableName))).containsOnly(row(1L, "hello world"));

        onTrino().executeQuery("UPDATE %1$s.default.%2$s SET a_varchar = 'hallo Welt' WHERE a_bigint = 1".formatted(getCatalogName(), tableName).formatted());
        assertThat(onTrino().executeQuery("SELECT * FROM %1$s.default.%2$s".formatted(getCatalogName(), tableName))).containsOnly(row(1L, "hallo Welt"));

        onTrino().executeQuery("DELETE FROM %1$s.default.%2$s WHERE a_bigint = 1".formatted(getCatalogName(), tableName).formatted());
        assertThat(onTrino().executeQuery("SELECT * FROM %1$s.default.%2$s".formatted(getCatalogName(), tableName))).hasNoRows();
        onTrino().executeQuery("DROP TABLE %1$s.default.%2$s".formatted(getCatalogName(), tableName));
    }

    protected void testCreateAndInsertTable(String schemaLocation)
    {
        String tableName = "table_write_operations_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA %s.test WITH (location = '%s')".formatted(getCatalogName(), schemaLocation));
        try {
            onTrino().executeQuery("CREATE TABLE %s.test.%s (a_bigint bigint, a_varchar varchar)".formatted(getCatalogName(), tableName));

            onTrino().executeQuery("INSERT INTO %s.test.%s VALUES (1, 'hello world')".formatted(getCatalogName(), tableName).formatted());
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
            onTrino().executeQuery("CREATE SCHEMA %1$s.test WITH (location = '%2$s')".formatted(getCatalogName(), schemaLocation));
            onTrino().executeQuery("CREATE TABLE %1$s.test.%2$s (id bigint, part varchar) WITH (%3$s = ARRAY['part'])".formatted(
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
            assertThat(onTrino().executeQuery("SELECT * FROM %1$s.test.%2$s".formatted(getCatalogName(), tableName))).containsOnly(expectedRows);
            if (!getCatalogName().equalsIgnoreCase("delta")) { // must be skipped for delta since the env is not integrated with spark3-delta container
                assertThat(onSpark().executeQuery("SELECT * FROM %1$s.test.%2$s".formatted(getSparkCatalog(), tableName))).containsOnly(expectedRows);
            }
        }
        finally {
            onTrino().executeQuery("DROP TABLE %1$s.test.%2$s".formatted(getCatalogName(), tableName));
            onTrino().executeQuery("DROP SCHEMA %1$s.test".formatted(getCatalogName()));
        }
    }

    protected void testLocationContainsDiscouragedCharacter(String schemaLocation)
    {
        // According to https://docs.cloud.google.com/storage/docs/objects#recommendations some chars ([*]#?) are discouraged
        // because they are specially treated in gcloud cli. But they are not directly prohibited.
        // Chars used in schema location are not escaped like those in partition names
        // so it allows to test whether whole stack works e2e with discouraged characters.
        String schemaName = getCatalogName() + ".test";
        String tableName = "%s.test_location_contains_discouraged_character_%s".formatted(schemaName, randomNameSuffix());
        try {
            String locationWithDiscouragedChars = schemaLocation + "/[*]#?";
            onTrino().executeQuery("CREATE SCHEMA " + schemaName + " WITH (location = '" + locationWithDiscouragedChars + "')");
            onTrino().executeQuery("CREATE TABLE " + tableName + " (id bigint, someValue varchar)");

            onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'someValue')");

            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(row(1, "someValue"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
            onTrino().executeQuery("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    protected void testSparkCompatibilityOnTrinoCreatedTable(String schemaLocation)
    {
        String baseTableName = "trino_created_table_using_parquet_" + randomNameSuffix();
        String sparkTableName = "%s.test_compat.%s".formatted(getSparkCatalog(), baseTableName);
        String trinoTableName = "%s.test_compat.%s".formatted(getCatalogName(), baseTableName);
        try {
            onTrino().executeQuery("CREATE SCHEMA %s.test_compat WITH (location = '%s')".formatted(getCatalogName(), schemaLocation));

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
