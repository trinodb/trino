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

import io.trino.tempto.ProductTest;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
}
