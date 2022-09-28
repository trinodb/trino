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
package io.trino.tests.product.iceberg;

import com.google.inject.Inject;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestIcebergCreateTable
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setUp()
    {
        // Use IF NOT EXISTS because the schema can be left behind after previous test, as the tests are @Flaky
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.iceberg");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onTrino().executeQuery("DROP SCHEMA iceberg.iceberg");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testCreateTable()
    {
        String tableName = "iceberg.iceberg.test_create_table_" + randomTableSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a bigint, b varchar)");
        try {
            onTrino().executeQuery("INSERT INTO " + tableName + "(a, b) VALUES " +
                    "(NULL, NULL), " +
                    "(-42, 'abc'), " +
                    "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(
                            row(null, null),
                            row(-42, "abc"),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testCreateTableAsSelect()
    {
        String tableName = "iceberg.iceberg.test_create_table_as_select_" + randomTableSuffix();
        onTrino().executeQuery("" +
                "CREATE TABLE " + tableName + " AS " +
                "SELECT * FROM (VALUES " +
                "  (NULL, NULL), " +
                "  (-42, 'abc'), " +
                "  (9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')" +
                ") t(a, b)");
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(
                            row(null, null),
                            row(-42, "abc"),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testCreateExternalTableWithInaccessibleSchemaLocation()
    {
        String schemaName = "schema_without_location";
        String schemaLocation = "/tmp/" + schemaName;
        hdfsClient.createDirectory(schemaLocation);

        onTrino().executeQuery(format("CREATE SCHEMA iceberg.%s WITH (location = '%s')", schemaName, schemaLocation));

        hdfsClient.delete(schemaLocation);

        String tableName = "test_create_external";
        String tableLocation = "/tmp/" + tableName;

        String schemaAndTableName = format("iceberg.%s.%s", schemaName, tableName);
        onTrino().executeQuery(format("CREATE TABLE %s (a bigint, b VARCHAR) WITH (location = '%s')", schemaAndTableName, tableLocation));

        onTrino().executeQuery("INSERT INTO " + schemaAndTableName + "(a, b) VALUES " +
                "(NULL, NULL), " +
                "(-42, 'abc'), " +
                "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + schemaAndTableName))
                .containsOnly(
                        row(null, null),
                        row(-42, "abc"),
                        row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));

        onTrino().executeQuery(format("DROP TABLE %s", schemaAndTableName));
        onTrino().executeQuery(format("DROP SCHEMA iceberg.%s", schemaName));
    }
}
