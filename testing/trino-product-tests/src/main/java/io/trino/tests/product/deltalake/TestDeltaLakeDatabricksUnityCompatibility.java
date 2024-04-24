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
package io.trino.tests.product.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DATABRICKS_UNITY_HTTP_HMS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDatabricksUnityCompatibility
        extends ProductTest
{
    private final String schemaName = "test_delta_basic_" + randomNameSuffix();
    private String unityCatalogName;
    private String externalLocationPath;

    @BeforeMethodWithContext
    public void setUp()
    {
        unityCatalogName = requireNonNull(System.getenv("DATABRICKS_UNITY_CATALOG_NAME"), "Environment variable not set: DATABRICKS_UNITY_CATALOG_NAME");
        externalLocationPath = requireNonNull(System.getenv("DATABRICKS_UNITY_EXTERNAL_LOCATION"), "Environment variable not set: DATABRICKS_UNITY_EXTERNAL_LOCATION");
        onDelta().executeQuery(format("CREATE SCHEMA %s.%s", unityCatalogName, schemaName));
    }

    @AfterMethodWithContext
    public void cleanUp()
    {
        onDelta().executeQuery(format("DROP SCHEMA IF EXISTS %s.%s CASCADE", unityCatalogName, schemaName));
    }

    @Test(groups = {DATABRICKS_UNITY_HTTP_HMS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testBasicTableReadWrite()
    {
        String tableName = "test_read_write_" + randomNameSuffix();
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + " (c1 int, c2 string) USING delta LOCATION '" + tableLocation + "'");
        onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM delta"))
                .contains(row(schemaName.toLowerCase(ENGLISH)));

        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName.toLowerCase(ENGLISH)));

        // select
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(row(1, "one"));

        // insert
        List<QueryAssert.Row> expectedRowsForInsert = ImmutableList.of(row(1, "one"), row(2, "two"));
        onTrino().executeQuery("INSERT INTO " + deltaTableName + " VALUES (2, 'two')");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForInsert);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForInsert);

        // update
        List<QueryAssert.Row> expectedRowsForUpdate = ImmutableList.of(row(1, "one"), row(2, "two hundred"));
        onTrino().executeQuery("UPDATE " + deltaTableName + " SET c2 = 'two hundred' WHERE c1 = 2");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForUpdate);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForUpdate);

        // delete
        List<QueryAssert.Row> expectedRowsForDelete = ImmutableList.of(row(2, "two hundred"));
        onTrino().executeQuery("DELETE FROM " + deltaTableName + " WHERE c2 = 'one'");
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForDelete);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForDelete);

        // merge
        List<QueryAssert.Row> expectedRowsForMerge = ImmutableList.of(row(1, "one"), row(2, "two"), row(3, "three"));
        String sourceTableName = "test_source_" + randomNameSuffix();
        String tableLocation2 = format("%s/%s/%s", externalLocationPath, schemaName, sourceTableName);
        onDelta().executeQuery(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) using delta location '%s'", unityCatalogName, schemaName, sourceTableName, tableLocation2));
        onDelta().executeQuery(format("INSERT INTO %s.%s.%s values (1, 'one'), (2, 'two'), (3, 'three')", unityCatalogName, schemaName, sourceTableName));

        onTrino().executeQuery(format("MERGE INTO delta.%s.%s t USING delta.%s.%s s on t.c1 = s.c1 " +
                "WHEN MATCHED THEN UPDATE SET c2 = s.c2 " +
                "WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES (s.c1, s.c2)", schemaName, tableName, schemaName, sourceTableName));
        assertThat(onTrino().executeQuery("SELECT * FROM " + deltaTableName))
                .containsOnly(expectedRowsForMerge);
        assertThat(onDelta().executeQuery("SELECT * FROM " + unityTableName))
                .containsOnly(expectedRowsForMerge);
    }

    /**
     * The Unity Catalog's HMS API has a limitation where managed tables are not supported. This test
     * verifies that if a managed table is created using Databricks delta lake, Trino which connects
     * to Unity Catalog's HMS endpoint doesn't see it.
     */
    @Test(groups = {DATABRICKS_UNITY_HTTP_HMS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testManagedTable()
    {
        String tableName = "test_managed_table_" + randomNameSuffix();
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + " (c1 int, c2 string)");
        assertThat(onTrino().executeQuery("SHOW CREATE SCHEMA delta." + schemaName))
                .containsOnly(row("CREATE SCHEMA delta." + schemaName));
        assertThat(onTrino().executeQuery("SHOW TABLES IN delta." + schemaName)).hasNoRows();
    }

    @Test(groups = {DATABRICKS_UNITY_HTTP_HMS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnTypes()
    {
        String tableName = "test_column_types" + randomNameSuffix();
        String deltaTableName = "delta.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        onDelta().executeQuery(format("CREATE TABLE %s (" +
                "int_col INT," +
                "string_col STRING," +
                "tinyint_col TINYINT," +
                "smallint_col SMALLINT," +
                "bigint_col BIGINT," +
                "decimal_col DECIMAL," +
                "float_col FLOAT," +
                "double_col DOUBLE," +
                "date_col DATE," +
                "timestamp_col TIMESTAMP," +
                "binary_col BINARY," +
                "bool_col BOOLEAN," +
                "array_int_col ARRAY<int>," +
                "map_col MAP<TIMESTAMP,INT>," +
                "struct_col struct<a: LONG, b: String NOT NULL>" +
                ") " +
                "USING DELTA " +
                "LOCATION '%s'", unityTableName, tableLocation));
        assertThat(
                onTrino().executeQuery("SHOW TABLES IN delta." + schemaName))
                .containsOnly(row(tableName));
        assertThat(
                onTrino().executeQuery("SHOW COLUMNS IN " + deltaTableName))
                .containsOnly(
                    row("int_col", "integer", "", ""),
                    row("string_col", "varchar", "", ""),
                    row("tinyint_col", "tinyint", "", ""),
                    row("smallint_col", "smallint", "", ""),
                    row("bigint_col", "bigint", "", ""),
                    row("decimal_col", "decimal(10,0)", "", ""),
                    row("float_col", "real", "", ""),
                    row("double_col", "double", "", ""),
                    row("date_col", "date", "", ""),
                    row("timestamp_col", "timestamp(3) with time zone", "", ""),
                    row("binary_col", "varbinary", "", ""),
                    row("bool_col", "boolean", "", ""),
                    row("array_int_col", "array(integer)", "", ""),
                    row("map_col", "map(timestamp(3) with time zone, integer)", "", ""),
                    row("struct_col", "row(a bigint, b varchar)", "", ""));
    }

    @Test(groups = {DATABRICKS_UNITY_HTTP_HMS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testUnsupportedStatements()
    {
        String tableName = "test_unsupported_" + randomNameSuffix();
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);
        String trinoLocation = format("%s/%s", externalLocationPath, "trino_test_" + randomNameSuffix());

        onDelta().executeQuery(format("CREATE TABLE %s.%s.%s (c1 int, c2 string) USING delta LOCATION '%s'", unityCatalogName, schemaName, tableName, tableLocation));

        assertQueryFailure(() -> onTrino().executeQuery(format("CREATE TABLE delta.%s.new_table (c1 int) WITH (location = '%s')", schemaName, trinoLocation)))
                .hasMessageContaining("DDL not enabled in Hive metastore interface.");
        assertQueryFailure(() -> onTrino().executeQuery(format("CREATE TABLE delta.%s.new_table (c1) WITH (location = '%s') AS SELECT 1", schemaName, trinoLocation)))
                .hasRootCauseMessage("DDL not enabled in Hive metastore interface.");
        assertQueryFailure(() -> onTrino().executeQuery(format("ALTER TABLE delta.%s.%s RENAME TO delta.%s.new_t1", schemaName, tableName, schemaName)))
                .hasMessageContaining("DDL not enabled in Hive metastore interface.");
        assertQueryFailure(() -> onTrino().executeQuery(format("DROP TABLE delta.%s.%s", schemaName, tableName)))
                .hasMessageContaining("DDL not enabled in Hive metastore interface.");
        assertQueryFailure(() -> onTrino().executeQuery(format("DROP SCHEMA delta.%s CASCADE", schemaName)))
                .hasMessageContaining("DDL not enabled in Hive metastore interface.");
    }
}
