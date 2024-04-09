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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DATABRICKS_UNITY_HTTP_HMS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveDatabricksUnityCompatibility
        extends ProductTest
{
    private String unityCatalogName;
    private String externalLocationPath;
    private final String schemaName = "test_basic_hive_" + randomNameSuffix();

    @BeforeMethodWithContext
    public void setUp()
    {
        unityCatalogName = requireNonNull(System.getenv("DATABRICKS_UNITY_CATALOG_NAME"), "Environment variable not set: DATABRICKS_UNITY_CATALOG_NAME");
        externalLocationPath = requireNonNull(System.getenv("DATABRICKS_UNITY_EXTERNAL_LOCATION"), "Environment variable not set: DATABRICKS_UNITY_EXTERNAL_LOCATION");
        String schemaLocation = format("%s/%s", externalLocationPath, schemaName);
        onDelta().executeQuery("CREATE SCHEMA " + unityCatalogName + "." + schemaName + " MANAGED LOCATION '" + schemaLocation + "'");
    }

    @AfterMethodWithContext
    public void cleanUp()
    {
        onDelta().executeQuery("DROP SCHEMA IF EXISTS " + unityCatalogName + "." + schemaName + " CASCADE");
    }

    @Test(groups = {DATABRICKS_UNITY_HTTP_HMS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testBasicHiveOperations()
    {
        String tableName = "test_table_" + randomNameSuffix();
        String hiveTableName = "hive.%s.%s".formatted(schemaName, tableName);
        String unityTableName = "%s.%s.%s".formatted(unityCatalogName, schemaName, tableName);
        String tableLocation = format("%s/%s/%s", externalLocationPath, schemaName, tableName);

        onDelta().executeQuery("CREATE TABLE " + unityTableName + "(c1 int, c2 string) USING PARQUET LOCATION '" + tableLocation + "'");
        onDelta().executeQuery("INSERT INTO " + unityTableName + " VALUES (1, 'one')");

        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM hive"))
                .contains(row(schemaName));

        assertThat(onTrino().executeQuery("SHOW TABLES IN hive." + schemaName))
                .containsOnly(row(tableName));

        assertThat(onTrino().executeQuery("SELECT * FROM " + hiveTableName))
                .containsOnly(row(1, "one"));
    }
}
