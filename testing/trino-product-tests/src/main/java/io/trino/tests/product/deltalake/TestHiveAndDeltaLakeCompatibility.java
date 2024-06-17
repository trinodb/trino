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

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tests.product.hudi.TestHudiHiveViewsCompatibility;
import io.trino.tests.product.iceberg.TestIcebergHiveViewsCompatibility;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @see TestIcebergHiveViewsCompatibility
 * @see TestHudiHiveViewsCompatibility
 */
public class TestHiveAndDeltaLakeCompatibility
        extends ProductTest
{
    private String bucketName;

    @BeforeMethodWithContext
    public void setUp()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumnsOnPresenceOfHiveView()
    {
        // use dedicated schema so we control the number and shape of tables
        String schemaName = "test_redirect_to_delta_information_schema_columns_schema_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName + " WITH (location = 's3://" + bucketName + "/test_redirect_to_delta')");

        String hiveViewName = "delta_schema_columns_hive_view_" + randomNameSuffix();
        String hiveViewQualifiedName = format("hive.%s.%s", schemaName, hiveViewName);

        onTrino().executeQuery("CREATE VIEW " + hiveViewQualifiedName + " AS SELECT 1 AS col_one");

        try {
            assertThat(onTrino().executeQuery(format("SELECT table_name FROM delta.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                    .containsOnly(row(hiveViewName));
        }
        finally {
            onTrino().executeQuery("DROP VIEW IF EXISTS " + hiveViewQualifiedName);
            onTrino().executeQuery("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testUnregisterNotDeltaLakeTable()
    {
        String schemaName = "test_unregister_not_delta_lake_schema_" + randomNameSuffix();

        String baseTableName = "test_unregister_not_delta_table_" + randomNameSuffix();
        String hiveTableName = format("hive.%s.%s", schemaName, baseTableName);

        try {
            onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName + " WITH (location = 's3://" + bucketName + "/test_unregister_to_delta')");

            onTrino().executeQuery("CREATE TABLE " + hiveTableName + " AS SELECT 1 a");

            assertThatThrownBy(() -> onTrino().executeQuery(format("CALL delta.system.unregister_table('%s', '%s')", schemaName, baseTableName)))
                    .hasMessageContaining("not a Delta Lake table");
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
            onTrino().executeQuery("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
