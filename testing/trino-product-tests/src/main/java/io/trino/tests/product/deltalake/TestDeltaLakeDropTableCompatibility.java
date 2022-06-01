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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeTestWithContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDropTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    private static final Engine TRINO_ENGINE = new TrinoEngine();
    private static final Engine DATABRICKS_ENGINE = new DatabricksEngine();

    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeTestWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @DataProvider
    public static Object[][] engineConfigurations()
    {
        return new Object[][] {
                {TRINO_ENGINE, TRINO_ENGINE, true},
                {TRINO_ENGINE, TRINO_ENGINE, false},
                {TRINO_ENGINE, DATABRICKS_ENGINE, true},
                {TRINO_ENGINE, DATABRICKS_ENGINE, false},
                {DATABRICKS_ENGINE, TRINO_ENGINE, true},
                {DATABRICKS_ENGINE, TRINO_ENGINE, false},
                {DATABRICKS_ENGINE, DATABRICKS_ENGINE, true},
                {DATABRICKS_ENGINE, DATABRICKS_ENGINE, false},
        };
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "engineConfigurations")
    public void testDatabricksManagedTableDroppedFromTrino(Engine creator, Engine dropper, boolean explicitLocation)
    {
        testCleanupOnDrop(creator, dropper, explicitLocation);
    }

    private void testCleanupOnDrop(Engine creator, Engine dropper, boolean explicitLocation)
    {
        String schemaName = "schema_with_location_" + randomTableSuffix();
        String tableName = explicitLocation ? "external_table" : "managed_table";
        creator.createSchema(schemaName, format("s3://%s/databricks-compatibility-test-%s", bucketName, schemaName));
        try {
            onTrino().executeQuery("USE delta." + schemaName);
            String tableLocation = explicitLocation ?
                    format("s3://" + bucketName + "/databricks-compatibility-test-%s/%s", schemaName, tableName) :
                    "";
            creator.createTable(schemaName, tableName, tableLocation);

            ObjectListing tableFiles = s3.listObjects(bucketName, "databricks-compatibility-test-" + schemaName + "/" + tableName);
            assertThat(tableFiles.getObjectSummaries()).isNotEmpty();

            dropper.dropTable(schemaName, tableName);
            tableFiles = s3.listObjects(bucketName, "databricks-compatibility-test-" + schemaName + "/" + tableName);
            if (explicitLocation) {
                assertThat(tableFiles.getObjectSummaries()).isNotEmpty();
            }
            else {
                assertThat(tableFiles.getObjectSummaries()).isEmpty();
            }
        }
        finally {
            onDelta().executeQuery("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            onDelta().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    private interface Engine
    {
        void createSchema(String schemaName, String location);

        void createTable(String schemaName, String tableName, String location);

        void dropTable(String schemaName, String tableName);
    }

    private static class TrinoEngine
            implements Engine
    {
        @Override
        public void createSchema(String schemaName, String location)
        {
            onTrino().executeQuery(format("CREATE SCHEMA delta.%s WITH (location = '%s')", schemaName, location));
        }

        @Override
        public void createTable(String schemaName, String tableName, String location)
        {
            String locationStatement = location.isEmpty() ? "" : "WITH (location = '" + location + "')";
            onTrino().executeQuery(format("CREATE TABLE %s.%s (a, b) %s AS VALUES (1, 2), (2, 3), (3, 4)", schemaName, tableName, locationStatement));
        }

        @Override
        public void dropTable(String schemaName, String tableName)
        {
            onTrino().executeQuery("DROP TABLE " + schemaName + "." + tableName);
        }
    }

    private static class DatabricksEngine
            implements Engine
    {
        @Override
        public void createSchema(String schemaName, String location)
        {
            onDelta().executeQuery(format("CREATE SCHEMA %s LOCATION \"%s\"", schemaName, location));
        }

        @Override
        public void createTable(String schemaName, String tableName, String location)
        {
            String locationStatement = location.isEmpty() ? "" : "LOCATION \"" + location + "\"";
            onDelta().executeQuery(format("CREATE TABLE %s.%s USING DELTA %s AS VALUES (1, 2), (2, 3), (3, 4)", schemaName, tableName, locationStatement));
        }

        @Override
        public void dropTable(String schemaName, String tableName)
        {
            onDelta().executeQuery("DROP TABLE " + schemaName + "." + tableName);
        }
    }
}
