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

import com.google.inject.name.Named;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutionException;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestCreateDropSchema
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @BeforeTestWithContext
    public void useIceberg()
    {
        onTrino().executeQuery("USE iceberg.default");
    }

    @Test(groups = ICEBERG)
    public void testDropSchemaFiles()
    {
        String schemaName = "schema_without_location_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        onTrino().executeQuery(format("CREATE SCHEMA %s", schemaName));
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test(groups = ICEBERG)
    public void testDropSchemaFilesWithLocation()
    {
        String schemaName = "schema_with_empty_location_" + randomTableSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-empty-location/";

        onTrino().executeQuery(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test(groups = ICEBERG) // specified location, external file in subdir
    public void testDropWithExternalFilesInSubdirectory()
    {
        String schemaName = "schema_with_nonempty_location_" + randomTableSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-nonempty-location/";
        // Use subdirectory to make sure file check is recursive
        String subDir = schemaDir + "subdir/";
        String externalFile = subDir + "external-file";

        // Create file below schema directory before creating schema
        hdfsClient.createDirectory(subDir);
        hdfsClient.saveFile(externalFile, "");

        onTrino().executeQuery(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertFileExistence(externalFile, true, "external file exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(externalFile, true, "external file exists after dropping schema");

        hdfsClient.delete(schemaDir);
    }

    @Test(groups = ICEBERG) // make sure empty directories are noticed as well
    public void testDropSchemaFilesWithEmptyExternalSubdir()
    {
        String schemaName = "schema_with_empty_subdirectory_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);
        String externalSubdir = schemaDir + "external-subdir/";

        hdfsClient.createDirectory(externalSubdir);

        onTrino().executeQuery("CREATE SCHEMA " + schemaName);
        assertFileExistence(externalSubdir, true, "external subdirectory exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(externalSubdir, true, "external subdirectory exists after dropping schema");

        hdfsClient.delete(schemaDir);
    }

    @Test(groups = ICEBERG) // default location, external file at top level
    public void testDropWithExternalFiles()
    {
        String schemaName = "schema_with_files_in_default_location_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        onTrino().executeQuery("CREATE SCHEMA " + schemaName);
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, true, "schema directory exists after dropping schema");
        assertFileExistence(externalFile, true, "external file exists after dropping schema");

        hdfsClient.delete(externalFile);
        hdfsClient.delete(schemaDir);
    }

    private void assertFileExistence(String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path)).as(description).isEqualTo(exists);
    }

    private static void assertQuerySucceeds(String query)
    {
        try {
            onTrino().executeQuery(query);
        }
        catch (QueryExecutionException e) {
            fail(format("Expected query to succeed: %s", query), e.getCause());
        }
    }
}
