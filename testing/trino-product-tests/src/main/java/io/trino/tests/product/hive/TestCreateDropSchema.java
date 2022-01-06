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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCreateDropSchema
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @javax.inject.Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Test
    public void testCreateDropSchema()
    {
        String schemaName = "test_drop_schema_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db", warehouseDirectory, schemaName);

        onTrino().executeQuery("CREATE SCHEMA " + schemaName);
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");

        onTrino().executeQuery(format("CREATE TABLE %s.test_drop (col1 int)", schemaName));
        assertQueryFailure(() -> onTrino().executeQuery("DROP SCHEMA " + schemaName))
                .hasMessageContaining("line 1:1: Cannot drop non-empty schema '%s'", schemaName);

        onTrino().executeQuery(format("DROP TABLE %s.test_drop", schemaName));
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    public void testDropSchemaWithLocationWithoutExternalFiles()
    {
        String schemaName = "schema_with_empty_location_" + randomTableSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-empty-location/";

        onTrino().executeQuery(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    public void testDropSchemaFilesWithoutLocation()
    {
        String schemaName = "schema_without_location_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        onTrino().executeQuery(format("CREATE SCHEMA %s", schemaName));
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    public void testDropSchemaFilesWithLocationWithExternalFile()
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

    @Test // make sure empty directories are noticed as well
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

    // Tests create/drop schema transactions with default schema location
    @Test
    public void testDropSchemaFilesTransactions()
    {
        String schemaName = "schema_directory_transactions_" + randomTableSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        onTrino().executeQuery(format("CREATE SCHEMA %s", schemaName));
        assertFileExistence(schemaDir, true, "schema directory exists after creating schema");

        onTrino().executeQuery("START TRANSACTION");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        onTrino().executeQuery("ROLLBACK");
        assertFileExistence(schemaDir, true, "schema directory exists after rollback");

        // Sanity check: schema is still working
        onTrino().executeQuery(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        onTrino().executeQuery(format("DROP TABLE %s.test_table", schemaName));

        onTrino().executeQuery("START TRANSACTION");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        onTrino().executeQuery("COMMIT");
        assertFileExistence(schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    public void testDropSchemaFilesTransactionsWithExternalFile()
    {
        String schemaName = "schema_transactions_with_external_files_" + randomTableSuffix();
        String schemaDir = warehouseDirectory + "/schema-transactions-with-external-files/";

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        onTrino().executeQuery(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));

        onTrino().executeQuery("START TRANSACTION");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        onTrino().executeQuery("ROLLBACK");
        assertFileExistence(externalFile, true, "external file exists after rolling back drop schema");

        // Sanity check: schema is still working
        onTrino().executeQuery(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        onTrino().executeQuery(format("DROP TABLE %s.test_table", schemaName));

        onTrino().executeQuery("START TRANSACTION");
        onTrino().executeQuery("DROP SCHEMA " + schemaName);
        onTrino().executeQuery("COMMIT");
        assertFileExistence(externalFile, true, "schema directory exists after committing drop schema");
    }

    private void assertFileExistence(String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path)).as("%s (%s)", description, path).isEqualTo(exists);
    }
}
