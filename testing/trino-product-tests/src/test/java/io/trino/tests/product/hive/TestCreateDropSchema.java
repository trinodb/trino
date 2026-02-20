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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CREATE and DROP SCHEMA operations in the Hive connector.
 * <p>
 * Ported from the Tempto-based TestCreateDropSchema.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestCreateDropSchema
{
    @Test
    void testCreateDropSchema(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "test_drop_schema_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db", warehouseDirectory, schemaName);

        env.executeTrinoUpdate("CREATE SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");

        env.executeTrinoUpdate(format("CREATE TABLE %s.test_drop (col1 int)", schemaName));
        assertThatThrownBy(() -> env.executeTrinoUpdate("DROP SCHEMA " + schemaName))
                .hasMessageContaining("Cannot drop non-empty schema '%s'", schemaName);

        env.executeTrinoUpdate(format("DROP TABLE %s.test_drop", schemaName));
        env.executeTrinoUpdate("DROP SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    void testDropSchemaFiles(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_without_location_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        env.executeTrinoUpdate(format("CREATE SCHEMA %s", schemaName));
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    void testDropSchemaFilesWithLocation(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_empty_location_" + randomNameSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-empty-location/";

        env.executeTrinoUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test // specified location, external file in subdir
    void testDropWithExternalFilesInSubdirectory(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_nonempty_location_" + randomNameSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-nonempty-location/";
        // Use subdirectory to make sure file check is recursive
        String subDir = schemaDir + "subdir/";
        String externalFile = subDir + "external-file";

        // Create file below schema directory before creating schema
        hdfsClient.createDirectory(subDir);
        hdfsClient.saveFile(externalFile, "");

        env.executeTrinoUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after dropping schema");

        hdfsClient.delete(schemaDir);
    }

    @Test // default location, empty external subdir
    void testDropSchemaFilesWithEmptyExternalSubdir(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_empty_subdirectory_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);
        String externalSubdir = schemaDir + "external-subdir/";

        hdfsClient.createDirectory(externalSubdir);

        env.executeTrinoUpdate("CREATE SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, externalSubdir, true, "external subdirectory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA " + schemaName);
        assertFileExistence(hdfsClient, externalSubdir, true, "external subdirectory exists after dropping schema");

        hdfsClient.delete(schemaDir);
    }

    @Test // default location, transactions without external files
    void testDropSchemaFilesTransactions(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_directory_transactions_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        env.executeTrinoUpdate(format("CREATE SCHEMA %s", schemaName));
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");

        // Test transaction rollback
        env.executeTrinoInSession(session -> {
            session.executeUpdate("START TRANSACTION");
            session.executeUpdate("DROP SCHEMA " + schemaName);
            session.executeUpdate("ROLLBACK");
        });
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after rollback");

        // Sanity check: schema is still working
        env.executeTrinoUpdate(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        env.executeTrinoUpdate(format("DROP TABLE %s.test_table", schemaName));

        // Test transaction commit
        env.executeTrinoInSession(session -> {
            session.executeUpdate("START TRANSACTION");
            session.executeUpdate("DROP SCHEMA " + schemaName);
            session.executeUpdate("COMMIT");
        });
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test // specified location, transaction with top-level external file
    void testDropTransactionsWithExternalFiles(HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_transactions_with_external_files_" + randomNameSuffix();
        String schemaDir = warehouseDirectory + "/schema-transactions-with-external-files/";

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        env.executeTrinoUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));

        // Test transaction rollback
        env.executeTrinoInSession(session -> {
            session.executeUpdate("START TRANSACTION");
            session.executeUpdate("DROP SCHEMA " + schemaName);
            session.executeUpdate("ROLLBACK");
        });
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after rolling back drop schema");

        // Sanity check: schema is still working
        env.executeTrinoUpdate(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        env.executeTrinoUpdate(format("DROP TABLE %s.test_table", schemaName));

        // Test transaction commit
        env.executeTrinoInSession(session -> {
            session.executeUpdate("START TRANSACTION");
            session.executeUpdate("DROP SCHEMA " + schemaName);
            session.executeUpdate("COMMIT");
        });
        assertFileExistence(hdfsClient, externalFile, true, "schema directory exists after committing drop schema");
    }

    private void assertFileExistence(HdfsClient hdfsClient, String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path)).as("%s (%s)", description, path).isEqualTo(exists);
    }
}
