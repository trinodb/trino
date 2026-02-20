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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for schema creation and deletion with HDFS directory management.
 * <p>
 * Ported from the Tempto-based TestCreateDropSchema.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestCreateDropSchema
{
    @Test
    void testDropSchemaFiles(SparkIcebergEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_without_location_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        env.executeTrinoUpdate(format("CREATE SCHEMA iceberg.%s", schemaName));
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA iceberg." + schemaName);
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    void testDropSchemaFilesWithLocation(SparkIcebergEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_empty_location_" + randomNameSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-empty-location-" + randomNameSuffix() + "/";

        env.executeTrinoUpdate(format("CREATE SCHEMA iceberg.%s WITH (location = 'hdfs://hadoop-master:9000%s')", schemaName, schemaDir));
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA iceberg." + schemaName);
        assertFileExistence(hdfsClient, schemaDir, false, "schema directory exists after dropping schema");
    }

    @Test
    void testDropWithExternalFilesInSubdirectory(SparkIcebergEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_nonempty_location_" + randomNameSuffix();
        String schemaDir = warehouseDirectory + "/schema-with-nonempty-location-" + randomNameSuffix() + "/";
        // Use subdirectory to make sure file check is recursive
        String subDir = schemaDir + "subdir/";
        String externalFile = subDir + "external-file";

        // Create file below schema directory before creating schema
        hdfsClient.createDirectory(subDir);
        hdfsClient.saveFile(externalFile, "");

        env.executeTrinoUpdate(format("CREATE SCHEMA iceberg.%s WITH (location = 'hdfs://hadoop-master:9000%s')", schemaName, schemaDir));
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA iceberg." + schemaName);
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after dropping schema");

        hdfsClient.delete(schemaDir);
    }

    @Test
    void testDropWithExternalFiles(SparkIcebergEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        String warehouseDirectory = env.getWarehouseDirectory();

        String schemaName = "schema_with_files_in_default_location_" + randomNameSuffix();
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        env.executeTrinoUpdate("CREATE SCHEMA iceberg." + schemaName);
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after creating schema");
        env.executeTrinoUpdate("DROP SCHEMA iceberg." + schemaName);
        assertFileExistence(hdfsClient, schemaDir, true, "schema directory exists after dropping schema");
        assertFileExistence(hdfsClient, externalFile, true, "external file exists after dropping schema");

        hdfsClient.delete(externalFile);
        hdfsClient.delete(schemaDir);
    }

    private void assertFileExistence(HdfsClient hdfsClient, String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path)).as(description).isEqualTo(exists);
    }
}
