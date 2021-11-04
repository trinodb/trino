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
import io.trino.tempto.query.QueryExecutionException;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
        String schemaName = "test_drop_schema";
        String schemaDir = warehouseDirectory + "/test_drop_schema.db";

        ensureSchemaDoesNotExist(schemaName);

        assertQuerySucceeds("CREATE SCHEMA test_drop_schema");
        assertThat(hdfsClient.exist(schemaDir))
                .as("Check if expected schema directory exists after creating schema")
                .isTrue();

        onTrino().executeQuery("CREATE TABLE test_drop_schema.test_drop (col1 int)");

        assertQueryFailure(() -> query("DROP SCHEMA test_drop_schema"))
                .hasMessageContaining("line 1:1: Cannot drop non-empty schema 'test_drop_schema'");

        onTrino().executeQuery("DROP TABLE test_drop_schema.test_drop");

        assertQuerySucceeds("DROP SCHEMA test_drop_schema");
        assertThat(hdfsClient.exist(schemaDir))
                .as("Check if schema directory exists after dropping schema")
                .isFalse();
    }

    @Test
    public void testDropSchemaWithEmptyLocation()
    {
        String schemaName = schemaName("schema_with_empty_location");
        String schemaDir = warehouseDirectory + "/schema-with-empty-location/";

        createSchema(schemaName, schemaDir);
        assertFileExists(schemaDir, true, "schema directory exists after create schema");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertFileExists(schemaDir, false, "schema directory exists after drop schema");
    }

    @Test
    public void testDropSchemaFilesWithoutLocation()
    {
        String schemaName = schemaName("schema_without_location");
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        createSchema(schemaName);
        assertFileExists(schemaDir, true, "schema directory exists after create schema");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertFileExists(schemaDir, false, "schema directory exists after drop schema");
    }

    @Test
    public void testDropSchemaFilesWithNonemptyLocation()
    {
        String schemaName = schemaName("schema_with_nonempty_location");
        String schemaDir = warehouseDirectory + "/schema-with-nonempty-location/";

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        createSchema(schemaName, schemaDir);
        assertFileExists(schemaDir, true, "schema directory exists after create schema");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertFileExists(schemaDir, true, "schema directory exists after drop schema");

        assertFileExists(externalFile, true, "external file exists after drop schema");

        hdfsClient.delete(externalFile);
    }

    // Tests create/drop schema transactions with default schema location
    @Test
    public void testDropSchemaFilesTransactions()
    {
        String schemaName = schemaName("schema_directory_transactions");
        String schemaDir = format("%s/%s.db/", warehouseDirectory, schemaName);

        createSchema(schemaName);
        assertFileExists(schemaDir, true, "schema directory exists after create schema");

        onTrino().executeQuery("START TRANSACTION");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertQuerySucceeds("ROLLBACK");
        assertFileExists(schemaDir, true, "schema directory exists after rollback");

        // Sanity check: schema is still working
        onTrino().executeQuery(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        onTrino().executeQuery(format("DROP TABLE %s.test_table", schemaName));

        onTrino().executeQuery("START TRANSACTION");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertQuerySucceeds("COMMIT");
        assertFileExists(schemaDir, false, "schema directory exists after drop schema");
    }

    @Test
    public void testDropSchemaFilesTransactionsWithExternalFiles()
    {
        String schemaName = schemaName("schema_transactions_with_external_files");
        String schemaDir = warehouseDirectory + "/schema-transactions-with-external-files/";

        // Create file in schema directory before creating schema
        String externalFile = schemaDir + "external-file";
        hdfsClient.createDirectory(schemaDir);
        hdfsClient.saveFile(externalFile, "");

        createSchema(schemaName, schemaDir);

        onTrino().executeQuery("START TRANSACTION");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertQuerySucceeds("ROLLBACK");
        assertFileExists(externalFile, true, "external file exists after rolling back drop schema");

        // Sanity check: schema is still working
        onTrino().executeQuery(format("CREATE TABLE %s.test_table (i integer)", schemaName));
        onTrino().executeQuery(format("DROP TABLE %s.test_table", schemaName));

        onTrino().executeQuery("START TRANSACTION");
        assertQuerySucceeds("DROP SCHEMA " + schemaName);
        assertQuerySucceeds("COMMIT");
        assertFileExists(externalFile, true, "schema directory exists after committing drop schema");
    }

    private void assertFileExists(String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path)).as("%s (%s)", description, path).isEqualTo(exists);
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

    private void createSchema(String name)
    {
        onTrino().executeQuery(format("CREATE SCHEMA %s", name));
    }

    private void createSchema(String name, String location)
    {
        onTrino().executeQuery(format("CREATE SCHEMA %s WITH (location = '%s')", name, location));
    }

    private static String schemaName(String name)
    {
        return format("%s_%s", name, randomTableSuffix());
    }

    private static void ensureSchemaDoesNotExist(String schemaName)
    {
        onHive().executeQuery(format("DROP DATABASE IF EXISTS %s CASCADE", schemaName));
    }
}
