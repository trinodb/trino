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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests drop table compatibility between Iceberg connector and Spark Iceberg.
 * <p>
 * This test verifies that dropping an Iceberg table properly cleans up
 * the underlying data files, depending on which engine (Trino or Spark)
 * creates and drops the table.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestIcebergSparkDropTableCompatibility
{
    static Stream<Arguments> tableCleanupEngineConfigurations()
    {
        return Stream.of(
                Arguments.of(Engine.TRINO, Engine.TRINO),
                Arguments.of(Engine.TRINO, Engine.SPARK),
                Arguments.of(Engine.SPARK, Engine.SPARK),
                Arguments.of(Engine.SPARK, Engine.TRINO));
    }

    @ParameterizedTest(name = "creator={0}, dropper={1}")
    @MethodSource("tableCleanupEngineConfigurations")
    void testCleanupOnDropTable(Engine tableCreatorEngine, Engine tableDropperEngine, SparkIcebergEnvironment env)
    {
        // Set default catalogs (matching original test's @BeforeMethodWithContext)
        env.executeTrinoUpdate("USE iceberg.default");
        env.executeSparkUpdate("USE iceberg_test.default");

        HdfsClient hdfsClient = env.createHdfsClient();
        String tableName = "test_cleanup_on_drop_table" + randomNameSuffix();
        String trinoTableName = "iceberg.default." + tableName;
        String sparkTableName = "iceberg_test.default." + tableName;

        // Create table using the specified engine
        executeUpdate(
                env,
                tableCreatorEngine,
                "CREATE TABLE " + trinoTableName + "(col0 INT, col1 INT)",
                "CREATE TABLE " + sparkTableName + "(col0 INT, col1 INT)");
        // Always insert using Trino (original test does this)
        env.executeTrinoUpdate("INSERT INTO " + trinoTableName + " VALUES (1, 2)");

        // Get table location and verify directory exists
        String tableDirectory = SparkIcebergEnvironment.stripNamenodeURI(getTableLocation(env, trinoTableName));
        assertFileExistence(hdfsClient, tableDirectory, true, "The table directory exists after creating the table");

        // Get data file paths before dropping
        List<String> dataFilePaths = getDataFilePaths(env, trinoTableName);

        // Drop table using the specified engine
        // Note: PURGE option is required to remove data since Iceberg 0.14.0,
        // but the statement hangs in Spark, so we don't use it
        executeUpdate(
                env,
                tableDropperEngine,
                "DROP TABLE " + trinoTableName,
                "DROP TABLE " + sparkTableName);

        // Verify cleanup behavior
        // Note: Spark's behavior is Catalog dependent - it doesn't always remove files
        boolean expectExists = (tableDropperEngine == Engine.SPARK);
        assertFileExistence(hdfsClient, tableDirectory, expectExists,
                "The table directory " + tableDirectory + (expectExists ? " should still exist" : " should be removed") + " after dropping the table");
        for (String dataFilePath : dataFilePaths) {
            assertFileExistence(hdfsClient, dataFilePath, expectExists,
                    "The data file " + dataFilePath + (expectExists ? " should still exist" : " should be removed") + " after dropping the table");
        }
    }

    private void executeUpdate(SparkIcebergEnvironment env, Engine engine, String trinoSql, String sparkSql)
    {
        switch (engine) {
            case TRINO -> env.executeTrinoUpdate(trinoSql);
            case SPARK -> env.executeSparkUpdate(sparkSql);
        }
    }

    private void assertFileExistence(HdfsClient hdfsClient, String path, boolean exists, String description)
    {
        assertThat(hdfsClient.exist(path))
                .as(description)
                .isEqualTo(exists);
    }

    private String getTableLocation(SparkIcebergEnvironment env, String tableName)
    {
        return (String) env.executeTrino(
                "SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + tableName)
                .getOnlyValue();
    }

    private List<String> getDataFilePaths(SparkIcebergEnvironment env, String tableName)
    {
        String filesTableName = tableNameToFilesTable(tableName);
        List<Object> filePaths = env.executeTrino("SELECT file_path FROM " + filesTableName).column(1);
        return filePaths.stream()
                .map(path -> getPath((String) path))
                .toList();
    }

    private static String tableNameToFilesTable(String tableName)
    {
        int lastDot = tableName.lastIndexOf('.');
        if (lastDot < 0) {
            return "\"" + tableName + "$files\"";
        }
        String prefix = tableName.substring(0, lastDot + 1);
        String baseName = tableName.substring(lastDot + 1);
        return prefix + "\"" + baseName + "$files\"";
    }

    private static String getPath(String uri)
    {
        if (uri.startsWith("hdfs://")) {
            return URI.create(uri).getPath();
        }
        return uri;
    }

    /**
     * Enum representing which engine to use for table operations.
     */
    enum Engine
    {
        TRINO,
        SPARK
    }
}
