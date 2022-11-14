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

import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tests.product.hive.Engine;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.Engine.SPARK;
import static io.trino.tests.product.hive.Engine.TRINO;
import static io.trino.tests.product.iceberg.util.IcebergTestUtils.getTableLocation;
import static io.trino.tests.product.iceberg.util.IcebergTestUtils.stripNamenodeURI;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

/**
 * Tests drop table compatibility between Iceberg connector and Spark Iceberg.
 */
public class TestIcebergSparkDropTableCompatibility
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void useIceberg()
    {
        onTrino().executeQuery("USE iceberg.default");
        // see spark-defaults.conf
        onSpark().executeQuery("USE iceberg_test.default");
    }

    @DataProvider
    public static Object[][] tableCleanupEngineConfigurations()
    {
        return new Object[][] {
                {TRINO, TRINO},
                {TRINO, SPARK},
                {SPARK, SPARK},
                {SPARK, TRINO},
        };
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS}, dataProvider = "tableCleanupEngineConfigurations")
    public void testCleanupOnDropTable(Engine tableCreatorEngine, Engine tableDropperEngine)
    {
        String tableName = "test_cleanup_on_drop_table" + randomNameSuffix();

        tableCreatorEngine.queryExecutor().executeQuery("CREATE TABLE " + tableName + "(col0 INT, col1 INT)");
        onTrino().executeQuery("INSERT INTO " + tableName + " VALUES (1, 2)");

        String tableDirectory = stripNamenodeURI(getTableLocation(tableName));
        assertFileExistence(tableDirectory, true, "The table directory exists after creating the table");
        List<String> dataFilePaths = getDataFilePaths(tableName);

        tableDropperEngine.queryExecutor().executeQuery("DROP TABLE " + tableName); // PURGE option is required to remove data since Iceberg 0.14.0, but the statement hangs in Spark
        boolean expectExists = tableDropperEngine == SPARK; // Note: Spark's behavior is Catalog dependent
        assertFileExistence(tableDirectory, expectExists, format("The table directory %s should be removed after dropping the table", tableDirectory));
        dataFilePaths.forEach(dataFilePath -> assertFileExistence(dataFilePath, expectExists, format("The data file %s removed after dropping the table", dataFilePath)));
    }

    private void assertFileExistence(String path, boolean exists, String description)
    {
        Assertions.assertThat(hdfsClient.exist(path)).as(description).isEqualTo(exists);
    }

    private static List<String> getDataFilePaths(String icebergTableName)
    {
        List<String> filePaths = onTrino().executeQuery(format("SELECT file_path FROM \"%s$files\"", icebergTableName)).column(1);
        return filePaths.stream().map(TestIcebergSparkDropTableCompatibility::getPath).collect(Collectors.toList());
    }

    private static String getPath(String uri)
    {
        if (uri.startsWith("hdfs://")) {
            try {
                return new URI(uri).getPath();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException("Invalid syntax for the URI: " + uri, e);
            }
        }

        return uri;
    }
}
