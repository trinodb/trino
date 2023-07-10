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
package io.trino.plugin.hive.s3select;

import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.newSession;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.s3select.S3SelectTestHelper.expectedResult;
import static io.trino.plugin.hive.s3select.S3SelectTestHelper.isSplitCountInOpenInterval;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertTrue;

public class TestHiveFileSystemS3SelectJsonPushdownWithSplits
{
    private String host;
    private int port;
    private String databaseName;
    private String awsAccessKey;
    private String awsSecretKey;
    private String writableBucket;
    private String testDirectory;

    private SchemaTableName tableJsonWithSplits;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket",
            "hive.hadoop2.s3.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey, String writableBucket, String testDirectory)
    {
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.writableBucket = writableBucket;
        this.testDirectory = testDirectory;

        this.tableJsonWithSplits = new SchemaTableName(databaseName, "trino_s3select_test_json_scan_range_pushdown");
    }

    @DataProvider(name = "testSplitSize")
    public static Object[][] splitSizeParametersProvider()
    {
        return new Object[][] {{15, 10, 6, 12}, {50, 30, 2, 4}};
    }

    @Test(dataProvider = "testSplitSize")
    public void testQueryPushdownWithSplitSizeForJson(int maxSplitSizeKB,
                                                      int maxInitialSplitSizeKB,
                                                      int minSplitCount,
                                                      int maxSplitCount)
    {
        S3SelectTestHelper s3SelectTestHelper = null;
        try {
            HiveConfig hiveConfig = new HiveConfig()
                    .setS3SelectPushdownEnabled(true)
                    .setMaxSplitSize(DataSize.of(maxSplitSizeKB, KILOBYTE))
                    .setMaxInitialSplitSize(DataSize.of(maxInitialSplitSizeKB, KILOBYTE));
            s3SelectTestHelper = new S3SelectTestHelper(
                    host,
                    port,
                    databaseName,
                    awsAccessKey,
                    awsSecretKey,
                    writableBucket,
                    testDirectory,
                    hiveConfig);

            int tableSplitsCount = s3SelectTestHelper.getTableSplitsCount(tableJsonWithSplits);
            assertTrue(isSplitCountInOpenInterval(tableSplitsCount, minSplitCount, maxSplitCount));

            ColumnHandle indexColumn = createBaseColumn("col_1", 0, HIVE_INT, BIGINT, REGULAR, Optional.empty());
            MaterializedResult filteredTableResult = s3SelectTestHelper.getFilteredTableResult(tableJsonWithSplits, indexColumn);
            assertEqualsIgnoreOrder(filteredTableResult,
                    expectedResult(newSession(s3SelectTestHelper.getHiveConfig()), 1, 300));
        }
        finally {
            if (s3SelectTestHelper != null) {
                s3SelectTestHelper.tearDown();
            }
        }
    }
}
