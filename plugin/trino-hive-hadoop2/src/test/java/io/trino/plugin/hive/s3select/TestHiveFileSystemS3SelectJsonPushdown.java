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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.filterTable;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.newSession;
import static io.trino.plugin.hive.HiveFileSystemTestUtils.readTable;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;

public class TestHiveFileSystemS3SelectJsonPushdown
{
    private SchemaTableName tableJson;

    private S3SelectTestHelper s3SelectTestHelper;

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
        s3SelectTestHelper = new S3SelectTestHelper(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket, testDirectory);
        tableJson = new SchemaTableName(databaseName, "trino_s3select_test_external_fs_json");
    }

    @Test
    public void testGetRecordsJson()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(tableJson,
                        s3SelectTestHelper.getTransactionManager(),
                        s3SelectTestHelper.getHiveConfig(),
                        s3SelectTestHelper.getPageSourceProvider(),
                        s3SelectTestHelper.getSplitManager()),
                MaterializedResult.resultBuilder(newSession(s3SelectTestHelper.getHiveConfig()), BIGINT, BIGINT)
                        .row(2L, 4L).row(5L, 6L) // test_table.json
                        .row(7L, 23L).row(28L, 22L).row(13L, 10L) // test_table.json.gz
                        .row(1L, 19L).row(6L, 3L).row(24L, 22L).row(100L, 77L) // test_table.json.bz2
                        .build());
    }

    @Test
    public void testFilterRecordsJson()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                createBaseColumn("col_1", 0, HIVE_INT, BIGINT, REGULAR, Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(tableJson,
                        projectedColumns,
                        s3SelectTestHelper.getTransactionManager(),
                        s3SelectTestHelper.getHiveConfig(),
                        s3SelectTestHelper.getPageSourceProvider(),
                        s3SelectTestHelper.getSplitManager()),
                MaterializedResult.resultBuilder(newSession(s3SelectTestHelper.getHiveConfig()), BIGINT)
                        .row(2L).row(5L) // test_table.json
                        .row(7L).row(28L).row(13L) // test_table.json.gz
                        .row(1L).row(6L).row(24L).row(100L) // test_table.json.bz2
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        s3SelectTestHelper.tearDown();
    }
}
