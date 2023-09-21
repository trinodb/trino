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
import io.trino.plugin.hive.AbstractTestHiveFileSystemS3;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;

public class TestHiveFileSystemS3SelectPushdown
        extends AbstractTestHiveFileSystemS3
{
    protected SchemaTableName tableWithPipeDelimiter;
    protected SchemaTableName tableWithCommaDelimiter;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.s3.endpoint",
            "hive.hadoop2.s3.awsAccessKey",
            "hive.hadoop2.s3.awsSecretKey",
            "hive.hadoop2.s3.writableBucket",
            "hive.hadoop2.s3.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String s3endpoint, String awsAccessKey, String awsSecretKey, String writableBucket, String testDirectory)
    {
        super.setup(host, port, databaseName, s3endpoint, awsAccessKey, awsSecretKey, writableBucket, testDirectory, true);
        tableWithPipeDelimiter = new SchemaTableName(database, "trino_s3select_test_external_fs_with_pipe_delimiter");
        tableWithCommaDelimiter = new SchemaTableName(database, "trino_s3select_test_external_fs_with_comma_delimiter");
    }

    @Test
    public void testGetRecordsWithPipeDelimiter()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(tableWithPipeDelimiter),
                MaterializedResult.resultBuilder(newSession(), BIGINT, BIGINT)
                    .row(1L, 2L).row(3L, 4L).row(55L, 66L) // test_table_with_pipe_delimiter.csv
                    .row(27L, 10L).row(8L, 2L).row(456L, 789L) // test_table_with_pipe_delimiter.csv.gzip
                    .row(22L, 11L).row(78L, 76L).row(1L, 2L).row(36L, 90L) // test_table_with_pipe_delimiter.csv.bz2
                    .build());
    }

    @Test
    public void testFilterRecordsWithPipeDelimiter()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                createBaseColumn("t_bigint", 0, HIVE_INT, BIGINT, REGULAR, Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(tableWithPipeDelimiter, projectedColumns),
                MaterializedResult.resultBuilder(newSession(), BIGINT)
                        .row(1L).row(3L).row(55L) // test_table_with_pipe_delimiter.csv
                        .row(27L).row(8L).row(456L) // test_table_with_pipe_delimiter.csv.gzip
                        .row(22L).row(78L).row(1L).row(36L) // test_table_with_pipe_delimiter.csv.bz2
                        .build());
    }

    @Test
    public void testGetRecordsWithCommaDelimiter()
            throws Exception
    {
        assertEqualsIgnoreOrder(
                readTable(tableWithCommaDelimiter),
                MaterializedResult.resultBuilder(newSession(), BIGINT, BIGINT)
                        .row(7L, 1L).row(19L, 10L).row(1L, 345L) // test_table_with_comma_delimiter.csv
                        .row(27L, 10L).row(28L, 9L).row(90L, 94L) // test_table_with_comma_delimiter.csv.gzip
                        .row(11L, 24L).row(1L, 6L).row(21L, 12L).row(0L, 0L) // test_table_with_comma_delimiter.csv.bz2
                        .build());
    }

    @Test
    public void testFilterRecordsWithCommaDelimiter()
            throws Exception
    {
        List<ColumnHandle> projectedColumns = ImmutableList.of(
                createBaseColumn("t_bigint", 0, HIVE_INT, BIGINT, REGULAR, Optional.empty()));

        assertEqualsIgnoreOrder(
                filterTable(tableWithCommaDelimiter, projectedColumns),
                MaterializedResult.resultBuilder(newSession(), BIGINT)
                        .row(7L).row(19L).row(1L) // test_table_with_comma_delimiter.csv
                        .row(27L).row(28L).row(90L) // test_table_with_comma_delimiter.csv.gzip
                        .row(11L).row(1L).row(21L).row(0L) // test_table_with_comma_delimiter.csv.bz2
                        .build());
    }
}
