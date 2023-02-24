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
package io.trino.plugin.deltalake;

import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorContext;

import java.io.IOException;
import java.util.List;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingConnectorSession.SESSION;

public final class TestingDeltaLakeUtils
{
    private TestingDeltaLakeUtils() {}

    public static List<AddFileEntry> getAddFileEntries(String tableLocation)
            throws IOException
    {
        SchemaTableName dummyTable = new SchemaTableName("dummy_schema_placeholder", "dummy_table_placeholder");
        TestingConnectorContext context = new TestingConnectorContext();

        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                context.getTypeManager(),
                new CheckpointSchemaManager(context.getTypeManager()),
                new DeltaLakeConfig(),
                new FileFormatDataSourceStats(),
                new HdfsFileSystemFactory(HDFS_ENVIRONMENT),
                new ParquetReaderConfig());

        return transactionLogAccess.getActiveFiles(transactionLogAccess.loadSnapshot(dummyTable, tableLocation, SESSION), SESSION);
    }
}
