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

import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TrackingTransactionLogAccess
        extends TransactionLogAccess
{
    private final AccessTrackingFileSystem fileSystem;

    public TrackingTransactionLogAccess(
            String tableName,
            Path tableLocation,
            ConnectorSession session,
            TypeManager typeManager,
            CheckpointSchemaManager checkpointSchemaManager,
            DeltaLakeConfig deltaLakeConfig,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            HdfsEnvironment hdfsEnvironment,
            ParquetReaderConfig parquetReaderConfig)
    {
        super(typeManager, checkpointSchemaManager, deltaLakeConfig, fileFormatDataSourceStats, hdfsEnvironment, parquetReaderConfig);
        this.fileSystem = new AccessTrackingFileSystem(super.getFileSystem(tableLocation, new SchemaTableName("schema", tableName), session));
    }

    public AccessTrackingFileSystem getAccessTrackingFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected FileSystem getFileSystem(Path tableLocation, SchemaTableName table, ConnectorSession session)
    {
        return fileSystem;
    }
}
