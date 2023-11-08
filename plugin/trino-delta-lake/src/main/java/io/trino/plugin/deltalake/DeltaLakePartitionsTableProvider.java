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

import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class DeltaLakePartitionsTableProvider
{
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;

    public DeltaLakePartitionsTableProvider(
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        this.transactionLogAccess = transactionLogAccess;
        this.typeManager = typeManager;
    }

    public DeltaLakePartitionsTable getDeltaLakePartitionsTable(SchemaTableName tableName, String tableLocation, ConnectorSession session)
    {
        List<DeltaLakeColumnHandle> partitionColumns = getPartitionColumns(tableName, tableLocation, session);
        return new DeltaLakePartitionsTable(
                tableName,
                tableLocation,
                transactionLogAccess,
                partitionColumns);
    }

    public List<DeltaLakeColumnHandle> getPartitionColumns(SchemaTableName tableName, String tableLocation, ConnectorSession session)
    {
        try {
            // Verify the transaction log is readable
            SchemaTableName baseTableName = new SchemaTableName(tableName.getSchemaName(), DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            TableSnapshot tableSnapshot = transactionLogAccess.getSnapshot(session, baseTableName, tableLocation, Optional.empty());
            MetadataEntry metadata = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
            return DeltaLakeSchemaSupport.extractPartitionColumns(metadata, transactionLogAccess.getProtocolEntry(session, tableSnapshot), typeManager);
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + tableLocation, e);
        }
    }
}
