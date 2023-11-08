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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakePartitionsTable
        implements SystemTable
{
    private final SchemaTableName tableName;
    private final String tableLocation;
    private final TransactionLogAccess transactionLogAccess;
    private final ConnectorTableMetadata tableMetadata;
    private final List<DeltaLakeColumnHandle> partitionColumns;

    public DeltaLakePartitionsTable(
            SchemaTableName tableName,
            String tableLocation,
            TransactionLogAccess transactionLogAccess,
            List<DeltaLakeColumnHandle> partitionColumns)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        partitionColumns.forEach(column -> columns.add(new ColumnMetadata(column.getBaseColumnName(), VARCHAR)));

        this.tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                columns.build());
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            TupleDomain<Integer> constraint)
    {
        TableSnapshot tableSnapshot;
        MetadataEntry metadataEntry;
        try {
            // Verify the transaction log is readable
            SchemaTableName baseTableName = new SchemaTableName(tableName.getSchemaName(),
                    DeltaLakeTableName.tableNameFrom(tableName.getTableName()));
            tableSnapshot = transactionLogAccess.getSnapshot(session, baseTableName, tableLocation, Optional.empty());
            metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
        }
        catch (IOException e) {
            throw new TrinoException(DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA,
                    "Unable to load table metadata from location: " + tableLocation, e);
        }
        if (partitionColumns.isEmpty()) {
            return new EmptyPageSource();
        }
        return new FixedPageSource(buildPages(session, tableSnapshot, metadataEntry, constraint));
    }

    private List<Page> buildPages(ConnectorSession session, TableSnapshot tableSnapshot, MetadataEntry metadataEntry,
            TupleDomain<Integer> constraint)
    {
        PageListBuilder pageListBuilder = PageListBuilder.forTable(tableMetadata);

        List<AddFileEntry> activeFiles = transactionLogAccess.getActiveFiles(tableSnapshot, metadataEntry,
                transactionLogAccess.getProtocolEntry(session, tableSnapshot), session);

        for (Map<String, Optional<String>> partitionValue : getDedupedPartitionValues(activeFiles)) {
            if (partitionValue.isEmpty()) {
                pageListBuilder.beginRow();
                pageListBuilder.appendNull();
                pageListBuilder.endRow();
                continue;
            }

            pageListBuilder.beginRow();
            for (int i = 0; i < partitionColumns.size(); i++) {
                DeltaLakeColumnHandle column = partitionColumns.get(i);
                String actualPartitionValue = partitionValue.get(column.getBaseColumnName()).orElse(null);
                if (actualPartitionValue == null) {
                    pageListBuilder.appendNull();
                }
                else {
                    pageListBuilder.appendVarchar(actualPartitionValue);
                }
            }
            pageListBuilder.endRow();
        }
        return pageListBuilder.build();
    }

    private ImmutableList<Map<String, Optional<String>>> getDedupedPartitionValues(List<AddFileEntry> addFileEntries)
    {
        return addFileEntries.stream()
                .map(AddFileEntry::getCanonicalPartitionValues)
                .distinct()
                .collect(toImmutableList());
    }
}
