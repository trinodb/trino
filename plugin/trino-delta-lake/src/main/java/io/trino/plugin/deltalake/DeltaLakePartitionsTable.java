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
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogParser;
import io.trino.plugin.deltalake.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

public class DeltaLakePartitionsTable
        implements SystemTable
{
    private final SchemaTableName tableName;
    private final String tableLocation;
    private final TransactionLogAccess transactionLogAccess;
    private final TypeManager typeManager;
    private final ConnectorTableMetadata tableMetadata;
    private final List<DeltaLakeColumnHandle> partitionColumns;
    private final List<RowType.Field> partitionFields;

    public DeltaLakePartitionsTable(
            ConnectorSession session,
            SchemaTableName tableName,
            String tableLocation,
            TransactionLogAccess transactionLogAccess,
            TypeManager typeManager)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.partitionColumns = getPartitionColumns(session, this.tableName, this.tableLocation);

        this.partitionFields = this.partitionColumns.stream()
                .map(column -> RowType.field(column.getBaseColumnName(), column.getType()))
                .collect(toImmutableList());

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        if (!this.partitionFields.isEmpty()) {
            columnMetadataBuilder.add(new ColumnMetadata("partition", RowType.from(this.partitionFields)));
        }

        columnMetadataBuilder.add(new ColumnMetadata("file_count", BIGINT));
        columnMetadataBuilder.add(new ColumnMetadata("total_size", BIGINT));

        this.tableMetadata = new ConnectorTableMetadata(tableName, columnMetadataBuilder.build());
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

        try {
            // Verify the transaction log is readable
            tableSnapshot = transactionLogAccess.loadSnapshot(session, tableName, tableLocation);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                    "Unable to load table metadata from location: " + tableLocation, e);
        }

        if (partitionColumns.isEmpty()) {
            return new EmptyPageSource();
        }
        return new FixedPageSource(buildPages(session, tableSnapshot));
    }

    private List<Page> buildPages(ConnectorSession session, TableSnapshot tableSnapshot)
    {
        PageListBuilder pageListBuilder = PageListBuilder.forTable(tableMetadata);
        MetadataEntry metadataEntry = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(session, tableSnapshot);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry, protocolEntry);

        try (Stream<AddFileEntry> activeFiles = transactionLogAccess.loadActiveFiles(session, tableSnapshot, metadataEntry, protocolEntry, TupleDomain.all(), alwaysTrue())) {
            for (Map.Entry<Map<String, Optional<String>>, DeltaLakePartitionStatistics> partitionEntry : getStatisticsByPartition(activeFiles).entrySet()) {
                Map<String, Optional<String>> partitionValue = partitionEntry.getKey();
                DeltaLakePartitionStatistics deltaLakePartitionStatistics = partitionEntry.getValue();

                RowType partitionValuesRowType = RowType.from(this.partitionFields);
                SqlRow partitionValuesRow = buildRowValue(partitionValuesRowType, fields -> {
                    for (int i = 0; i < partitionColumns.size(); i++) {
                        DeltaLakeColumnHandle column = partitionColumns.get(i);
                        Type type = column.getType();
                        Optional<String> value = switch (columnMappingMode) {
                            case NONE:
                                yield partitionValue.get(column.getBaseColumnName());
                            case ID, NAME:
                                yield partitionValue.get(column.getBasePhysicalColumnName());
                            default:
                                throw new IllegalStateException("Unknown column mapping mode");
                        };
                        Object deserializedPartitionValue = TransactionLogParser.deserializePartitionValue(column, value);
                        writeNativeValue(type, fields.get(i), deserializedPartitionValue);
                    }
                });

                pageListBuilder.beginRow();

                pageListBuilder.appendNativeValue(partitionValuesRowType, partitionValuesRow);
                pageListBuilder.appendBigint(deltaLakePartitionStatistics.fileCount());
                pageListBuilder.appendBigint(deltaLakePartitionStatistics.size());

                pageListBuilder.endRow();
            }
        }

        return pageListBuilder.build();
    }

    private Map<Map<String, Optional<String>>, DeltaLakePartitionStatistics> getStatisticsByPartition(Stream<AddFileEntry> addFileEntryStream)
    {
        Map<Map<String, Optional<String>>, DeltaLakePartitionStatistics.Builder> partitionValueStatistics = new HashMap<>();

        addFileEntryStream.forEach(addFileEntry -> {
            Map<String, Optional<String>> partitionValues = addFileEntry.getCanonicalPartitionValues();
            partitionValueStatistics.computeIfAbsent(partitionValues, key -> new DeltaLakePartitionStatistics.Builder())
                    .acceptAddFileEntry(addFileEntry);
        });

        return partitionValueStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
    }

    private List<DeltaLakeColumnHandle> getPartitionColumns(ConnectorSession session, SchemaTableName tableName, String tableLocation)
    {
        try {
            TableSnapshot tableSnapshot = transactionLogAccess.loadSnapshot(session, tableName, tableLocation);
            MetadataEntry metadata = transactionLogAccess.getMetadataEntry(session, tableSnapshot);
            Map<String, DeltaLakeColumnMetadata> columnsMetadataByName = extractSchema(metadata, transactionLogAccess.getProtocolEntry(session, tableSnapshot), typeManager).stream()
                    .collect(toImmutableMap(DeltaLakeColumnMetadata::getName, Function.identity()));
            return metadata.getOriginalPartitionColumns().stream()
                .map(partitionColumnName ->
                    new DeltaLakeColumnHandle(
                        columnsMetadataByName.get(partitionColumnName).getName(),
                        columnsMetadataByName.get(partitionColumnName).getType(), OptionalInt.empty(),
                        columnsMetadataByName.get(partitionColumnName).getPhysicalName(),
                        columnsMetadataByName.get(partitionColumnName).getPhysicalColumnType(), PARTITION_KEY, Optional.empty()))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Unable to load table metadata from location: " + tableLocation, e);
        }
    }

    private record DeltaLakePartitionStatistics(long fileCount, long size)
    {
        public static class Builder
        {
            private long fileCount;
            private long size;

            public void acceptAddFileEntry(AddFileEntry addFileEntry)
            {
                fileCount++;
                size += addFileEntry.getSize();
            }

            public DeltaLakePartitionStatistics build()
            {
                return new DeltaLakePartitionStatistics(fileCount, size);
            }
        }
    }
}
