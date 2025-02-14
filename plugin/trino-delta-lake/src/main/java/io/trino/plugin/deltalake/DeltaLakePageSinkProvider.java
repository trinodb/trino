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
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.deltalake.procedure.DeltaTableOptimizeHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeCdfPageSink.CHANGE_DATA_FOLDER_NAME;
import static io.trino.plugin.deltalake.DeltaLakeCdfPageSink.CHANGE_TYPE_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeParquetSchemas.createParquetSchemaMapping;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.changeDataFeedEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getRandomPrefixLength;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isDeletionVectorEnabled;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PageIndexerFactory pageIndexerFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final DeltaLakeWriterStats stats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final int maxPartitionsPerWriter;
    private final DateTimeZone parquetDateTimeZone;
    private final TypeManager typeManager;
    private final String trinoVersion;
    private final int domainCompactionThreshold;

    @Inject
    public DeltaLakePageSinkProvider(
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            DeltaLakeWriterStats stats,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            DeltaLakeConfig deltaLakeConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.pageIndexerFactory = pageIndexerFactory;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataFileInfoCodec = dataFileInfoCodec;
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.stats = stats;
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.maxPartitionsPerWriter = deltaLakeConfig.getMaxPartitionsPerWriter();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoVersion = nodeVersion.toString();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeOutputTableHandle tableHandle = (DeltaLakeOutputTableHandle) outputTableHandle;
        DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(
                tableHandle.schemaString(),
                typeManager,
                tableHandle.columnMappingMode(),
                tableHandle.partitionedBy());
        return new DeltaLakePageSink(
                typeManager.getTypeOperators(),
                tableHandle.inputColumns(),
                tableHandle.partitionedBy(),
                pageIndexerFactory,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                Location.of(tableHandle.location()),
                session,
                stats,
                trinoVersion,
                parquetSchemaMapping);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeInsertTableHandle tableHandle = (DeltaLakeInsertTableHandle) insertTableHandle;
        MetadataEntry metadataEntry = tableHandle.metadataEntry();
        DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(metadataEntry, tableHandle.protocolEntry(), typeManager);
        return new DeltaLakePageSink(
                typeManager.getTypeOperators(),
                tableHandle.inputColumns(),
                tableHandle.metadataEntry().getOriginalPartitionColumns(),
                pageIndexerFactory,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                Location.of(tableHandle.location()),
                session,
                stats,
                trinoVersion,
                parquetSchemaMapping);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeTableExecuteHandle executeHandle = (DeltaLakeTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.procedureHandle();
                DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(optimizeHandle.getMetadataEntry(), optimizeHandle.getProtocolEntry(), typeManager);
                return new DeltaLakePageSink(
                        typeManager.getTypeOperators(),
                        optimizeHandle.getTableColumns(),
                        optimizeHandle.getOriginalPartitionColumns(),
                        pageIndexerFactory,
                        fileSystemFactory,
                        maxPartitionsPerWriter,
                        dataFileInfoCodec,
                        Location.of(executeHandle.tableLocation()),
                        session,
                        stats,
                        trinoVersion,
                        parquetSchemaMapping);
        }

        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.procedureId());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeMergeTableHandle merge = (DeltaLakeMergeTableHandle) mergeHandle;
        DeltaLakeInsertTableHandle tableHandle = merge.insertTableHandle();
        ConnectorPageSink pageSink = createPageSink(transactionHandle, session, tableHandle, pageSinkId);
        DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(tableHandle.metadataEntry(), tableHandle.protocolEntry(), typeManager);

        return new DeltaLakeMergeSink(
                typeManager.getTypeOperators(),
                fileSystemFactory,
                session,
                parquetDateTimeZone,
                trinoVersion,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                stats,
                Location.of(tableHandle.location()),
                pageSink,
                tableHandle.inputColumns(),
                domainCompactionThreshold,
                () -> createCdfPageSink(merge, session),
                changeDataFeedEnabled(tableHandle.metadataEntry(), tableHandle.protocolEntry()).orElse(false),
                parquetSchemaMapping,
                parquetReaderOptions,
                fileFormatDataSourceStats,
                isDeletionVectorEnabled(tableHandle.metadataEntry(), tableHandle.protocolEntry()),
                merge.deletionVectors(),
                getRandomPrefixLength(tableHandle.metadataEntry()));
    }

    private DeltaLakeCdfPageSink createCdfPageSink(
            DeltaLakeMergeTableHandle mergeTableHandle,
            ConnectorSession session)
    {
        MetadataEntry metadataEntry = mergeTableHandle.tableHandle().getMetadataEntry();
        ProtocolEntry protocolEntry = mergeTableHandle.tableHandle().getProtocolEntry();
        Set<String> partitionKeys = mergeTableHandle.tableHandle().getMetadataEntry().getOriginalPartitionColumns().stream().collect(toImmutableSet());
        List<DeltaLakeColumnHandle> tableColumns = extractSchema(metadataEntry, protocolEntry, typeManager).stream()
                .map(metadata -> new DeltaLakeColumnHandle(
                        metadata.name(),
                        metadata.type(),
                        metadata.fieldId(),
                        metadata.physicalName(),
                        metadata.physicalColumnType(),
                        partitionKeys.contains(metadata.name()) ? PARTITION_KEY : REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());
        List<DeltaLakeColumnHandle> allColumns = ImmutableList.<DeltaLakeColumnHandle>builder()
                .addAll(tableColumns)
                .add(new DeltaLakeColumnHandle(
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        OptionalInt.empty(),
                        CHANGE_TYPE_COLUMN_NAME,
                        VARCHAR,
                        REGULAR,
                        Optional.empty()))
                .build();
        Location tableLocation = Location.of(mergeTableHandle.tableHandle().getLocation());

        DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(metadataEntry, protocolEntry, typeManager, true);

        return new DeltaLakeCdfPageSink(
                typeManager.getTypeOperators(),
                allColumns,
                metadataEntry.getOriginalPartitionColumns(),
                pageIndexerFactory,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                tableLocation,
                tableLocation.appendPath(CHANGE_DATA_FOLDER_NAME),
                session,
                stats,
                trinoVersion,
                parquetSchemaMapping);
    }
}
