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

import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.deltalake.procedure.DeltaTableOptimizeHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.hive.NodeVersion;
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

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.changeDataFeedEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static java.util.Objects.requireNonNull;

public class DeltaLakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PageIndexerFactory pageIndexerFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final DeltaLakeWriterStats stats;
    private final int maxPartitionsPerWriter;
    private final DateTimeZone parquetDateTimeZone;
    private final TypeManager typeManager;
    private final String trinoVersion;

    @Inject
    public DeltaLakePageSinkProvider(
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            DeltaLakeWriterStats stats,
            DeltaLakeConfig deltaLakeConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.pageIndexerFactory = pageIndexerFactory;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.hdfsEnvironment = hdfsEnvironment;
        this.dataFileInfoCodec = dataFileInfoCodec;
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.stats = stats;
        this.maxPartitionsPerWriter = deltaLakeConfig.getMaxPartitionsPerWriter();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoVersion = nodeVersion.toString();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeOutputTableHandle tableHandle = (DeltaLakeOutputTableHandle) outputTableHandle;
        return new DeltaLakePageSink(
                tableHandle.getInputColumns(),
                tableHandle.getPartitionedBy(),
                pageIndexerFactory,
                hdfsEnvironment,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                tableHandle.getLocation(),
                session,
                stats,
                typeManager,
                trinoVersion);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeInsertTableHandle tableHandle = (DeltaLakeInsertTableHandle) insertTableHandle;
        return new DeltaLakePageSink(
                tableHandle.getInputColumns(),
                tableHandle.getMetadataEntry().getOriginalPartitionColumns(),
                pageIndexerFactory,
                hdfsEnvironment,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                tableHandle.getLocation(),
                session,
                stats,
                typeManager,
                trinoVersion);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeTableExecuteHandle executeHandle = (DeltaLakeTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.getProcedureHandle();
                return new DeltaLakePageSink(
                        optimizeHandle.getTableColumns(),
                        optimizeHandle.getOriginalPartitionColumns(),
                        pageIndexerFactory,
                        hdfsEnvironment,
                        fileSystemFactory,
                        maxPartitionsPerWriter,
                        dataFileInfoCodec,
                        executeHandle.getTableLocation(),
                        session,
                        stats,
                        typeManager,
                        trinoVersion);
        }

        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.getProcedureId());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        DeltaLakeMergeTableHandle merge = (DeltaLakeMergeTableHandle) mergeHandle;
        DeltaLakeInsertTableHandle tableHandle = merge.getInsertTableHandle();
        ConnectorPageSink pageSink = createPageSink(transactionHandle, session, tableHandle, pageSinkId);

        return new DeltaLakeMergeSink(
                fileSystemFactory,
                session,
                parquetDateTimeZone,
                trinoVersion,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                stats,
                tableHandle.getLocation(),
                pageSink,
                tableHandle.getInputColumns(),
                () -> createCDFPageSink(merge, session),
                changeDataFeedEnabled(tableHandle.getMetadataEntry()));
    }

    private DeltaLakeCDFPageSink createCDFPageSink(
            DeltaLakeMergeTableHandle mergeTableHandle,
            ConnectorSession session)
    {
        MetadataEntry metadataEntry = mergeTableHandle.getTableHandle().getMetadataEntry();
        Set<String> partitionKeys = mergeTableHandle.getTableHandle().getMetadataEntry().getOriginalPartitionColumns().stream().collect(toImmutableSet());
        List<DeltaLakeColumnHandle> allColumns = extractSchema(metadataEntry, typeManager).stream()
                .map(metadata -> new DeltaLakeColumnHandle(
                        metadata.getName(),
                        metadata.getType(),
                        metadata.getFieldId(),
                        metadata.getPhysicalName(),
                        metadata.getPhysicalColumnType(),
                        partitionKeys.contains(metadata.getName()) ? PARTITION_KEY : REGULAR))
                .collect(toImmutableList());

        return new DeltaLakeCDFPageSink(
                allColumns,
                metadataEntry.getOriginalPartitionColumns(),
                pageIndexerFactory,
                hdfsEnvironment,
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                mergeTableHandle.getTableHandle().getLocation() + "/_change_data/",
                mergeTableHandle.getTableHandle().getLocation(),
                session,
                stats,
                typeManager,
                trinoVersion);
    }
}
