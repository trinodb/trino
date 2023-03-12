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
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.deltalake.procedure.DeltaTableOptimizeHandle;
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
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DeltaLakePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PageIndexerFactory pageIndexerFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final DeltaLakeWriterStats stats;
    private final int maxPartitionsPerWriter;
    private final DateTimeZone parquetDateTimeZone;
    private final String trinoVersion;
    private final int domainCompactionThreshold;

    @Inject
    public DeltaLakePageSinkProvider(
            PageIndexerFactory pageIndexerFactory,
            TrinoFileSystemFactory fileSystemFactory,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            DeltaLakeWriterStats stats,
            DeltaLakeConfig deltaLakeConfig,
            NodeVersion nodeVersion)
    {
        this.pageIndexerFactory = pageIndexerFactory;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataFileInfoCodec = dataFileInfoCodec;
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.stats = stats;
        this.maxPartitionsPerWriter = deltaLakeConfig.getMaxPartitionsPerWriter();
        this.parquetDateTimeZone = deltaLakeConfig.getParquetDateTimeZone();
        this.domainCompactionThreshold = deltaLakeConfig.getDomainCompactionThreshold();
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
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                tableHandle.getLocation(),
                session,
                stats,
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
                fileSystemFactory,
                maxPartitionsPerWriter,
                dataFileInfoCodec,
                tableHandle.getLocation(),
                session,
                stats,
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
                        fileSystemFactory,
                        maxPartitionsPerWriter,
                        dataFileInfoCodec,
                        executeHandle.getTableLocation(),
                        session,
                        stats,
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
                domainCompactionThreshold);
    }
}
