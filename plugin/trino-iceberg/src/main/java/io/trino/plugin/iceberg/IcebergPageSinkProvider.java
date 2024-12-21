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
package io.trino.plugin.iceberg;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.SortingFileWriterConfig;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.LocationProvider;

import java.util.Map;

import static com.google.common.collect.Maps.transformValues;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static java.util.Objects.requireNonNull;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final IcebergFileSystemFactory fileSystemFactory;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;
    private final DataSize sortingFileWriterBufferSize;
    private final int sortingFileWriterMaxOpenFiles;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;

    @Inject
    public IcebergPageSinkProvider(
            IcebergFileSystemFactory fileSystemFactory,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergConfig config,
            SortingFileWriterConfig sortingFileWriterConfig,
            TypeManager typeManager,
            PageSorter pageSorter)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.maxOpenPartitions = config.getMaxPartitionsPerWriter();
        this.sortingFileWriterBufferSize = sortingFileWriterConfig.getWriterSortBufferSize();
        this.sortingFileWriterMaxOpenFiles = sortingFileWriterConfig.getMaxOpenSortFiles();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(session, (IcebergWritableTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return createPageSink(session, (IcebergWritableTableHandle) insertTableHandle);
    }

    private ConnectorPageSink createPageSink(ConnectorSession session, IcebergWritableTableHandle tableHandle)
    {
        Schema schema = SchemaParser.fromJson(tableHandle.schemaAsJson());
        String partitionSpecJson = tableHandle.partitionsSpecsAsJson().get(tableHandle.partitionSpecId());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, partitionSpecJson);
        LocationProvider locationProvider = getLocationProvider(tableHandle.name(), tableHandle.outputPath(), tableHandle.storageProperties());
        return new IcebergPageSink(
                schema,
                partitionSpec,
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                fileSystemFactory.create(session.getIdentity(), tableHandle.fileIoProperties()),
                tableHandle.inputColumns(),
                jsonCodec,
                session,
                tableHandle.fileFormat(),
                tableHandle.storageProperties(),
                maxOpenPartitions,
                tableHandle.sortOrder(),
                sortingFileWriterBufferSize,
                sortingFileWriterMaxOpenFiles,
                typeManager,
                pageSorter);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
                Schema schema = SchemaParser.fromJson(optimizeHandle.schemaAsJson());
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, optimizeHandle.partitionSpecAsJson());
                LocationProvider locationProvider = getLocationProvider(executeHandle.schemaTableName(),
                        executeHandle.tableLocation(), optimizeHandle.tableStorageProperties());
                return new IcebergPageSink(
                        schema,
                        partitionSpec,
                        locationProvider,
                        fileWriterFactory,
                        pageIndexerFactory,
                        fileSystemFactory.create(session.getIdentity(), executeHandle.fileIoProperties()),
                        optimizeHandle.tableColumns(),
                        jsonCodec,
                        session,
                        optimizeHandle.fileFormat(),
                        optimizeHandle.tableStorageProperties(),
                        maxOpenPartitions,
                        optimizeHandle.sortOrder(),
                        sortingFileWriterBufferSize,
                        sortingFileWriterMaxOpenFiles,
                        typeManager,
                        pageSorter);
            case DROP_EXTENDED_STATS:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
            case CREATE_BRANCH:
            case DROP_BRANCH:
            case FAST_FORWARD:
                // handled via ConnectorMetadata.executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure: " + executeHandle.procedureId());
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        IcebergMergeTableHandle merge = (IcebergMergeTableHandle) mergeHandle;
        IcebergWritableTableHandle tableHandle = merge.getInsertTableHandle();
        LocationProvider locationProvider = getLocationProvider(tableHandle.name(), tableHandle.outputPath(), tableHandle.storageProperties());
        Schema schema = SchemaParser.fromJson(tableHandle.schemaAsJson());
        Map<Integer, PartitionSpec> partitionsSpecs = transformValues(tableHandle.partitionsSpecsAsJson(), json -> PartitionSpecParser.fromJson(schema, json));
        ConnectorPageSink pageSink = createPageSink(session, tableHandle);

        return new IcebergMergeSink(
                locationProvider,
                fileWriterFactory,
                fileSystemFactory.create(session.getIdentity(), tableHandle.fileIoProperties()),
                jsonCodec,
                session,
                tableHandle.fileFormat(),
                tableHandle.storageProperties(),
                schema,
                partitionsSpecs,
                pageSink,
                schema.columns().size());
    }
}
