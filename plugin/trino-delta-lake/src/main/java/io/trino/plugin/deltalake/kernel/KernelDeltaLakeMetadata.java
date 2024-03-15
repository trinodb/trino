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
package io.trino.plugin.deltalake.kernel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DataFileInfo;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeInputInfo;
import io.trino.plugin.deltalake.DeltaLakeMergeResult;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeRedirectionsProvider;
import io.trino.plugin.deltalake.DeltaLakeTable;
import io.trino.plugin.deltalake.DeltaLakeTableName;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileSizeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.pathColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.deltalake.kernel.KernelClient.createEngine;
import static io.trino.plugin.deltalake.kernel.KernelSchemaUtils.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class KernelDeltaLakeMetadata
        extends DeltaLakeMetadata
{
    public KernelDeltaLakeMetadata(DeltaLakeMetastore metastore,
            TransactionLogAccess transactionLogAccess,
            DeltaLakeTableStatisticsProvider tableStatisticsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            AccessControlMetadata accessControlMetadata,
            TrinoViewHiveMetastore trinoViewHiveMetastore,
            int domainCompactionThreshold, boolean unsafeWritesEnabled,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            long defaultCheckpointInterval,
            boolean deleteSchemaLocationsFallback,
            DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider,
            CachingExtendedStatisticsAccess statisticsAccess,
            DeltaLakeTableMetadataScheduler metadataScheduler,
            boolean useUniqueTableLocation,
            boolean allowManagedTableRename)
    {
        super(
                metastore,
                transactionLogAccess,
                tableStatisticsProvider,
                fileSystemFactory,
                typeManager,
                accessControlMetadata,
                trinoViewHiveMetastore,
                domainCompactionThreshold,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                transactionLogWriterFactory,
                nodeManager,
                checkpointWriterManager,
                defaultCheckpointInterval,
                deleteSchemaLocationsFallback,
                deltaLakeRedirectionsProvider,
                statisticsAccess,
                metadataScheduler,
                useUniqueTableLocation,
                allowManagedTableRename);
    }

    @Override
    public LocatedTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        if (endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with end version is not supported");
        }

        requireNonNull(tableName, "tableName is null");
        if (!DeltaLakeTableName.isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }
        Optional<DeltaMetastoreTable> table = getMetastore()
                .getTable(tableName.getSchemaName(), tableName.getTableName());

        if (table.isEmpty()) {
            return null;
        }
        boolean managed = table.get().managed();
        String tableLocation = table.get().location();

        Engine engine = createEngine(fileSystemFactory.create(session.getIdentity()), typeManager);

        Optional<Snapshot> snapshot = KernelClient.getSnapshot(engine, tableLocation);
        if (snapshot.isEmpty()) {
            return null;
        }
        return new KernelDeltaLakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                managed,
                tableLocation,
                Optional.empty(),
                KernelClient.getVersion(engine, snapshot.get()),
                snapshot);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(table);
        // This method does not calculate column metadata for the projected columns
        checkArgument(tableHandle.getProjectedColumns().isEmpty(), "Unexpected projected columns");

        Snapshot snapshot = tableHandle.getDeltaSnapshot().orElseThrow(
                // TODO: this shouldn't happen, but it's better to handle it gracefully
                () -> new RuntimeException("table not found"));

        List<ColumnMetadata> columns = KernelClient.getTableColumnMetadata(
                createEngine(fileSystemFactory.create(session.getIdentity()), typeManager),
                typeManager,
                tableHandle.getSchemaTableName(),
                snapshot);

        // DeltaLakeTable deltaTable = DeltaLakeTable.builder(snapshot.getSchema(tableClient)).build();
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(LOCATION_PROPERTY, tableHandle.getLocation());
        // List<String> partitionColumnNames = metadataEntry.getLowercasePartitionColumns();
        // if (!partitionColumnNames.isEmpty()) {
        //     properties.put(PARTITIONED_BY_PROPERTY, partitionColumnNames);
        // }

        // checkpointInterval.ifPresent(value -> properties.put(CHECKPOINT_INTERVAL_PROPERTY, value));
        // changeDataFeedEnabled(metadataEntry, protocolEntry)
        //        .ifPresent(value -> properties.put(CHANGE_DATA_FEED_ENABLED_PROPERTY, value));

        // DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry, protocolEntry);
        // if (columnMappingMode != NONE) {
        //     properties.put(COLUMN_MAPPING_MODE_PROPERTY, columnMappingMode.name());
        //}

        return new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(),
                columns,
                properties.buildOrThrow(),
                Optional.of("Delta table description" /* TODO: fetch from Delta table */),
                Collections.emptyList() /* constraints */);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(table);
        Snapshot snapshot = tableHandle.getDeltaSnapshot().orElseThrow(
                // TODO: this shouldn't happen, but it's better to handle it gracefully
                () -> new RuntimeException("table not found"));
        Engine engine = createEngine(fileSystemFactory.create(session.getIdentity()), typeManager);

        return getColumns(tableHandle.getSchemaTableName(), KernelClient.getSchema(engine, snapshot)).stream()
                .collect(Collectors.toMap(DeltaLakeColumnHandle::columnName, column -> column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(table);
        Snapshot snapshot = tableHandle.getDeltaSnapshot().orElseThrow(
                // TODO: this shouldn't happen, but it's better to handle it gracefully
                () -> new RuntimeException("table not found"));

        Engine engine = createEngine(fileSystemFactory.create(session.getIdentity()), typeManager);
        DeltaLakeTable deltaTable = DeltaLakeTable.builder(KernelClient.getSchema(engine, snapshot)).build();
        return getColumnMetadata(deltaTable, (DeltaLakeColumnHandle) columnHandle);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.empty(); // TODO: fill this out. Not needed now.
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(table);
        return new ConnectorTableProperties(
                TupleDomain.all(), /* fill out enforced predicate when pruning is supported */
                Optional.empty(), /* table partitioning */
                Optional.empty(), /* discrete predicates */
                Collections.emptyList() /* local properties */);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return checkValidKernelTableHandle(table).getSchemaTableName();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(handle);
        // TODO: validate the scan has partition filter
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        KernelDeltaLakeTableHandle tableHandle = checkValidKernelTableHandle(table);
        return Optional.of(new DeltaLakeInputInfo(false /* TODO: partitioned */, tableHandle.getReadVersion()));
    }

    private KernelDeltaLakeTableHandle checkValidKernelTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (!(tableHandle instanceof KernelDeltaLakeTableHandle)) {
            throw new IllegalArgumentException("tableHandle is not an instance of KernelDeltaLakeTableHandle");
        }
        return ((KernelDeltaLakeTableHandle) tableHandle);
    }

    private List<DeltaLakeColumnHandle> getColumns(SchemaTableName tableName, StructType schema)
    {
        ImmutableList.Builder<DeltaLakeColumnHandle> columns = ImmutableList.builder();

        schema.fields().forEach(field -> {
            Type type = toTrinoType(tableName, typeManager, field.getDataType());
            columns.add(toColumnHandle(
                    field.getName(),
                    type,
                    OptionalInt.empty(), /* fill out the field id */
                    field.getName(), /* fill out the physical name */
                    type,
                    Collections.emptyList()));
        });

        columns.add(pathColumnHandle());
        columns.add(fileSizeColumnHandle());
        columns.add(fileModifiedTimeColumnHandle());
        return columns.build();
    }
}
