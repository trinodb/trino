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
package io.trino.plugin.iceberg.catalog;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergMaterializedViewDefinition;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expressions;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.fromConnectorMaterializedViewDefinition;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.getStorageSchema;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameWithType;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getSortOrder;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.createTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshot;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshotAfter;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH_WITHIN_GRACE_PERIOD;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.Transactions.createOrReplaceTableTransaction;
import static org.apache.iceberg.Transactions.createTableTransaction;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    public static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    public static final String ICEBERG_VIEW_RUN_AS_OWNER = "trino.run-as-owner";

    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String TRINO_QUERY_ID_NAME = HiveMetadata.TRINO_QUERY_ID_NAME;

    public static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";
    public static final String DEPENDS_ON_TABLES = "dependsOnTables";
    public static final String DEPENDS_ON_TABLE_FUNCTIONS = "dependsOnTableFunctions";
    public static final String DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS = "dependsOnNonDeterministicFunctions";
    // Value should be ISO-8601 formatted time instant
    public static final String TRINO_QUERY_START_TIME = "trino-query-start-time";

    private final CatalogName catalogName;
    private final boolean useUniqueTableLocation;
    protected final TypeManager typeManager;
    protected final IcebergTableOperationsProvider tableOperationsProvider;
    protected final TrinoFileSystemFactory fileSystemFactory;
    protected final ForwardingFileIoFactory fileIoFactory;

    protected AbstractTrinoCatalog(
            CatalogName catalogName,
            boolean useUniqueTableLocation,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        if (comment.isEmpty()) {
            icebergTable.updateProperties().remove(TABLE_COMMENT).commit();
        }
        else {
            icebergTable.updateProperties().set(TABLE_COMMENT, comment.get()).commit();
        }
        invalidateTableCache(schemaTableName);
    }

    @Override
    public void updateColumnComment(ConnectorSession session, SchemaTableName schemaTableName, ColumnIdentity columnIdentity, Optional<String> comment)
    {
        Table icebergTable = loadTable(session, schemaTableName);
        icebergTable.updateSchema().updateColumnDoc(columnIdentity.getName(), comment.orElse(null)).commit();
        invalidateTableCache(schemaTableName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (TableInfo tableInfo : listTables(session, namespace)) {
            if (tableInfo.extendedRelationType() != TableInfo.ExtendedRelationType.TRINO_VIEW) {
                continue;
            }
            SchemaTableName name = tableInfo.tableName();
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode()) || e instanceof TableNotFoundException || e instanceof ViewNotFoundException) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return doGetMaterializedView(session, schemaViewName);
    }

    protected abstract Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName);

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        SchemaTableName storageTableName = definition.getStorageTable()
                .orElseThrow(() -> new TrinoException(ICEBERG_INVALID_METADATA, "Materialized view definition is missing a storage table"))
                .getSchemaTableName();

        try {
            BaseTable storageTable = loadTable(session, definition.getStorageTable().orElseThrow().getSchemaTableName());
            return ImmutableMap.<String, Object>builder()
                    .putAll(getIcebergTableProperties(storageTable))
                    .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                    .buildOrThrow();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to load storage table metadata for materialized view: " + viewName);
        }
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName, boolean considerGracePeriod)
    {
        return getMaterializedViewFreshnessUsingDependsOnTables(session, materializedViewName, considerGracePeriod, directExecutor());
    }

    /**
     * Computes materialized view freshness using the {@code dependsOnTables} snapshot-summary
     * bookkeeping this class writes on refresh. Callers that have a dedicated metadata-fetching
     * executor (for parallelizing the per-source-table resolution below) should pass it in;
     * {@link #getMaterializedViewFreshness} itself uses a direct executor.
     */
    protected final MaterializedViewFreshness getMaterializedViewFreshnessUsingDependsOnTables(
            ConnectorSession session,
            SchemaTableName materializedViewName,
            boolean considerGracePeriod,
            Executor metadataFetchingExecutor)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, materializedViewName);
        if (materializedViewDefinition.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + materializedViewName));

        Table icebergTable = loadTable(session, storageTableName);
        Optional<Snapshot> currentSnapshot = Optional.ofNullable(icebergTable.currentSnapshot());
        String dependsOnTables = currentSnapshot
                .map(snapshot -> snapshot.summary().getOrDefault(DEPENDS_ON_TABLES, ""))
                .orElse("");
        boolean dependsOnTableFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_TABLE_FUNCTIONS, "false")))
                .orElse(false);
        // For MVs refreshed before non-deterministic function tracking was added this flag
        // defaults to false. Such MVs will be correctly flagged after their next refresh.
        boolean dependsOnNonDeterministicFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS, "false")))
                .orElse(false);

        Optional<Instant> refreshStartTime = currentSnapshot.map(snapshot -> snapshot.summary().get(TRINO_QUERY_START_TIME))
                .map(Instant::parse);
        Optional<Instant> refreshTime = refreshStartTime
                // Fallback to snapshot commit time (end of refresh) for MVs defined before TRINO_QUERY_START_TIME was introduced
                .or(() -> currentSnapshot.map(snapshot -> Instant.ofEpochMilli(snapshot.timestampMillis())));

        if (dependsOnTableFunctions) {
            // It can't be determined whether a value returned by table function is STALE or not
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnNonDeterministicFunctions) {
            // Non-deterministic functions like current_timestamp produce different values over time,
            // so the materialized view may be stale even if base tables haven't changed
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnTables.isEmpty()) {
            // Information missing. While it's "unknown" whether storage is stale, we return "stale".
            // Normally dependsOnTables may be missing only when there was no refresh yet.
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        Optional<Duration> gracePeriod = materializedViewDefinition.get().getGracePeriod();
        if (considerGracePeriod && withinGracePeriod(session.getStart(), refreshStartTime, gracePeriod)) {
            // To determine freshness, we normally load current metadata for each base table and check if there
            // is a newer snapshot than the recorded one (DEPENDS_ON_TABLES). This requires expensive metastore
            // operations for each base Iceberg table.
            //
            // The refresh query can read base table snapshots created before or during its execution. In the most
            // pessimistic scenario, a new base table snapshot is created immediately after the refresh started
            // (at refreshStartTime + epsilon), but the refresh reads an older snapshot. This new snapshot would
            // not be recorded in DEPENDS_ON_TABLES, making the MV technically stale. However, when the caller set
            // considerGracePeriod to true and refreshStartTime + gracePeriod > referenceTime, we can safely say that
            // the MV is at least within the grace period because refreshStartTime is before the new snapshot creation time.
            return new MaterializedViewFreshness(FRESH_WITHIN_GRACE_PERIOD, Optional.empty());
        }

        boolean hasUnknownTables = false;
        OptionalLong firstTableChange = OptionalLong.of(Long.MAX_VALUE);
        ImmutableList.Builder<Callable<TableChangeInfo>> tableChangeInfoTasks = ImmutableList.builder();
        for (String tableToSnapShot : Splitter.on(',').split(dependsOnTables)) {
            if (tableToSnapShot.equals(UNKNOWN_SNAPSHOT_TOKEN)) {
                hasUnknownTables = true;
                firstTableChange = OptionalLong.empty();
                continue;
            }

            tableChangeInfoTasks.add(() -> getTableChangeInfo(session, tableToSnapShot));
        }

        boolean hasStaleIcebergTables = false;
        List<TableChangeInfo> tableChangeInfos;

        try {
            tableChangeInfos = processWithAdditionalThreads(tableChangeInfoTasks.build(), metadataFetchingExecutor);
        }
        catch (ExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }

        verifyNotNull(tableChangeInfos);

        for (TableChangeInfo tableChangeInfo : tableChangeInfos) {
            switch (tableChangeInfo) {
                case NoTableChange() -> {
                    // Fresh
                }
                case FirstChangeSnapshot(Snapshot snapshot) -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = firstTableChange.isPresent() ?
                            OptionalLong.of(Math.min(firstTableChange.orElseThrow(), snapshot.timestampMillis())) :
                            OptionalLong.empty();
                }
                case UnknownTableChange(), GoneOrCorruptedTableChange() -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = OptionalLong.empty();
                }
            }
        }

        Optional<Instant> lastKnownFreshTime = firstTableChange.isPresent() ? Optional.of(Instant.ofEpochMilli(firstTableChange.orElseThrow())) : refreshTime;
        if (hasStaleIcebergTables) {
            return new MaterializedViewFreshness(STALE, lastKnownFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastKnownFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private static boolean withinGracePeriod(Instant sessionStart, Optional<Instant> refreshStartTime, Optional<Duration> gracePeriod)
    {
        if (gracePeriod.isEmpty()) {
            // infinite grace period
            return true;
        }
        //noinspection OptionalIsPresent
        if (refreshStartTime.isEmpty()) {
            // refresh time unknown
            return false;
        }
        return refreshStartTime.get().plus(gracePeriod.get()).isAfter(sessionStart);
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, String entry)
    {
        List<String> keyValue = Splitter.on("=").splitToList(entry);
        if (keyValue.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid entry in '%s' property: %s'", DEPENDS_ON_TABLES, entry));
        }
        String tableName = keyValue.get(0);
        String value = keyValue.get(1);
        List<String> strings = Splitter.on(".").splitToList(tableName);
        if (strings.size() == 3) {
            strings = strings.subList(1, 3);
        }
        else if (strings.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
        }
        String schema = strings.get(0);
        String name = strings.get(1);
        SchemaTableName schemaTableName = new SchemaTableName(schema, name);

        Table icebergSourceTable;
        try {
            icebergSourceTable = loadTable(session, schemaTableName);
        }
        catch (RuntimeException e) {
            // Base table is gone, or table is corrupted, or can't otherwise be resolved: treat conservatively as changed
            return new GoneOrCorruptedTableChange();
        }

        OptionalLong snapshotAtRefresh;
        if (value.isEmpty()) {
            snapshotAtRefresh = OptionalLong.empty();
        }
        else {
            snapshotAtRefresh = OptionalLong.of(Long.parseLong(value));
        }
        return getTableChangeInfo(icebergSourceTable, snapshotAtRefresh);
    }

    private static TableChangeInfo getTableChangeInfo(Table icebergTable, OptionalLong snapshotAtRefresh)
    {
        Snapshot currentSnapshot = icebergTable.currentSnapshot();

        if (snapshotAtRefresh.isEmpty()) {
            // Table had no snapshot at refresh time.
            if (currentSnapshot == null) {
                return new NoTableChange();
            }
            return firstSnapshot(icebergTable)
                    .<TableChangeInfo>map(FirstChangeSnapshot::new)
                    .orElse(new UnknownTableChange());
        }

        if (snapshotAtRefresh.orElseThrow() == currentSnapshot.snapshotId()) {
            // A schema-only change (rename, added column, etc.) doesn't bump the snapshot id.
            Integer recordedSchemaId = currentSnapshot.schemaId();
            if (recordedSchemaId != null && !recordedSchemaId.equals(icebergTable.schema().schemaId())) {
                return new UnknownTableChange();
            }
            return new NoTableChange();
        }
        return firstSnapshotAfter(icebergTable, snapshotAtRefresh.orElseThrow())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    private sealed interface TableChangeInfo
            permits FirstChangeSnapshot,
                    GoneOrCorruptedTableChange,
                    NoTableChange,
                    UnknownTableChange {}

    private record NoTableChange()
            implements TableChangeInfo {}

    private record FirstChangeSnapshot(Snapshot snapshot)
            implements TableChangeInfo
    {
        FirstChangeSnapshot
        {
            requireNonNull(snapshot, "snapshot is null");
        }
    }

    private record UnknownTableChange()
            implements TableChangeInfo {}

    private record GoneOrCorruptedTableChange()
            implements TableChangeInfo {}

    protected Transaction newCreateTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            Optional<String> location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, location.orElse(null), properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                location);
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    protected Transaction newCreateOrReplaceTableTransaction(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Schema schema,
            PartitionSpec partitionSpec,
            SortOrder sortOrder,
            String location,
            Map<String, String> properties,
            Optional<String> owner)
    {
        BaseTable table;
        Optional<TableMetadata> metadata = Optional.empty();
        try {
            table = loadTable(session, new SchemaTableName(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            metadata = Optional.of(table.operations().current());
        }
        catch (TableNotFoundException _) {
            // ignored
        }
        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
                this,
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                owner,
                Optional.of(location));
        TableMetadata newMetaData;
        if (metadata.isPresent()) {
            operations.initializeFromMetadata(metadata.get());
            newMetaData = operations.current()
                    // don't inherit table properties from earlier snapshots
                    .replaceProperties(properties)
                    .buildReplacement(schema, partitionSpec, sortOrder, location, properties);
        }
        else {
            newMetaData = newTableMetadata(schema, partitionSpec, sortOrder, location, properties);
        }
        return createOrReplaceTableTransaction(schemaTableName.toString(), operations, newMetaData);
    }

    protected String createNewTableName(String baseTableName)
    {
        String tableNameLocationComponent = escapeTableName(baseTableName);
        if (useUniqueTableLocation) {
            tableNameLocationComponent += "-" + randomUUID().toString().replace("-", "");
        }
        return tableNameLocationComponent;
    }

    protected void deleteTableDirectory(TrinoFileSystem fileSystem, SchemaTableName schemaTableName, String tableLocation)
    {
        try {
            fileSystem.deleteDirectory(Location.of(tableLocation));
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", tableLocation, schemaTableName), e);
        }
    }

    protected Location createMaterializedViewStorage(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        if (getStorageSchema(materializedViewProperties).isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized view property '%s' is not supported when hiding materialized view storage tables is enabled".formatted(STORAGE_SCHEMA));
        }
        SchemaTableName storageTableName = new SchemaTableName(viewName.getSchemaName(), tableNameWithType(viewName.getTableName(), MATERIALIZED_VIEW_STORAGE));
        String tableLocation = getTableLocation(materializedViewProperties)
                .orElseGet(() -> defaultTableLocation(session, viewName));
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        Schema schema = schemaFromMetadata(columns);
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(materializedViewProperties));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(materializedViewProperties));
        Map<String, String> properties = createTableProperties(new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty()), _ -> false);

        TableMetadata metadata = newTableMetadata(schema, partitionSpec, sortOrder, tableLocation, properties);

        String fileName = format("%05d-%s%s", 0, randomUUID(), getFileExtension(METADATA_COMPRESSION_DEFAULT));
        Location metadataFileLocation = Location.of(tableLocation).appendPath(METADATA_FOLDER_NAME).appendPath(fileName);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TableMetadataParser.write(metadata, new ForwardingOutputFile(fileSystem, metadataFileLocation));

        return metadataFileLocation;
    }

    protected void dropMaterializedViewStorage(ConnectorSession session, TrinoFileSystem fileSystem, String storageMetadataLocation)
            throws IOException
    {
        TableMetadata metadata = TableMetadataParser.read(fileIoFactory.create(fileSystem, isUseFileSizeFromMetadata(session)), storageMetadataLocation);
        String storageLocation = metadata.location();
        fileSystem.deleteDirectory(Location.of(storageLocation));
    }

    protected SchemaTableName createMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        // Generate a storage table name and create a storage table. The properties in the definition are table properties for the
        // storage table as indicated in the materialized view definition.
        String storageTableName = "st_" + randomUUID().toString().replace("-", "");

        String storageSchema = getStorageSchema(materializedViewProperties).orElse(viewName.getSchemaName());
        SchemaTableName storageTable = new SchemaTableName(storageSchema, storageTableName);
        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(storageTable, columns, materializedViewProperties, Optional.empty());
        String tableLocation = getTableLocation(tableMetadata.getProperties())
                .orElseGet(() -> defaultTableLocation(session, tableMetadata.getTable()));
        Transaction transaction = IcebergUtil.newCreateTableTransaction(this, tableMetadata, session, false, tableLocation, _ -> false, ImmutableList.of());
        AppendFiles appendFiles = transaction.newAppend();
        commit(appendFiles, session);
        transaction.commitTransaction();
        return storageTable;
    }

    protected List<ColumnMetadata> columnsForMaterializedView(ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties)
    {
        return MaterializedViewStorageColumns.columnsForMaterializedView(typeManager, definition, materializedViewProperties);
    }

    protected ConnectorMaterializedViewDefinition getMaterializedViewDefinition(
            Optional<String> owner,
            String viewOriginalText,
            SchemaTableName storageTableName)
    {
        IcebergMaterializedViewDefinition definition = decodeMaterializedViewData(viewOriginalText);
        return new ConnectorMaterializedViewDefinition(
                definition.originalSql(),
                Optional.of(new CatalogSchemaTableName(catalogName.toString(), storageTableName)),
                definition.catalog(),
                definition.schema(),
                toSpiMaterializedViewColumns(definition.columns()),
                definition.gracePeriod(),
                definition.whenStaleBehavior(),
                definition.comment(),
                owner,
                definition.path());
    }

    protected List<ConnectorMaterializedViewDefinition.Column> toSpiMaterializedViewColumns(List<IcebergMaterializedViewDefinition.Column> columns)
    {
        return columns.stream()
                .map(column -> new ConnectorMaterializedViewDefinition.Column(column.name(), column.type(), column.comment()))
                .collect(toImmutableList());
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, SchemaTableName storageTableName)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(STORAGE_SCHEMA, storageTableName.getSchemaName())
                .put(STORAGE_TABLE, storageTableName.getTableName())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected Map<String, String> createMaterializedViewProperties(ConnectorSession session, Location storageMetadataLocation)
    {
        return ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                .put(METADATA_LOCATION_PROP, storageMetadataLocation.toString())
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE)
                .put(TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .buildOrThrow();
    }

    protected void replaceMaterializedViewStorageTable(
            ConnectorSession session,
            SchemaTableName storageTableName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            ExecutorService icebergScanExecutor)
    {
        BaseTable icebergTable = loadTable(session, storageTableName);
        AbstractIcebergTableOperations tableOperations = (AbstractIcebergTableOperations) icebergTable.operations();
        if (isMaterializedViewStorage(storageTableName.getTableName())) {
            // Update the view definition while performing the commit operation on the iceberg table
            Map<String, String> viewProperties = createMaterializedViewProperties(session, Location.of(tableOperations.current().metadataFileLocation()));
            tableOperations.applyMaterializedViewCommitData(Optional.of(new MaterializedViewCommitData(
                    encodeMaterializedViewData(fromConnectorMaterializedViewDefinition(definition)),
                    viewProperties)));
        }

        Optional<String> providedTableLocation = getTableLocation(materializedViewProperties);
        if (providedTableLocation.isPresent() && !stripTrailingSlash(providedTableLocation.get()).equals(icebergTable.location())) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("The provided location '%s' does not match the existing storage table location '%s'", providedTableLocation.get(), icebergTable.location()));
        }

        List<ColumnMetadata> columns = columnsForMaterializedView(definition, materializedViewProperties);
        Schema schema = IcebergUtil.schemaFromMetadata(columns);
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(materializedViewProperties));
        SortOrder sortOrder = parseSortFields(schema, getSortOrder(materializedViewProperties));
        Map<String, String> properties = createTableProperties(new ConnectorTableMetadata(storageTableName, columns, materializedViewProperties, Optional.empty()), _ -> false);
        TableMetadata newTableMetadata = icebergTable.operations().current()
                // don't inherit table properties from earlier snapshots
                .replaceProperties(properties)
                .buildReplacement(schema, partitionSpec, sortOrder, icebergTable.location(), properties);
        Transaction transaction = createOrReplaceTableTransaction(storageTableName.getTableName(), icebergTable.operations(), newTableMetadata);
        transaction.newDelete()
                .deleteFromRowFilter(Expressions.alwaysTrue())
                .scanManifestsWith(icebergScanExecutor)
                .commit();
        try {
            transaction.commitTransaction();
        }
        finally {
            if (isMaterializedViewStorage(storageTableName.getTableName())) {
                tableOperations.applyMaterializedViewCommitData(Optional.empty());
            }
        }
    }

    protected abstract void invalidateTableCache(SchemaTableName schemaTableName);
}
