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
import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.plugin.iceberg.fileio.ForwardingOutputFile;
import io.trino.spi.ErrorCode;
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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
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
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.hive.HiveMetadata.STORAGE_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.decodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.STORAGE_SCHEMA;
import static io.trino.plugin.iceberg.IcebergMaterializedViewProperties.getStorageSchema;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
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
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH_WITHIN_GRACE_PERIOD;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.Transactions.createOrReplaceTableTransaction;
import static org.apache.iceberg.Transactions.createTableTransaction;

public abstract class AbstractTrinoCatalog
        implements TrinoCatalog
{
    public static final String TRINO_CREATED_BY_VALUE = "Trino Iceberg connector";
    public static final String ICEBERG_VIEW_RUN_AS_OWNER = "trino.run-as-owner";

    protected static final String TRINO_CREATED_BY = HiveMetadata.TRINO_CREATED_BY;
    protected static final String TRINO_QUERY_ID_NAME = HiveMetadata.TRINO_QUERY_ID_NAME;

    private static final String DEPENDS_ON_TABLES = "dependsOnTables";
    private static final String DEPENDS_ON_TABLE_FUNCTIONS = "dependsOnTableFunctions";
    private static final String DEPENDS_ON_NON_DETERMINISTIC_FUNCTIONS = "dependsOnNonDeterministicFunctions";
    // Value should be ISO-8601 formatted time instant
    private static final String TRINO_QUERY_START_TIME = "trino-query-start-time";
    private static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";

    private final CatalogName catalogName;
    private final boolean useUniqueTableLocation;
    protected final TypeManager typeManager;
    protected final IcebergTableOperationsProvider tableOperationsProvider;
    protected final TrinoFileSystemFactory fileSystemFactory;
    protected final ForwardingFileIoFactory fileIoFactory;
    protected final Executor metadataFetchingExecutor;

    protected AbstractTrinoCatalog(
            CatalogName catalogName,
            boolean useUniqueTableLocation,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            Executor metadataFetchingExecutor)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.metadataFetchingExecutor = requireNonNull(metadataFetchingExecutor, "metadataFetchingExecutor is null");
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
                            OptionalLong.of(Math.min(firstTableChange.getAsLong(), snapshot.timestampMillis())) :
                            OptionalLong.empty();
                }
                case UnknownTableChange(), GoneOrCorruptedTableChange() -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = OptionalLong.empty();
                }
            }
        }

        Optional<Instant> lastKnownFreshTime = firstTableChange.isPresent() ? Optional.of(Instant.ofEpochMilli(firstTableChange.getAsLong())) : refreshTime;
        if (hasStaleIcebergTables) {
            return new MaterializedViewFreshness(STALE, lastKnownFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastKnownFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private boolean withinGracePeriod(Instant sessionStart, Optional<Instant> refreshStartTime, Optional<Duration> gracePeriod)
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

        BaseTable table;
        try {
            table = loadTable(session, schemaTableName);
        }
        catch (TableNotFoundException e) {
            return new GoneOrCorruptedTableChange();
        }
        catch (TrinoException e) {
            ErrorCode errorCode = e.getErrorCode();
            if (errorCode.equals(ICEBERG_MISSING_METADATA.toErrorCode())
                    || errorCode.equals(ICEBERG_INVALID_METADATA.toErrorCode())) {
                return new GoneOrCorruptedTableChange();
            }
            throw e;
        }
        OptionalLong snapshotAtRefresh;
        if (value.isEmpty()) {
            snapshotAtRefresh = OptionalLong.empty();
        }
        else {
            snapshotAtRefresh = OptionalLong.of(Long.parseLong(value));
        }
        return getTableChangeInfo(table, snapshotAtRefresh);
    }

    private TableChangeInfo getTableChangeInfo(BaseTable icebergTable, OptionalLong snapshotAtRefresh)
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

        if (snapshotAtRefresh.getAsLong() == currentSnapshot.snapshotId()) {
            return new NoTableChange();
        }
        return firstSnapshotAfter(icebergTable, snapshotAtRefresh.getAsLong())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    private sealed interface TableChangeInfo
            permits NoTableChange, FirstChangeSnapshot, UnknownTableChange, GoneOrCorruptedTableChange {}

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

    private List<ColumnMetadata> columnsForMaterializedView(ConnectorMaterializedViewDefinition definition, Map<String, Object> materializedViewProperties)
    {
        Schema schemaWithTimestampTzPreserved = schemaFromMetadata(definition.getColumns().stream()
                .map(column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6) {
                        // For now preserve timestamptz columns so that we can parse partitioning
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                })
                .collect(toImmutableList()));
        PartitionSpec partitionSpec = parsePartitionFields(schemaWithTimestampTzPreserved, getPartitioning(materializedViewProperties));
        Set<String> temporalPartitioningSources = partitionSpec.fields().stream()
                .flatMap(partitionField -> {
                    Types.NestedField sourceField = schemaWithTimestampTzPreserved.findField(partitionField.sourceId());
                    Type sourceType = toTrinoType(sourceField.type(), typeManager);
                    ColumnTransform columnTransform = getColumnTransform(partitionField, sourceType);
                    if (!columnTransform.temporal()) {
                        return Stream.of();
                    }
                    return Stream.of(sourceField.name());
                })
                .collect(toImmutableSet());

        return definition.getColumns().stream()
                .map(column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6 && temporalPartitioningSources.contains(column.getName())) {
                        // Apply point-in-time semantics to maintain partitioning capabilities
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                })
                .collect(toImmutableList());
    }

    /**
     * Substitutes type not supported by Iceberg with a type that is supported.
     * Upon reading from a materialized view, the types will be coerced back to the original ones,
     * stored in the materialized view definition.
     */
    private Type typeForMaterializedViewStorageTable(Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type == NUMBER) {
            return VARCHAR;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof TimeType timeType) {
            // Iceberg supports microsecond precision only
            return timeType.getPrecision() <= 6
                    ? TIME_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return VARCHAR;
        }
        if (type instanceof TimestampType timestampType) {
            // Iceberg supports microsecond precision only
            return timestampType.getPrecision() <= 6
                    ? TIMESTAMP_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            // Iceberg does not store the time zone.
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(typeForMaterializedViewStorageTable(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(
                    typeForMaterializedViewStorageTable(mapType.getKeyType()),
                    typeForMaterializedViewStorageTable(mapType.getValueType()),
                    typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.rowType(
                    rowType.getFields().stream()
                            .map(field -> new RowType.Field(field.getName(), typeForMaterializedViewStorageTable(field.getType())))
                            .toArray(RowType.Field[]::new));
        }

        // Pass through all the types not explicitly handled above. If a type is not accepted by the connector,
        // creation of the storage table will fail anyway.
        return type;
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

    protected abstract void invalidateTableCache(SchemaTableName schemaTableName);
}
