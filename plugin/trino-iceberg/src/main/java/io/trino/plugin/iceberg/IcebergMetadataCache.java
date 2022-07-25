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

import com.google.common.base.Suppliers;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.glue.GlueIcebergTableOperations;
import io.trino.plugin.iceberg.catalog.glue.TrinoGlueCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isAllowLegacySnapshotSyntax;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;

public class IcebergMetadataCache
{
    public enum StatsRecording
    {
        ENABLED,
        DISABLED
    }

    private static IcebergMetadataCache icebergMetadataCache;

    private IcebergMetadataFactory metadataFactory;
    private TypeManager typeManager;
    private final CatalogType catalogType;
    private static final String schemaCacheKey = "SCHEMA_CACHE";
    private final LoadingCache<String, List<String>> schemaNamesCache;
    private final LoadingCache<CatalogSchemaName, Map<String, Object>> schemaPropertiesCache;
    private final LoadingCache<CatalogSchemaName, Optional<TrinoPrincipal>> schemaOwnerCache;
    private final LoadingCache<SchemaTableName, Table> tableCache;
    private final LoadingCache<SchemaTableName, Optional<SystemTable>> systemTableCache;
    private final LoadingCache<IcebergMetadataCache.SchemaTableWrappedCacheKey<ConnectorTableHandle>, ConnectorTableProperties> tablePropertiesCache;
    private final LoadingCache<IcebergMetadataCache.SchemaTableWrappedCacheKey<ConnectorTableHandle>, ConnectorTableMetadata> tableMetadataCache;
    private final LoadingCache<Optional<String>, List<SchemaTableName>> tableListingCache;
    private final LoadingCache<IcebergMetadataCache.SchemaTableWrappedCacheKey<ConnectorTableHandle>, TableStatistics> tableStatsCache;
    private final LoadingCache<Optional<String>, Map<SchemaTableName, ConnectorViewDefinition>> viewListingCache;
    private final LoadingCache<SchemaTableName, Optional<ConnectorViewDefinition>> viewCache;

    public IcebergMetadataCache(IcebergMetadataFactory metadataFactory, TypeManager typeManager, CatalogType catalogType, long globalMetadataCacheTtl, int maxCacheSize, long globalMetadataCacheTtlForListing)
    {
        this.metadataFactory = metadataFactory;
        this.typeManager = typeManager;
        this.catalogType = catalogType;
        schemaNamesCache = buildCache(OptionalLong.of(globalMetadataCacheTtlForListing), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::listCachedSchemaNames);
        schemaPropertiesCache = buildCache(OptionalLong.of(globalMetadataCacheTtlForListing), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedSchemaProperties);
        schemaOwnerCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedSchemaOwner);
        tableCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedTableHandle);
        systemTableCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedSystemTable);
        tablePropertiesCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedTableProperties);
        tableStatsCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedTableStats);
        tableMetadataCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedTableMetadata);
        tableListingCache = buildCache(OptionalLong.of(globalMetadataCacheTtlForListing), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedListTables);
        viewListingCache = buildCache(OptionalLong.of(globalMetadataCacheTtlForListing), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedListViews);
        viewCache = buildCache(OptionalLong.of(globalMetadataCacheTtl), maxCacheSize, CachingHiveMetastore.StatsRecording.ENABLED, this::getCachedView);
    }

    public static IcebergMetadataCache getMetadataCache(IcebergMetadataFactory metadataFactory, TypeManager typeManager,
            CatalogType catalogType, long globalMetadataCacheTtl, int maxCacheSize, long globalMetadataCacheTtlForListing)
    {
        if (icebergMetadataCache == null) {
            synchronized (IcebergMetadataCache.class) {
                if (icebergMetadataCache == null) {
                    icebergMetadataCache = new IcebergMetadataCache(metadataFactory, typeManager, catalogType, globalMetadataCacheTtl, maxCacheSize, globalMetadataCacheTtlForListing);
                }
            }
        }
        return icebergMetadataCache;
    }

    public List<String> listSchemaNames(ConnectorSession session)
    {
        return get(schemaNamesCache, schemaCacheKey);
    }

    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return get(schemaPropertiesCache, schemaName);
    }

    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return get(schemaOwnerCache, schemaName);
    }

    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() != DATA) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable cachedTable = null;
        try {
            cachedTable = (BaseTable) get(tableCache, tableName);
        }
        catch (TableNotFoundException e) {
            return null;
        }

        BaseTable currentTable = cachedTable;
        if (catalogType.equals(CatalogType.GLUE)) {
            TrinoGlueCatalog catalog = (TrinoGlueCatalog) metadataFactory.getCatalogFactory().create(null);
            String currentMetadataLocation = catalog.getRefreshedLocation(null, tableName);
            if (!((GlueIcebergTableOperations) cachedTable.operations()).getCurrentMetadataLocation().equals(currentMetadataLocation)) {
                // table metadata location has updated so flush all cache for this schema table name and update table metadata
                flushCaches(tableName);
                currentTable = (BaseTable) get(tableCache, tableName);
            }
        }

        if (name.getSnapshotId().isPresent() && endVersion.isPresent()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Cannot specify end version both in table name and FOR clause");
        }

        BaseTable table = currentTable;
        Optional<Long> snapshotId = endVersion.map(version -> getSnapshotIdFromVersion(table, version))
                .or(() -> getSnapshotId(table, name.getSnapshotId(), isAllowLegacySnapshotSyntax(session)));

        String nameMappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        return new IcebergTableHandle(
                tableName.getSchemaName(),
                name.getTableName(),
                name.getTableType(),
                snapshotId,
                SchemaParser.toJson(table.schema()),
                PartitionSpecParser.toJson(table.spec()),
                table.operations().current().formatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                Optional.ofNullable(nameMappingJson),
                table.location(),
                table.properties(),
                NO_RETRIES,
                ImmutableList.of(),
                false,
                Optional.empty());
    }

    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return get(tableCache, schemaTableName);
    }

    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return get(systemTableCache, tableName);
    }

    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        SchemaTableWrappedCacheKey<ConnectorTableHandle> key = new SchemaTableWrappedCacheKey<>(icebergTableHandle.getSchemaTableName(), tableHandle);
        return get(tablePropertiesCache, key);
    }

    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        SchemaTableWrappedCacheKey<ConnectorTableHandle> key = new SchemaTableWrappedCacheKey<>(icebergTableHandle.getSchemaTableName(), tableHandle);
        return get(tableStatsCache, key);
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        SchemaTableWrappedCacheKey<ConnectorTableHandle> key = new SchemaTableWrappedCacheKey<>(icebergTableHandle.getSchemaTableName(), tableHandle);
        return get(tableMetadataCache, key);
    }

    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return get(tableListingCache, schemaName);
    }

    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = get(tableCache, table.getSchemaTableName());
        return getColumns(icebergTable.schema(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return get(viewListingCache, schemaName);
    }

    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return get(viewCache, viewName);
    }

    private List<String> listCachedSchemaNames(String key)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.listSchemaNames(null);
    }

    private Map<String, Object> getCachedSchemaProperties(CatalogSchemaName catalogSchemaName)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.getSchemaProperties(null, catalogSchemaName);
    }

    private Optional<TrinoPrincipal> getCachedSchemaOwner(CatalogSchemaName catalogSchemaName)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.getSchemaOwner(null, catalogSchemaName);
    }

    private Table getCachedTableHandle(SchemaTableName schemaTableName)
    {
        TrinoCatalog catalog = metadataFactory.getCatalogFactory().create(null);
        if (catalogType.equals(CatalogType.GLUE)) {
            return ((TrinoGlueCatalog) catalog).loadTableWithoutCache(null, schemaTableName);
        }
        else {
            return catalog.loadTable(null, schemaTableName);
        }
    }

    private Optional<SystemTable> getCachedSystemTable(SchemaTableName schemaTableName)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.getSystemTable(null, schemaTableName);
    }

    private ConnectorTableProperties getCachedTableProperties(SchemaTableWrappedCacheKey<ConnectorTableHandle> key)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        IcebergTableHandle table = (IcebergTableHandle) key.getLoadingKey();

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = get(tableCache, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = icebergMetadata.identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getColumns(icebergTable.schema(), typeManager).stream()
                    .filter(column -> partitionSourceIds.contains(column.getId()))
                    .collect(toImmutableMap(IcebergColumnHandle::getId, Function.identity()));

            Supplier<List<FileScanTask>> lazyFiles = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toIcebergExpression(enforcedPredicate))
                        .includeColumnStats();

                try (CloseableIterable<FileScanTask> iterator = tableScan.planFiles()) {
                    return ImmutableList.copyOf(iterator);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<FileScanTask> files = () -> lazyFiles.get().iterator();

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(files, fileScan -> {
                // Extract partition values in the data file
                Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(fileScan);
                Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                        .filter(partitionColumnValueStrings::containsKey)
                        .collect(toImmutableMap(
                                columns::get,
                                columnId -> {
                                    IcebergColumnHandle column = columns.get(columnId);
                                    Object prestoValue = deserializePartitionValue(
                                            column.getType(),
                                            partitionColumnValueStrings.get(columnId).orElse(null),
                                            column.getName());

                                    return NullableValue.of(column.getType(), prestoValue);
                                }));

                return TupleDomain.fromFixedValues(partitionValues);
            });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                // TODO: implement table partitioning
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(discretePredicates),
                ImmutableList.of());
    }

    private TableStatistics getCachedTableStats(SchemaTableWrappedCacheKey<ConnectorTableHandle> key)
    {
        IcebergTableHandle handle = (IcebergTableHandle) key.getLoadingKey();
        Table icebergTable = get(tableCache, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, handle, icebergTable);
    }

    private ConnectorTableMetadata getCachedTableMetadata(SchemaTableWrappedCacheKey<ConnectorTableHandle> key)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        IcebergTableHandle tableHandle = (IcebergTableHandle) key.getLoadingKey();
        Table icebergTable = get(tableCache, tableHandle.getSchemaTableName());

        List<ColumnMetadata> columns = icebergMetadata.getColumnMetadatas(icebergTable);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));
        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        if (!icebergTable.location().isEmpty()) {
            properties.put(LOCATION_PROPERTY, icebergTable.location());
        }

        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns, properties.buildOrThrow(), getTableComment(icebergTable));
    }

    private Optional<Long> getSnapshotId(Table table, Optional<Long> snapshotId, boolean allowLegacySnapshotSyntax)
    {
        // table.name() is an encoded version of SchemaTableName
        return snapshotId
                .map(id -> IcebergUtil.resolveSnapshotId(table, id, allowLegacySnapshotSyntax))
                .or(() -> Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId));
    }

    private List<SchemaTableName> getCachedListTables(Optional<String> schema)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.listTables(null, schema);
    }

    private Map<SchemaTableName, ConnectorViewDefinition> getCachedListViews(Optional<String> schema)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.getViews(null, schema);
    }

    private Optional<ConnectorViewDefinition> getCachedView(SchemaTableName tableName)
    {
        IcebergMetadata icebergMetadata = metadataFactory.create(null);
        return icebergMetadata.getView(null, tableName);
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys)
    {
        try {
            return cache.getAll(keys);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    private <K, V> LoadingCache<K, V> buildCache(
            OptionalLong expiresAfterWriteMillis,
            long maximumSize,
            CachingHiveMetastore.StatsRecording statsRecording,
            com.google.common.base.Function<K, V> loader)
    {
        CacheLoader<K, V> cacheLoader = CacheLoader.from(loader);
        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), MINUTES);
        }
        cacheBuilder.maximumSize(maximumSize);
        if (statsRecording == CachingHiveMetastore.StatsRecording.ENABLED) {
            cacheBuilder.recordStats();
        }

        return cacheBuilder.build(cacheLoader);
    }

    private void flushCaches(SchemaTableName schemaTableName)
    {
        SchemaTableWrappedCacheKey<ConnectorTableHandle> cacheKey = new SchemaTableWrappedCacheKey<>(schemaTableName, null);
        tableCache.invalidate(schemaTableName);
        systemTableCache.invalidate(schemaTableName);
        tableMetadataCache.invalidate(cacheKey);
        tablePropertiesCache.invalidate(cacheKey);
        tableStatsCache.invalidate(cacheKey);
        viewCache.invalidate(schemaTableName);
    }

    private static long getSnapshotIdFromVersion(Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        switch (version.getPointerType()) {
            case TEMPORAL:
                long epochMillis;
                if (versionType instanceof TimestampWithTimeZoneType) {
                    epochMillis = ((TimestampWithTimeZoneType) versionType).isShort()
                            ? unpackMillisUtc((long) version.getVersion())
                            : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
                }
                else {
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
                }
                return getSnapshotIdAsOfTime(table, epochMillis);

            case TARGET_ID:
                if (versionType != BIGINT) {
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
                }
                long snapshotId = (long) version.getVersion();
                if (table.snapshot(snapshotId) == null) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Iceberg snapshot ID does not exists: " + snapshotId);
                }
                return snapshotId;
        }
        throw new TrinoException(NOT_SUPPORTED, "Version pointer type is not supported: " + version.getPointerType());
    }

    /**
     * This class is used for wrapping cache loader key with schema table key
     * @param <T>
     */
    class SchemaTableWrappedCacheKey<T>
    {
        SchemaTableName schemaTableName;
        T loadingKey;

        public SchemaTableWrappedCacheKey(SchemaTableName schemaTableName, T loadingKey)
        {
            this.schemaTableName = schemaTableName;
            this.loadingKey = loadingKey;
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        public T getLoadingKey()
        {
            return loadingKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SchemaTableWrappedCacheKey<?> that = (SchemaTableWrappedCacheKey<?>) o;
            return schemaTableName.equals(that.schemaTableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaTableName);
        }
    }
}
