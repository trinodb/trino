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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.stats.HudiTableStatistics;
import io.trino.plugin.hudi.stats.TableStatisticsReader;
import io.trino.plugin.hudi.util.HudiTableTypeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.util.Lazy;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.util.HiveUtil.columnMetadataGetter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hudi.HudiSessionProperties.getColumnsToHide;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.hudi.HudiSessionProperties.isResolveColumnNameCasingEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isTableStatisticsEnabled;
import static io.trino.plugin.hudi.HudiTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static io.trino.plugin.hudi.HudiUtil.getLatestTableSchema;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INDEXING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

public class HudiMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HudiMetadata.class);
    private static final Map<TableStatisticsCacheKey, HudiTableStatistics> tableStatisticsCache = new ConcurrentHashMap<>();
    private static final Set<TableStatisticsCacheKey> refreshingKeysInProgress = ConcurrentHashMap.newKeySet();
    private final HiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final ExecutorService tableStatisticsExecutor;

    public HudiMetadata(
            HiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            ExecutorService tableStatisticsExecutor)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableStatisticsExecutor = requireNonNull(tableStatisticsExecutor, "tableStatisticsExecutor is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !isHiveSystemSchema(schemaName))
                .collect(toImmutableList());
    }

    @Override
    public HudiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        log.info("Creating new HudiTableHandle for %s", tableName);
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Optional<Table> tableOpt = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (tableOpt.isEmpty()) {
            return null;
        }

        Table table = tableOpt.get();
        if (!isHudiTable(table)) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Not a Hudi table: %s", tableName));
        }
        String basePath = table.getStorage().getLocation();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        String inputFormat = table.getStorage().getStorageFormat().getInputFormat();
        HoodieTableType hoodieTableType = HudiTableTypeUtils.fromInputFormat(inputFormat);
        Lazy<HoodieTableMetaClient> lazyMetaClient = Lazy.lazily(() -> buildTableMetaClient(fileSystem, tableName.toString(), basePath));
        Optional<Lazy<Schema>> hudiTableSchema = isResolveColumnNameCasingEnabled(session) ?
                Optional.of(Lazy.lazily(() -> getLatestTableSchema(lazyMetaClient.get(), tableName.getTableName()))) : Optional.empty();

        return new HudiTableHandle(
                Optional.of(table),
                Optional.of(lazyMetaClient),
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getStorage().getLocation(),
                hoodieTableType,
                getPartitionKeyColumnHandles(table, typeManager),
                ImmutableSet.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                hudiTableSchema);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(tableName, session)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(SchemaTableName tableName, ConnectorSession session)
    {
        Optional<HudiTableName> nameOptional = HudiTableName.from(tableName.getTableName());
        if (nameOptional.isEmpty()) {
            return Optional.empty();
        }
        HudiTableName name = nameOptional.get();
        if (name.tableType() == TableType.DATA) {
            return Optional.empty();
        }

        Optional<Table> tableOptional = metastore.getTable(tableName.getSchemaName(), name.tableName());
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }
        if (!isHudiTable(tableOptional.get())) {
            return Optional.empty();
        }
        return switch (name.tableType()) {
            case DATA -> throw new AssertionError();
            case TIMELINE -> {
                SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.tableNameWithType());
                yield Optional.of(new TimelineTable(fileSystemFactory.create(session), systemTableName, tableOptional.get()));
            }
        };
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) table;
        return getTableMetadata(hudiTableHandle.getSchemaTableName(), getColumnsToHide(session));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        HudiPredicates predicates = HudiPredicates.from(constraint.getSummary());
        TupleDomain<HiveColumnHandle> regularColumnPredicates = predicates.getRegularColumnPredicates();
        TupleDomain<HiveColumnHandle> partitionColumnPredicates = predicates.getPartitionColumnPredicates();

        // TODO Since the constraint#predicate isn't utilized during split generation. So,
        //  Let's not add constraint#predicateColumns to newConstraintColumns.
        Set<HiveColumnHandle> newConstraintColumns = Stream.concat(
                        Stream.concat(
                                regularColumnPredicates.getDomains().stream()
                                        .map(Map::keySet)
                                        .flatMap(Collection::stream),
                                partitionColumnPredicates.getDomains().stream()
                                        .map(Map::keySet)
                                        .flatMap(Collection::stream)),
                        handle.getConstraintColumns().stream())
                .collect(toImmutableSet());

        HudiTableHandle newHudiTableHandle = handle.applyPredicates(
                newConstraintColumns,
                partitionColumnPredicates,
                regularColumnPredicates);

        if (handle.getPartitionPredicates().equals(newHudiTableHandle.getPartitionPredicates())
                && handle.getRegularPredicates().equals(newHudiTableHandle.getRegularPredicates())
                && handle.getConstraintColumns().equals(newHudiTableHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHudiTableHandle,
                newHudiTableHandle.getRegularPredicates().transformKeys(ColumnHandle.class::cast),
                constraint.getExpression(),
                false));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));
        return hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        HudiTableHandle table = (HudiTableHandle) tableHandle;
        return Optional.of(new HudiTableInfo(table.getSchemaTableName(), table.getTableType().name(), table.getBasePath()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (TableInfo tableInfo : metastore.getTables(schemaName)) {
                tableNames.add(tableInfo.tableName());
            }
        }
        return tableNames.build();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);
        List<SchemaTableName> tables = prefix.getTable()
                .map(_ -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = tables.stream()
                .map(table -> getTableColumnMetadata(session, table))
                .flatMap(Optional::stream)
                .collect(toImmutableMap(RelationColumnsMetadata::name, Function.identity()));
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isTableStatisticsEnabled(session) || !isHudiMetadataTableEnabled(session)) {
            return TableStatistics.empty();
        }

        List<HiveColumnHandle> columnHandles = getColumnHandles(session, tableHandle)
                .values().stream()
                .map(e -> (HiveColumnHandle) e)
                .filter(e -> !e.isHidden())
                .toList();
        return getTableStatisticsFromCache(
                (HudiTableHandle) tableHandle, columnHandles, tableStatisticsCache, refreshingKeysInProgress, tableStatisticsExecutor);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) handle;
        if (isQueryPartitionFilterRequired(session)) {
            if (!hudiTableHandle.getPartitionColumns().isEmpty()) {
                Set<String> partitionColumns = hudiTableHandle.getPartitionColumns().stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toImmutableSet());
                Set<String> constraintColumns = hudiTableHandle.getConstraintColumns().stream()
                        .map(HiveColumnHandle::getBaseColumnName)
                        .collect(toImmutableSet());
                if (Collections.disjoint(constraintColumns, partitionColumns)) {
                    throw new TrinoException(
                            QUERY_REJECTED,
                            format("Filter required on %s for at least one of the partition columns: %s", hudiTableHandle.getSchemaTableName(), String.join(", ", partitionColumns)));
                }
            }
        }
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // hudi supports only a columnar (parquet) storage format
        return true;
    }

    HiveMetastore getMetastore()
    {
        return metastore;
    }

    private Optional<RelationColumnsMetadata> getTableColumnMetadata(ConnectorSession session, SchemaTableName table)
    {
        try {
            List<ColumnMetadata> columns = getTableMetadata(table, getColumnsToHide(session)).getColumns();
            return Optional.of(RelationColumnsMetadata.forTable(table, columns));
        }
        catch (TableNotFoundException _) {
            return Optional.empty();
        }
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName, Collection<String> columnsToHide)
    {
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table);
        List<ColumnMetadata> columns = hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .filter(column -> !columnsToHide.contains(column.getName()))
                .map(metadataGetter)
                .collect(toImmutableList());

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        // Location property
        String location = table.getStorage().getOptionalLocation().orElse(null);
        if (!isNullOrEmpty(location)) {
            properties.put(LOCATION_PROPERTY, location);
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(TABLE_COMMENT));
        return new ConnectorTableMetadata(tableName, columns, properties.buildOrThrow(), comment);
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName
                .filter(name -> !isHiveSystemSchema(name))
                .map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session));
    }

    private static TableStatistics getTableStatisticsFromCache(
            HudiTableHandle tableHandle,
            List<HiveColumnHandle> columnHandles,
            Map<TableStatisticsCacheKey, HudiTableStatistics> cache,
            Set<TableStatisticsCacheKey> refreshingKeysInProgress,
            ExecutorService tableStatisticsExecutor)
    {
        TableStatisticsCacheKey key = new TableStatisticsCacheKey(tableHandle.getBasePath());
        HudiTableStatistics cachedValue = cache.get(key);
        TableStatistics statisticsToReturn = TableStatistics.empty();
        if (cachedValue != null) {
            // Here we avoid checking the latest commit which requires loading the meta client and timeline
            // which can block query planning. We assume that the cache result might be stale but close
            // enough for CBO.
            log.info("Returning cached table statistics for table: %s, latest commit in cache: %s",
                    tableHandle.getSchemaTableName(), cachedValue.latestCommit());
            statisticsToReturn = cachedValue.tableStatistics();
        }

        triggerAsyncStatsRefresh(tableHandle, columnHandles, cache, key, refreshingKeysInProgress, tableStatisticsExecutor);
        return statisticsToReturn;
    }

    private static void triggerAsyncStatsRefresh(
            HudiTableHandle tableHandle,
            List<HiveColumnHandle> columnHandles,
            Map<TableStatisticsCacheKey, HudiTableStatistics> cache,
            TableStatisticsCacheKey key,
            Set<TableStatisticsCacheKey> refreshingKeysInProgress,
            ExecutorService tableStatisticsExecutor)
    {
        if (refreshingKeysInProgress.add(key)) {
            tableStatisticsExecutor.submit(() -> {
                HoodieTimer refreshTimer = HoodieTimer.start();
                try {
                    log.info("Starting async statistics calculation for table: %s", tableHandle.getSchemaTableName());
                    HoodieTableMetaClient metaClient = tableHandle.getMetaClient();
                    Option<HoodieInstant> latestCommitOption = metaClient.getActiveTimeline()
                            .getTimelineOfActions(CollectionUtils.createSet(
                                    COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION, INDEXING_ACTION))
                            .filterCompletedInstants().lastInstant();

                    if (latestCommitOption.isEmpty()) {
                        log.info("Putting table statistics of 0 row in %s ms for empty table: %s",
                                refreshTimer.endTimer(), tableHandle.getSchemaTableName());
                        cache.put(key, new HudiTableStatistics(
                                // A dummy instant that does not match any commit
                                new HoodieInstant(COMPLETED, COMMIT_ACTION, "", InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
                                TableStatistics.builder().setRowCount(Estimate.of(0)).build()));
                        return;
                    }

                    HoodieInstant latestCommit = latestCommitOption.get();
                    HudiTableStatistics oldValue = cache.get(key);
                    if (oldValue != null && latestCommit.equals(oldValue.latestCommit())) {
                        log.info("Table statistics is still valid for table: %s (checked in %s ms)",
                                tableHandle.getSchemaTableName(), refreshTimer.endTimer());
                        return;
                    }

                    if (!metaClient.getTableConfig().isMetadataTableAvailable()
                            || !metaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS)) {
                        log.info("Putting empty table statistics in %s ms as metadata table or "
                                + "column stats is not available for table: %s",
                                refreshTimer.endTimer(), tableHandle.getSchemaTableName());
                        cache.put(key, new HudiTableStatistics(latestCommit, TableStatistics.empty()));
                        return;
                    }

                    TableStatistics newStatistics = TableStatisticsReader.create(metaClient)
                            .getTableStatistics(latestCommit, columnHandles);
                    HudiTableStatistics newValue = new HudiTableStatistics(latestCommit, newStatistics);
                    cache.put(key, newValue);
                    log.info("Async table statistics calculation finished in %s ms for table: %s, commit: %s",
                            refreshTimer.endTimer(), tableHandle.getSchemaTableName(), latestCommit);
                }
                catch (Throwable e) {
                    log.error(e, "Error calculating table statistics asynchronously for table %s", tableHandle.getSchemaTableName());
                }
                finally {
                    refreshingKeysInProgress.remove(key);
                }
            });
        }
        else {
            log.debug("Table statistics refresh already in progress for table: %s", tableHandle.getSchemaTableName());
        }
    }

    private record TableStatisticsCacheKey(String basePath) {}
}
