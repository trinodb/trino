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
package io.trino.plugin.hive.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionUpdateAndMergeResults;
import io.trino.plugin.hive.TableInvalidationCallback;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.projection.PartitionProjection;
import io.trino.plugin.hive.security.SqlStandardAccessControlMetadataMetastore;
import io.trino.plugin.hive.util.ValidTxnWriteIdList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.TypeManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.metastore.Partitions.toPartitionValues;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.StatisticsUpdateMode.MERGE_INCREMENTAL;
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_SOME_COLUMNS;
import static io.trino.metastore.StatisticsUpdateMode.UNDO_MERGE_INCREMENTAL;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_DROPPED_DURING_QUERY;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getBasicStatisticsWithSparkFallback;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.SparkMetastoreUtil.getSparkTableStatistics;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getPartitionProjectionFromTable;
import static io.trino.plugin.hive.util.AcidTables.isTransactionalTable;
import static io.trino.plugin.hive.util.HiveWriteUtils.isFileCreatedByQuery;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SemiTransactionalHiveMetastore
        implements SqlStandardAccessControlMetadataMetastore
{
    private static final Logger log = Logger.get(SemiTransactionalHiveMetastore.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 20;
    private static final Pattern DELTA_DIRECTORY_MATCHER = Pattern.compile("(delete_)?delta_\\d+_\\d+_\\d+$");

    private static final RetryPolicy<?> DELETE_RETRY_POLICY = RetryPolicy.builder()
            .withDelay(java.time.Duration.ofSeconds(1))
            .withMaxDuration(java.time.Duration.ofSeconds(30))
            .withMaxAttempts(3)
            .abortOn(TrinoFileSystem::isUnrecoverableException)
            .build();

    private static final Map<AcidOperation, ActionType> ACID_OPERATION_ACTION_TYPES = ImmutableMap.of(
            AcidOperation.INSERT, ActionType.INSERT_EXISTING,
            AcidOperation.MERGE, ActionType.MERGE);

    private final HiveMetastore delegate;
    private final TypeManager typeManager;
    private final boolean partitionProjectionEnabled;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Executor fileSystemExecutor;
    private final Executor dropExecutor;
    private final Executor updateExecutor;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean deleteSchemaLocationsFallback;
    private final ScheduledExecutorService heartbeatExecutor;
    private final Optional<Duration> configuredTransactionHeartbeatInterval;
    private final TableInvalidationCallback tableInvalidationCallback;

    @GuardedBy("this")
    private final Map<SchemaTableName, Action<TableAndMore>> tableActions = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> partitionActions = new HashMap<>();
    @GuardedBy("this")
    private long declaredIntentionsToWriteCounter;
    @GuardedBy("this")
    private final List<DeclaredIntentionToWrite> declaredIntentionsToWrite = new ArrayList<>();
    @GuardedBy("this")
    private ExclusiveOperation bufferedExclusiveOperation;
    @GuardedBy("this")
    private State state = State.EMPTY;

    @GuardedBy("this")
    private Optional<String> currentQueryId = Optional.empty();
    @GuardedBy("this")
    private Optional<Supplier<HiveTransaction>> hiveTransactionSupplier = Optional.empty();
    // hiveTransactionSupplier is used to lazily open hive transaction for queries.  It is opened
    // eagerly for insert operations. currentHiveTransaction is needed to do hive transaction
    // cleanup only if a transaction was opened
    @GuardedBy("this")
    private Optional<HiveTransaction> currentHiveTransaction = Optional.empty();

    public SemiTransactionalHiveMetastore(
            TypeManager typeManager,
            boolean partitionProjectionEnabled,
            TrinoFileSystemFactory fileSystemFactory,
            HiveMetastore delegate,
            Executor fileSystemExecutor,
            Executor dropExecutor,
            Executor updateExecutor,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean deleteSchemaLocationsFallback,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            ScheduledExecutorService heartbeatService,
            TableInvalidationCallback tableInvalidationCallback)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.partitionProjectionEnabled = partitionProjectionEnabled;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.fileSystemExecutor = requireNonNull(fileSystemExecutor, "fileSystemExecutor is null");
        this.dropExecutor = requireNonNull(dropExecutor, "dropExecutor is null");
        this.updateExecutor = requireNonNull(updateExecutor, "updateExecutor is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        this.heartbeatExecutor = heartbeatService;
        this.configuredTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");
        this.tableInvalidationCallback = requireNonNull(tableInvalidationCallback, "tableInvalidationCallback is null");
    }

    public List<String> getAllDatabases()
    {
        synchronized (this) {
            checkReadable();
        }
        return delegate.getAllDatabases();
    }

    /**
     * Get the underlying metastore. Use this method with caution as it bypasses the current transactional state,
     * so modifications made in the transaction are visible.
     */
    public HiveMetastore unsafeGetRawHiveMetastore()
    {
        return delegate;
    }

    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        checkReadable();
        return delegate.getDatabase(databaseName);
    }

    public List<TableInfo> getTables(String databaseName)
    {
        synchronized (this) {
            checkReadable();
            if (!tableActions.isEmpty()) {
                throw new UnsupportedOperationException("Listing all tables after adding/dropping/altering tables/views in a transaction is not supported");
            }
        }
        return delegate.getTables(databaseName);
    }

    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        checkReadable();
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return delegate.getTable(databaseName, tableName);
        }
        return switch (tableAction.type()) {
            case ADD, ALTER, INSERT_EXISTING, MERGE -> Optional.of(tableAction.data().getTable());
            case DROP -> Optional.empty();
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + tableAction.type());
        };
    }

    public synchronized boolean isReadableWithinTransaction(String databaseName, String tableName)
    {
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return true;
        }
        return switch (tableAction.type()) {
            case ADD, ALTER -> true;
            case INSERT_EXISTING, MERGE -> false; // Until the transaction is committed, the table data may or may not be visible.
            case DROP, DROP_PRESERVE_DATA -> false;
        };
    }

    public synchronized PartitionStatistics getTableStatistics(String databaseName, String tableName, Optional<Set<String>> columns)
    {
        checkReadable();
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            Table table = getExistingTable(databaseName, tableName);
            Set<String> columnNames = columns.orElseGet(() -> Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                    .map(Column::getName)
                    .collect(toImmutableSet()));

            if (delegate.useSparkTableStatistics()) {
                Optional<PartitionStatistics> sparkTableStatistics = getSparkTableStatistics(table.getParameters(), columnNames.stream()
                        .map(table::getColumn)
                        .flatMap(Optional::stream)
                        .collect(toImmutableMap(Column::getName, Column::getType)));
                if (sparkTableStatistics.isPresent()) {
                    return sparkTableStatistics.get();
                }
            }

            HiveBasicStatistics basicStatistics = getHiveBasicStatistics(table.getParameters());
            if (columnNames.isEmpty()) {
                return new PartitionStatistics(basicStatistics, ImmutableMap.of());
            }
            return new PartitionStatistics(basicStatistics, delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
        }
        return switch (tableAction.type()) {
            case ADD, ALTER, INSERT_EXISTING, MERGE -> tableAction.data().getStatistics();
            case DROP -> PartitionStatistics.empty();
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + tableAction.type());
        };
    }

    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> columns, Set<String> partitionNames)
    {
        checkReadable();
        Optional<Table> table = getTable(databaseName, tableName);
        if (table.isEmpty()) {
            return ImmutableMap.of();
        }
        TableSource tableSource = getTableSource(databaseName, tableName);
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(table.get().getSchemaTableName(), k -> new HashMap<>());
        ImmutableSet.Builder<String> partitionNamesToQuery = ImmutableSet.builder();
        ImmutableMap.Builder<String, PartitionStatistics> resultBuilder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<PartitionAndMore> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                switch (tableSource) {
                    case PRE_EXISTING_TABLE -> partitionNamesToQuery.add(partitionName);
                    case CREATED_IN_THIS_TRANSACTION -> resultBuilder.put(partitionName, PartitionStatistics.empty());
                }
            }
            else {
                resultBuilder.put(partitionName, partitionAction.data().statistics());
            }
        }

        Set<String> missingPartitions = partitionNamesToQuery.build();
        if (missingPartitions.isEmpty()) {
            return resultBuilder.buildOrThrow();
        }

        Map<String, Partition> existingPartitions = getExistingPartitions(databaseName, tableName, partitionNames);
        if (delegate.useSparkTableStatistics()) {
            Map<String, Partition> unprocessedPartitions = new HashMap<>();
            existingPartitions.forEach((partitionName, partition) -> {
                Optional<PartitionStatistics> sparkPartitionStatistics = getSparkTableStatistics(partition.getParameters(), columns.stream()
                        .map(table.get()::getColumn)
                        .flatMap(Optional::stream)
                        .collect(toImmutableMap(Column::getName, Column::getType)));
                sparkPartitionStatistics.ifPresentOrElse(
                        statistics -> resultBuilder.put(partitionName, statistics),
                        () -> unprocessedPartitions.put(partitionName, partition));
            });
            existingPartitions = unprocessedPartitions;
        }

        if (!existingPartitions.isEmpty()) {
            Map<String, HiveBasicStatistics> basicStats = existingPartitions.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> {
                        if (delegate.useSparkTableStatistics()) {
                            return getBasicStatisticsWithSparkFallback(entry.getValue().getParameters());
                        }
                        return getHiveBasicStatistics(entry.getValue().getParameters());
                    }));

            if (columns.isEmpty()) {
                basicStats.forEach((partitionName, basicStatistics) -> resultBuilder.put(partitionName, new PartitionStatistics(basicStatistics, ImmutableMap.of())));
            }
            else {
                Map<String, Map<String, HiveColumnStatistics>> columnStats = delegate.getPartitionColumnStatistics(databaseName, tableName, basicStats.keySet(), columns);
                basicStats.forEach((key, value) -> resultBuilder.put(key, new PartitionStatistics(value, columnStats.getOrDefault(key, ImmutableMap.of()))));
            }
        }
        return clearRowCountWhenAllPartitionsHaveNoRows(resultBuilder.buildOrThrow());
    }

    private static Map<String, PartitionStatistics> clearRowCountWhenAllPartitionsHaveNoRows(Map<String, PartitionStatistics> partitionStatistics)
    {
        if (partitionStatistics.isEmpty()) {
            return partitionStatistics;
        }

        // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
        // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
        // used to ingest data into partitioned hive tables.
        long tableRowCount = partitionStatistics.values().stream()
                .mapToLong(statistics -> statistics.basicStatistics().getRowCount().orElse(0))
                .sum();
        if (tableRowCount != 0) {
            return partitionStatistics;
        }
        return partitionStatistics.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(),
                        entry.getValue().withBasicStatistics(entry.getValue().basicStatistics().withEmptyRowCount())))
                .collect(toImmutableMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    /**
     * This method can only be called when the table is known to exist
     */
    @GuardedBy("this")
    private TableSource getTableSource(String databaseName, String tableName)
    {
        checkHoldsLock();

        checkReadable();
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return TableSource.PRE_EXISTING_TABLE;
        }
        return switch (tableAction.type()) {
            case ADD -> TableSource.CREATED_IN_THIS_TRANSACTION;
            case DROP -> throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            case ALTER, INSERT_EXISTING, MERGE -> TableSource.PRE_EXISTING_TABLE;
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + tableAction.type());
        };
    }

    public synchronized HivePageSinkMetadata generatePageSinkMetadata(SchemaTableName schemaTableName)
    {
        checkReadable();
        Optional<Table> table = getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (table.isEmpty()) {
            return new HivePageSinkMetadata(schemaTableName, Optional.empty(), ImmutableMap.of());
        }
        Map<List<String>, Action<PartitionAndMore>> partitionActionMap = partitionActions.get(schemaTableName);
        Map<List<String>, Optional<Partition>> modifiedPartitionMap;
        if (partitionActionMap == null) {
            modifiedPartitionMap = ImmutableMap.of();
        }
        else {
            ImmutableMap.Builder<List<String>, Optional<Partition>> modifiedPartitionMapBuilder = ImmutableMap.builder();
            for (Entry<List<String>, Action<PartitionAndMore>> entry : partitionActionMap.entrySet()) {
                modifiedPartitionMapBuilder.put(entry.getKey(), getPartitionFromPartitionAction(entry.getValue()));
            }
            modifiedPartitionMap = modifiedPartitionMapBuilder.buildOrThrow();
        }
        return new HivePageSinkMetadata(
                schemaTableName,
                table,
                modifiedPartitionMap);
    }

    public synchronized void createDatabase(ConnectorSession session, Database database)
    {
        String queryId = session.getQueryId();

        // Ensure the database has queryId set. This is relied on for exception handling
        verify(
                getQueryId(database).orElseThrow(() -> new IllegalArgumentException("Query id is not present")).equals(queryId),
                "Database '%s' does not have correct query id set",
                database.getDatabaseName());

        setExclusive(delegate -> delegate.createDatabase(database));
    }

    public synchronized void dropDatabase(ConnectorSession session, String schemaName)
    {
        setExclusive(delegate -> {
            boolean deleteData = shouldDeleteDatabaseData(session, schemaName);
            delegate.dropDatabase(schemaName, deleteData);
        });
    }

    public boolean shouldDeleteDatabaseData(ConnectorSession session, String schemaName)
    {
        Optional<Location> location = delegate.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName))
                .getLocation()
                .map(Location::of);

        // If we see files in the schema location, don't delete it.
        // If we see no files, request deletion.
        // If we fail to check the schema location, behave according to fallback.
        return location.map(path -> {
            try {
                TrinoFileSystem fileSystem = fileSystemFactory.create(session);
                return !fileSystem.listFiles(path).hasNext() &&
                        fileSystem.listDirectories(path).isEmpty();
            }
            catch (IOException e) {
                log.warn(e, "Could not check schema directory '%s'", path);
                return deleteSchemaLocationsFallback;
            }
        }).orElse(deleteSchemaLocationsFallback);
    }

    public synchronized void renameDatabase(String source, String target)
    {
        setExclusive(delegate -> delegate.renameDatabase(source, target));
    }

    public synchronized void setDatabaseOwner(String source, HivePrincipal principal)
    {
        setExclusive(delegate -> delegate.setDatabaseOwner(source, principal));
    }

    // TODO: Allow updating statistics for 2 tables in the same transaction
    public synchronized void setTableStatistics(Table table, PartitionStatistics tableStatistics)
    {
        OptionalLong acidWriteId = getOptionalAcidTransaction().getOptionalWriteId();
        setExclusive(delegate -> delegate.updateTableStatistics(table.getDatabaseName(), table.getTableName(), acidWriteId, OVERWRITE_SOME_COLUMNS, tableStatistics));
    }

    // TODO: Allow updating statistics for 2 tables in the same transaction
    public synchronized void setPartitionStatistics(Table table, Map<List<String>, PartitionStatistics> partitionStatisticsMap)
    {
        Map<String, PartitionStatistics> updates = partitionStatisticsMap.entrySet().stream().collect(
                toImmutableMap(
                        entry -> getPartitionName(table, entry.getKey()),
                        Entry::getValue));
        setExclusive(delegate ->
                delegate.updatePartitionStatistics(
                        delegate.getTable(table.getDatabaseName(), table.getTableName())
                                .orElseThrow(() -> new TableNotFoundException(table.getSchemaTableName())),
                        OVERWRITE_SOME_COLUMNS,
                        updates));
    }

    /**
     * {@code currentLocation} needs to be supplied if a writePath exists for the table.
     */
    public synchronized void createTable(
            ConnectorSession session,
            Table table,
            PrincipalPrivileges principalPrivileges,
            Optional<Location> currentLocation,
            Optional<List<String>> files,
            boolean ignoreExisting,
            PartitionStatistics statistics,
            boolean cleanExtraOutputFilesOnCommit)
    {
        setShared();
        // When creating a table, it should never have partition actions. This is just a validation check.
        checkNoPartitionAction(table.getDatabaseName(), table.getTableName());
        Action<TableAndMore> oldTableAction = tableActions.get(table.getSchemaTableName());
        TableAndMore tableAndMore = new TableAndMore(table, Optional.of(principalPrivileges), currentLocation, files, ignoreExisting, statistics, statistics, cleanExtraOutputFilesOnCommit);
        if (oldTableAction == null) {
            tableActions.put(table.getSchemaTableName(), new Action<>(ActionType.ADD, tableAndMore, session.getIdentity(), session.getQueryId()));
            return;
        }
        switch (oldTableAction.type()) {
            case DROP -> {
                if (!oldTableAction.identity().getUser().equals(session.getUser())) {
                    throw new TrinoException(TRANSACTION_CONFLICT, "Operation on the same table with different user in the same transaction is not supported");
                }
                tableActions.put(table.getSchemaTableName(), new Action<>(ActionType.ALTER, tableAndMore, session.getIdentity(), session.getQueryId()));
            }
            case ADD, ALTER, INSERT_EXISTING, MERGE -> throw new TableAlreadyExistsException(table.getSchemaTableName());
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + oldTableAction.type());
        }
    }

    public synchronized void dropTable(ConnectorSession session, String databaseName, String tableName)
    {
        setShared();
        // Dropping table with partition actions requires cleaning up staging data, which is not implemented yet.
        checkNoPartitionAction(databaseName, tableName);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null || oldTableAction.type() == ActionType.ALTER) {
            tableActions.put(schemaTableName, new Action<>(ActionType.DROP, null, session.getIdentity(), session.getQueryId()));
            return;
        }
        switch (oldTableAction.type()) {
            case DROP -> throw new TableNotFoundException(schemaTableName);
            case ADD, ALTER, INSERT_EXISTING, MERGE -> throw new UnsupportedOperationException("dropping a table added/modified in the same transaction is not supported");
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + oldTableAction.type());
        }
    }

    public synchronized void replaceTable(String databaseName, String tableName, Table table, PrincipalPrivileges principalPrivileges)
    {
        setExclusive(delegate -> delegate.replaceTable(databaseName, tableName, table, principalPrivileges));
    }

    public synchronized void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        setExclusive(delegate -> {
            Optional<Table> oldTable = delegate.getTable(databaseName, tableName);
            try {
                delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
            }
            finally {
                // perform explicit invalidation for the table in exclusive metastore operations
                oldTable.ifPresent(tableInvalidationCallback::invalidate);
            }
        });
    }

    public synchronized void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        setExclusive(delegate -> delegate.commentTable(databaseName, tableName, comment));
    }

    public synchronized void setTableOwner(String schema, String table, HivePrincipal principal)
    {
        setExclusive(delegate -> delegate.setTableOwner(schema, table, principal));
    }

    public synchronized void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        setExclusive(delegate -> delegate.commentColumn(databaseName, tableName, columnName, comment));
    }

    public synchronized void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        setExclusive(delegate -> delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment));
    }

    public synchronized void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        setExclusive(delegate -> delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName));
    }

    public synchronized void dropColumn(String databaseName, String tableName, String columnName)
    {
        setExclusive(delegate -> delegate.dropColumn(databaseName, tableName, columnName));
    }

    public synchronized void finishChangingExistingTable(
            AcidOperation acidOperation,
            ConnectorSession session,
            String databaseName,
            String tableName,
            Location currentLocation,
            List<String> fileNames,
            PartitionStatistics statisticsUpdate,
            boolean cleanExtraOutputFilesOnCommit)
    {
        // Data can only be inserted into partitions and unpartitioned tables. They can never be inserted into a partitioned table.
        // Therefore, this method assumes that the table is unpartitioned.
        setShared();
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        ActionType actionType = requireNonNull(ACID_OPERATION_ACTION_TYPES.get(acidOperation), "ACID_OPERATION_ACTION_TYPES doesn't contain the acidOperation");
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null) {
            Table table = getExistingTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (isAcidTransactionRunning()) {
                table = Table.builder(table).setWriteId(OptionalLong.of(getRequiredAcidTransaction().getWriteId())).build();
            }
            PartitionStatistics currentStatistics = getTableStatistics(databaseName, tableName, Optional.empty());
            tableActions.put(
                    schemaTableName,
                    new Action<>(
                            actionType,
                            new TableAndMore(
                                    table,
                                    Optional.empty(),
                                    Optional.of(currentLocation),
                                    Optional.of(fileNames),
                                    false,
                                    MERGE_INCREMENTAL.updatePartitionStatistics(currentStatistics, statisticsUpdate),
                                    statisticsUpdate,
                                    cleanExtraOutputFilesOnCommit),
                            session.getIdentity(),
                            session.getQueryId()));
            return;
        }

        switch (oldTableAction.type()) {
            case DROP -> throw new TableNotFoundException(schemaTableName);
            case ADD, ALTER, INSERT_EXISTING, MERGE ->
                    throw new UnsupportedOperationException("Inserting into an unpartitioned table that were added, altered, or inserted into in the same transaction is not supported");
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + oldTableAction.type());
        }
    }

    private synchronized boolean isAcidTransactionRunning()
    {
        return currentHiveTransaction.isPresent() && currentHiveTransaction.get().getTransaction().isAcidTransactionRunning();
    }

    public synchronized void truncateUnpartitionedTable(ConnectorSession session, String databaseName, String tableName)
    {
        checkReadable();
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        if (!table.getTableType().equals(MANAGED_TABLE.name())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot delete from non-managed Hive table");
        }
        if (!table.getPartitionColumns().isEmpty()) {
            throw new IllegalArgumentException("Table is partitioned");
        }

        Location location = Location.of(table.getStorage().getLocation());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        setExclusive(delegate -> {
            RecursiveDeleteResult recursiveDeleteResult = recursiveDeleteFiles(fileSystem, location, ImmutableSet.of(""), false);
            if (!recursiveDeleteResult.notDeletedEligibleItems().isEmpty()) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, format(
                        "Error deleting from unpartitioned table %s. These items cannot be deleted: %s",
                        schemaTableName,
                        recursiveDeleteResult.notDeletedEligibleItems()));
            }
        });
    }

    public synchronized void finishMerge(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Location currentLocation,
            List<PartitionUpdateAndMergeResults> partitionUpdateAndMergeResults,
            List<Partition> partitions)
    {
        if (partitionUpdateAndMergeResults.isEmpty()) {
            return;
        }
        checkArgument(partitionUpdateAndMergeResults.size() >= partitions.size(), "partitionUpdateAndMergeResults.size() (%s) < partitions.size() (%s)", partitionUpdateAndMergeResults.size(), partitions.size());
        setShared();
        if (partitions.isEmpty()) {
            return;
        }
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null) {
            Table table = getExistingTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            PrincipalPrivileges principalPrivileges = table.getOwner().isEmpty() ? NO_PRIVILEGES :
                    buildInitialPrivilegeSet(table.getOwner().get());
            tableActions.put(
                    schemaTableName,
                    new Action<>(
                            ActionType.MERGE,
                            new TableAndMergeResults(
                                    table,
                                    Optional.of(principalPrivileges),
                                    Optional.of(currentLocation),
                                    partitionUpdateAndMergeResults),
                            session.getIdentity(),
                            session.getQueryId()));
            return;
        }

        switch (oldTableAction.type()) {
            case DROP -> throw new TableNotFoundException(schemaTableName);
            case ADD, ALTER, INSERT_EXISTING, MERGE ->
                    throw new UnsupportedOperationException("Inserting, updating or deleting in a table that was added, altered, inserted into, updated or deleted from in the same transaction is not supported");
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + oldTableAction.type());
        }
    }

    public synchronized Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        Optional<Table> table = getTable(databaseName, tableName);

        if (table.isEmpty()) {
            return Optional.empty();
        }

        List<String> columnNames = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        return doGetPartitionNames(databaseName, tableName, columnNames, TupleDomain.all());
    }

    public synchronized Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return doGetPartitionNames(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @GuardedBy("this")
    private Optional<List<String>> doGetPartitionNames(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        checkHoldsLock();

        checkReadable();

        if (partitionKeysFilter.isNone()) {
            return Optional.of(ImmutableList.of());
        }

        Optional<Table> table = getTable(databaseName, tableName);
        if (table.isEmpty()) {
            return Optional.empty();
        }
        List<String> partitionNames;
        TableSource tableSource = getTableSource(databaseName, tableName);
        partitionNames = switch (tableSource) {
            case CREATED_IN_THIS_TRANSACTION -> ImmutableList.of();
            case PRE_EXISTING_TABLE -> getOptionalPartitions(databaseName, tableName, columnNames, partitionKeysFilter)
                    .orElseThrow(() -> new TrinoException(TRANSACTION_CONFLICT, format("Table '%s.%s' was dropped by another transaction", databaseName, tableName)));
        };
        Set<String> duplicatePartitionNames = ImmutableMultiset.copyOf(partitionNames)
                .entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableSet());
        if (!duplicatePartitionNames.isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, format("Metastore returned duplicate partition names for %s", duplicatePartitionNames));
        }
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(table.get().getSchemaTableName(), k -> new HashMap<>());
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        // alter/remove newly altered/dropped partitions from the results from underlying metastore
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<PartitionAndMore> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                resultBuilder.add(partitionName);
                continue;
            }
            switch (partitionAction.type()) {
                case ADD -> throw new TrinoException(TRANSACTION_CONFLICT, format("Another transaction created partition %s in table %s.%s", partitionValues, databaseName, tableName));
                case DROP, DROP_PRESERVE_DATA -> {
                    // do nothing
                }
                case ALTER, INSERT_EXISTING, MERGE -> resultBuilder.add(partitionName);
            }
        }
        // add newly added partitions to the results from underlying metastore.
        if (!partitionActionsOfTable.isEmpty()) {
            for (Action<PartitionAndMore> partitionAction : partitionActionsOfTable.values()) {
                if (partitionAction.type() == ActionType.ADD) {
                    List<String> values = partitionAction.data().partition().getValues();
                    resultBuilder.add(makePartName(columnNames, values));
                }
            }
        }
        return Optional.of(resultBuilder.build());
    }

    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        checkReadable();
        TableSource tableSource = getTableSource(databaseName, tableName);
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        ImmutableList.Builder<String> partitionNamesToQueryBuilder = ImmutableList.builder();
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<PartitionAndMore> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                switch (tableSource) {
                    case PRE_EXISTING_TABLE -> partitionNamesToQueryBuilder.add(partitionName);
                    case CREATED_IN_THIS_TRANSACTION -> resultBuilder.put(partitionName, Optional.empty());
                }
            }
            else {
                resultBuilder.put(partitionName, getPartitionFromPartitionAction(partitionAction));
            }
        }

        List<String> partitionNamesToQuery = partitionNamesToQueryBuilder.build();
        if (!partitionNamesToQuery.isEmpty()) {
            Map<String, Optional<Partition>> delegateResult = getOptionalPartitions(
                    databaseName,
                    tableName,
                    partitionNamesToQuery);
            resultBuilder.putAll(delegateResult);
        }

        return resultBuilder.buildOrThrow();
    }

    private static Optional<Partition> getPartitionFromPartitionAction(Action<PartitionAndMore> partitionAction)
    {
        return switch (partitionAction.type()) {
            case ADD, ALTER, INSERT_EXISTING, MERGE -> Optional.of(partitionAction.data().getAugmentedPartitionForInTransactionRead());
            case DROP, DROP_PRESERVE_DATA -> Optional.empty();
        };
    }

    public synchronized void addPartition(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Partition partition,
            Location currentLocation,
            Optional<List<String>> files,
            PartitionStatistics statistics,
            boolean cleanExtraOutputFilesOnCommit)
    {
        setShared();
        checkArgument(getQueryId(partition).isPresent());
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partition.getValues());
        if (oldPartitionAction == null) {
            partitionActionsOfTable.put(
                    partition.getValues(),
                    new Action<>(ActionType.ADD, new PartitionAndMore(partition, currentLocation, files, statistics, statistics, cleanExtraOutputFilesOnCommit), session.getIdentity(), session.getQueryId()));
            return;
        }
        switch (oldPartitionAction.type()) {
            case DROP, DROP_PRESERVE_DATA -> {
                if (!oldPartitionAction.identity().getUser().equals(session.getUser())) {
                    throw new TrinoException(TRANSACTION_CONFLICT, "Operation on the same partition with different user in the same transaction is not supported");
                }
                partitionActionsOfTable.put(
                        partition.getValues(),
                        new Action<>(ActionType.ALTER, new PartitionAndMore(partition, currentLocation, files, statistics, statistics, cleanExtraOutputFilesOnCommit), session.getIdentity(), session.getQueryId()));
            }
            case ADD, ALTER, INSERT_EXISTING, MERGE ->
                    throw new TrinoException(ALREADY_EXISTS, format("Partition already exists for table '%s.%s': %s", databaseName, tableName, partition.getValues()));
        }
    }

    public synchronized void dropPartition(ConnectorSession session, String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        setShared();
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partitionValues);
        if (oldPartitionAction == null) {
            if (deleteData) {
                partitionActionsOfTable.put(partitionValues, new Action<>(ActionType.DROP, null, session.getIdentity(), session.getQueryId()));
            }
            else {
                partitionActionsOfTable.put(partitionValues, new Action<>(ActionType.DROP_PRESERVE_DATA, null, session.getIdentity(), session.getQueryId()));
            }
            return;
        }
        switch (oldPartitionAction.type()) {
            case DROP, DROP_PRESERVE_DATA -> throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
            case ADD, ALTER, INSERT_EXISTING, MERGE ->
                    throw new TrinoException(NOT_SUPPORTED, "dropping a partition added in the same transaction is not supported: %s %s %s"
                            .formatted(databaseName, tableName, partitionValues));
        }
    }

    public synchronized void finishInsertIntoExistingPartitions(
            ConnectorSession session,
            String databaseName,
            String tableName,
            List<PartitionUpdateInfo> partitionUpdateInfos,
            boolean cleanExtraOutputFilesOnCommit)
    {
        setShared();
        Table table = getExistingTable(databaseName, tableName);
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(table.getSchemaTableName(), k -> new HashMap<>());

        for (PartitionUpdateInfo partitionInfo : partitionUpdateInfos) {
            Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partitionInfo.partitionValues());
            if (oldPartitionAction != null) {
                switch (oldPartitionAction.type()) {
                    case DROP, DROP_PRESERVE_DATA -> throw new PartitionNotFoundException(table.getSchemaTableName(), partitionInfo.partitionValues());
                    case ADD, ALTER, INSERT_EXISTING, MERGE -> throw new UnsupportedOperationException("Inserting into a partition that were added, altered, or inserted into in the same transaction is not supported");
                    default -> throw new IllegalStateException("Unknown action type: " + oldPartitionAction.type());
                }
            }
        }

        // new data will on include current table columns
        // partition column stats do not include the partition keys
        Set<String> columnNames = table.getDataColumns().stream()
                .map(Column::getName)
                .collect(toImmutableSet());
        for (List<PartitionUpdateInfo> partitionInfoBatch : Iterables.partition(partitionUpdateInfos, 100)) {
            List<String> partitionNames = partitionInfoBatch.stream()
                    .map(PartitionUpdateInfo::partitionValues)
                    .map(partitionValues -> getPartitionName(databaseName, tableName, partitionValues))
                    .collect(toImmutableList());

            Map<String, Partition> partitionsByNames = getExistingPartitions(databaseName, tableName, partitionNames);
            Map<String, HiveBasicStatistics> basicStats = partitionsByNames.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> getHiveBasicStatistics(entry.getValue().getParameters())));
            Map<String, Map<String, HiveColumnStatistics>> columnStats = delegate.getPartitionColumnStatistics(databaseName, tableName, basicStats.keySet(), columnNames);

            for (int i = 0; i < partitionInfoBatch.size(); i++) {
                PartitionUpdateInfo partitionInfo = partitionInfoBatch.get(i);
                String partitionName = partitionNames.get(i);

                Partition partition = partitionsByNames.get(partitionName);
                PartitionStatistics currentStatistics = new PartitionStatistics(basicStats.get(partitionName), columnStats.get(partitionName));

                partitionActionsOfTable.put(
                        partitionInfo.partitionValues(),
                        new Action<>(
                                ActionType.INSERT_EXISTING,
                                new PartitionAndMore(
                                        partition,
                                        partitionInfo.currentLocation(),
                                        Optional.of(partitionInfo.fileNames()),
                                        MERGE_INCREMENTAL.updatePartitionStatistics(currentStatistics, partitionInfo.statisticsUpdate()),
                                        partitionInfo.statisticsUpdate(),
                                        cleanExtraOutputFilesOnCommit),
                                session.getIdentity(),
                                session.getQueryId()));
            }
        }
    }

    private synchronized AcidTransaction getRequiredAcidTransaction()
    {
        return currentHiveTransaction.orElseThrow(() -> new IllegalStateException("currentHiveTransaction not present")).getTransaction();
    }

    private synchronized AcidTransaction getOptionalAcidTransaction()
    {
        return currentHiveTransaction.map(HiveTransaction::getTransaction).orElse(NO_ACID_TRANSACTION);
    }

    private String getPartitionName(String databaseName, String tableName, List<String> partitionValues)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        return getPartitionName(table, partitionValues);
    }

    private static String getPartitionName(Table table, List<String> partitionValues)
    {
        List<String> columnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        return makePartName(columnNames, partitionValues);
    }

    @Override
    public synchronized void createRole(String role, String grantor)
    {
        setExclusive(delegate -> delegate.createRole(role, grantor));
    }

    @Override
    public synchronized void dropRole(String role)
    {
        setExclusive(delegate -> delegate.dropRole(role));
    }

    @Override
    public synchronized Set<String> listRoles()
    {
        checkReadable();
        return delegate.listRoles();
    }

    @Override
    public synchronized void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        setExclusive(delegate -> delegate.grantRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public synchronized void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        setExclusive(delegate -> delegate.revokeRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        checkReadable();
        return delegate.listRoleGrants(principal);
    }

    @Override
    public Optional<HivePrincipal> getDatabaseOwner(String databaseName)
    {
        Database database = getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));

        return database.getOwnerName().map(ownerName -> new HivePrincipal(database.getOwnerType().orElseThrow(), ownerName));
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<HivePrincipal> principal)
    {
        checkReadable();
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> tableAction = tableActions.get(schemaTableName);
        if (tableAction == null) {
            return delegate.listTablePrivileges(databaseName, tableName, getExistingTable(databaseName, tableName).getOwner(), principal);
        }
        return switch (tableAction.type()) {
            case ADD, ALTER -> {
                if (principal.isPresent() && principal.get().getType() == PrincipalType.ROLE) {
                    yield ImmutableSet.of();
                }
                Optional<String> owner = tableAction.data().getTable().getOwner();
                if (owner.isEmpty()) {
                    // todo the existing logic below seem off. Only permissions held by the table owner are returned
                    yield ImmutableSet.of();
                }
                String ownerUsername = owner.orElseThrow();
                if (principal.isPresent() && !principal.get().getName().equals(ownerUsername)) {
                    yield ImmutableSet.of();
                }
                Collection<HivePrivilegeInfo> privileges = tableAction.data().getPrincipalPrivileges().getUserPrivileges().get(ownerUsername);
                yield ImmutableSet.<HivePrivilegeInfo>builder()
                        .addAll(privileges)
                        .add(new HivePrivilegeInfo(OWNERSHIP, true, new HivePrincipal(USER, ownerUsername), new HivePrincipal(USER, ownerUsername)))
                        .build();
            }
            case INSERT_EXISTING, MERGE -> delegate.listTablePrivileges(databaseName, tableName, getExistingTable(databaseName, tableName).getOwner(), principal);
            case DROP -> throw new TableNotFoundException(schemaTableName);
            case DROP_PRESERVE_DATA -> throw new IllegalStateException("Unsupported action type: " + tableAction.type());
        };
    }

    private synchronized String getRequiredTableOwner(String databaseName, String tableName)
    {
        return getExistingTable(databaseName, tableName).getOwner().orElseThrow();
    }

    private Table getExistingTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private Map<String, Partition> getExistingPartitions(String databaseName, String tableName, Collection<String> partitionNames)
    {
        return getOptionalPartitions(databaseName, tableName, ImmutableList.copyOf(partitionNames)).entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue()
                        .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), extractPartitionValues(entry.getKey())))));
    }

    private Map<String, Optional<Partition>> getOptionalPartitions(String databaseName, String tableName, List<String> partitionNames)
    {
        return delegate.getTable(databaseName, tableName)
                .map(table -> getOptionalPartitions(table, partitionNames))
                .orElseGet(() -> partitionNames.stream()
                        .collect(toImmutableMap(name -> name, _ -> Optional.empty())));
    }

    private Map<String, Optional<Partition>> getOptionalPartitions(Table table, List<String> partitionNames)
    {
        if (partitionProjectionEnabled) {
            Optional<PartitionProjection> projection = getPartitionProjectionFromTable(table, typeManager);
            if (projection.isPresent()) {
                return projection.get().getProjectedPartitionsByNames(table, partitionNames);
            }
        }
        return delegate.getPartitionsByNames(table, partitionNames);
    }

    private Optional<List<String>> getOptionalPartitions(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        if (partitionProjectionEnabled) {
            Table table = getTable(databaseName, tableName)
                    .orElseThrow(() -> new TrinoException(HIVE_TABLE_DROPPED_DURING_QUERY, "Table does not exists: " + tableName));

            Optional<PartitionProjection> projection = getPartitionProjectionFromTable(table, typeManager);
            if (projection.isPresent()) {
                return projection.get().getProjectedPartitionNamesByFilter(columnNames, partitionKeysFilter);
            }
        }
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public synchronized void grantTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        setExclusive(delegate -> delegate.grantTablePrivileges(databaseName, tableName, getRequiredTableOwner(databaseName, tableName), grantee, grantor, privileges, grantOption));
    }

    @Override
    public synchronized void revokeTablePrivileges(String databaseName, String tableName, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        setExclusive(delegate -> delegate.revokeTablePrivileges(databaseName, tableName, getRequiredTableOwner(databaseName, tableName), grantee, grantor, privileges, grantOption));
    }

    public synchronized boolean functionExists(SchemaFunctionName name, String signatureToken)
    {
        checkReadable();
        return delegate.functionExists(name.getSchemaName(), name.getFunctionName(), signatureToken);
    }

    public synchronized Collection<LanguageFunction> getFunctions(String schemaName)
    {
        checkReadable();
        return delegate.getAllFunctions(schemaName);
    }

    public synchronized Collection<LanguageFunction> getFunctions(SchemaFunctionName name)
    {
        checkReadable();
        return delegate.getFunctions(name.getSchemaName(), name.getFunctionName());
    }

    public synchronized void createFunction(SchemaFunctionName name, LanguageFunction function)
    {
        setExclusive(delegate -> delegate.createFunction(name.getSchemaName(), name.getFunctionName(), function));
    }

    public synchronized void replaceFunction(SchemaFunctionName name, LanguageFunction function)
    {
        setExclusive(delegate -> delegate.replaceFunction(name.getSchemaName(), name.getFunctionName(), function));
    }

    public synchronized void dropFunction(SchemaFunctionName name, String signatureToken)
    {
        setExclusive(delegate -> delegate.dropFunction(name.getSchemaName(), name.getFunctionName(), signatureToken));
    }

    public synchronized String declareIntentionToWrite(ConnectorSession session, WriteMode writeMode, Location stagingPathRoot, SchemaTableName schemaTableName)
    {
        setShared();
        if (writeMode == WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
            Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.get(schemaTableName);
            if (partitionActionsOfTable != null && !partitionActionsOfTable.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot insert into a table with a partition that has been modified in the same transaction when Trino is configured to skip temporary directories.");
            }
        }
        ConnectorIdentity identity = session.getIdentity();
        String queryId = session.getQueryId();
        String declarationId = queryId + "_" + declaredIntentionsToWriteCounter;
        declaredIntentionsToWriteCounter++;
        declaredIntentionsToWrite.add(new DeclaredIntentionToWrite(declarationId, writeMode, identity, queryId, stagingPathRoot, schemaTableName));
        return declarationId;
    }

    public synchronized void dropDeclaredIntentionToWrite(String declarationId)
    {
        boolean removed = declaredIntentionsToWrite.removeIf(intention -> intention.declarationId().equals(declarationId));
        if (!removed) {
            throw new IllegalArgumentException("Declaration with id " + declarationId + " not found");
        }
    }

    public synchronized boolean isFinished()
    {
        return state == State.FINISHED;
    }

    public synchronized void commit()
    {
        try {
            switch (state) {
                case EMPTY -> {}
                case SHARED_OPERATION_BUFFERED -> commitShared();
                case EXCLUSIVE_OPERATION_BUFFERED -> bufferedExclusiveOperation.execute(delegate);
                case FINISHED -> throw new IllegalStateException("Tried to commit buffered metastore operations after transaction has been committed/aborted");
            }
        }
        finally {
            state = State.FINISHED;
        }
    }

    public synchronized void rollback()
    {
        try {
            switch (state) {
                case EMPTY, EXCLUSIVE_OPERATION_BUFFERED -> {}
                case SHARED_OPERATION_BUFFERED -> rollbackShared();
                case FINISHED -> throw new IllegalStateException("Tried to rollback buffered metastore operations after transaction has been committed/aborted");
            }
        }
        finally {
            state = State.FINISHED;
        }
    }

    public void checkSupportsHiveAcidTransactions()
    {
        delegate.checkSupportsTransactions();
    }

    public void beginQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();

        synchronized (this) {
            checkState(
                    currentQueryId.isEmpty() && hiveTransactionSupplier.isEmpty(),
                    "Query already begun: %s while starting query %s",
                    currentQueryId,
                    queryId);
            currentQueryId = Optional.of(queryId);

            hiveTransactionSupplier = Optional.of(() -> makeHiveTransaction(session, transactionId -> NO_ACID_TRANSACTION));
        }
    }

    public AcidTransaction beginInsert(ConnectorSession session, Table table)
    {
        return beginOperation(session, table, AcidOperation.INSERT);
    }

    public AcidTransaction beginMerge(ConnectorSession session, Table table)
    {
        return beginOperation(session, table, AcidOperation.MERGE);
    }

    private AcidTransaction beginOperation(ConnectorSession session, Table table, AcidOperation operation)
    {
        String queryId = session.getQueryId();

        synchronized (this) {
            currentQueryId = Optional.of(queryId);

            // We start the transaction immediately, and allocate the write lock and the writeId,
            // because we need the writeId to write the delta files.
            HiveTransaction hiveTransaction = makeHiveTransaction(session, transactionId -> {
                acquireTableWriteLock(
                        new AcidTransactionOwner(session.getUser()),
                        queryId,
                        transactionId,
                        table.getDatabaseName(),
                        table.getTableName(),
                        operation,
                        !table.getPartitionColumns().isEmpty());
                long writeId = allocateWriteId(table.getDatabaseName(), table.getTableName(), transactionId);
                return new AcidTransaction(operation, transactionId, writeId);
            });
            hiveTransactionSupplier = Optional.of(() -> hiveTransaction);
            currentHiveTransaction = Optional.of(hiveTransaction);
            return hiveTransaction.getTransaction();
        }
    }

    private HiveTransaction makeHiveTransaction(ConnectorSession session, Function<Long, AcidTransaction> transactionMaker)
    {
        String queryId = session.getQueryId();

        long heartbeatInterval = configuredTransactionHeartbeatInterval
                .map(Duration::toMillis)
                .orElseGet(this::getServerExpectedHeartbeatIntervalMillis);
        // TODO consider adding query id to the owner
        long transactionId = delegate.openTransaction(new AcidTransactionOwner(session.getUser()));
        log.debug("Using hive transaction %s for %s", transactionId, queryId);

        ScheduledFuture<?> heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(
                () -> delegate.sendTransactionHeartbeat(transactionId),
                0,
                heartbeatInterval,
                MILLISECONDS);

        AcidTransaction transaction = transactionMaker.apply(transactionId);
        return new HiveTransaction(queryId, transactionId, heartbeatTask, transaction);
    }

    private long getServerExpectedHeartbeatIntervalMillis()
    {
        String timeout = delegate.getConfigValue("metastore.txn.timeout").orElse("300s");
        return metastoreTimeToMillis(timeout) / 2;
    }

    private static final Pattern METASTORE_TIME = Pattern.compile("([0-9]+)([a-zA-Z]+)");

    // based on org.apache.hadoop.hive.metastore.conf.MetastoreConf#convertTimeStr
    private static long metastoreTimeToMillis(String value)
    {
        if (CharMatcher.inRange('0', '9').matches(value.charAt(value.length() - 1))) {
            return SECONDS.toMillis(parseLong(value));
        }

        Matcher matcher = METASTORE_TIME.matcher(value);
        checkArgument(matcher.matches(), "Invalid time unit: %s", value);

        long duration = parseLong(matcher.group(1));
        String unit = matcher.group(2).toLowerCase(ENGLISH);

        if (unit.equals("s") || unit.startsWith("sec")) {
            return SECONDS.toMillis(duration);
        }
        if (unit.equals("ms") || unit.startsWith("msec")) {
            return duration;
        }
        if (unit.equals("m") || unit.startsWith("min")) {
            return MINUTES.toMillis(duration);
        }
        if (unit.equals("us") || unit.startsWith("usec")) {
            return MICROSECONDS.toMillis(duration);
        }
        if (unit.equals("ns") || unit.startsWith("nsec")) {
            return NANOSECONDS.toMillis(duration);
        }
        if (unit.equals("h") || unit.startsWith("hour")) {
            return HOURS.toMillis(duration);
        }
        if (unit.equals("d") || unit.startsWith("day")) {
            return DAYS.toMillis(duration);
        }
        throw new IllegalArgumentException("Invalid time unit " + unit);
    }

    public Optional<ValidTxnWriteIdList> getValidWriteIds(ConnectorSession session, HiveTableHandle tableHandle)
    {
        HiveTransaction hiveTransaction;
        synchronized (this) {
            String queryId = session.getQueryId();
            checkState(currentQueryId.equals(Optional.of(queryId)), "Invalid query id %s while current query is %s", queryId, currentQueryId);
            if (!isTransactionalTable(tableHandle.getTableParameters().orElseThrow(() -> new IllegalStateException("tableParameters missing")))) {
                return Optional.empty();
            }
            if (currentHiveTransaction.isEmpty()) {
                currentHiveTransaction = Optional.of(hiveTransactionSupplier
                        .orElseThrow(() -> new IllegalStateException("hiveTransactionSupplier is not set"))
                        .get());
            }
            hiveTransaction = currentHiveTransaction.get();
        }
        return Optional.of(hiveTransaction.getValidWriteIds(new AcidTransactionOwner(session.getUser()), delegate, tableHandle));
    }

    public synchronized void cleanupQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();
        checkState(currentQueryId.equals(Optional.of(queryId)), "Invalid query id %s while current query is %s", queryId, currentQueryId);
        Optional<HiveTransaction> transaction = currentHiveTransaction;

        if (transaction.isEmpty()) {
            clearCurrentTransaction();
            return;
        }

        try {
            commit();
        }
        catch (Throwable commitFailure) {
            try {
                postCommitCleanup(transaction, false);
            }
            catch (Throwable cleanupFailure) {
                if (cleanupFailure != commitFailure) {
                    commitFailure.addSuppressed(cleanupFailure);
                }
            }
            throw commitFailure;
        }
        postCommitCleanup(transaction, true);
    }

    private void postCommitCleanup(Optional<HiveTransaction> transaction, boolean commit)
    {
        clearCurrentTransaction();
        long transactionId = transaction.orElseThrow().getTransactionId();
        ScheduledFuture<?> heartbeatTask = transaction.get().getHeartbeatTask();
        heartbeatTask.cancel(true);

        if (commit) {
            // Any failure around aborted transactions, etc. would be handled by Hive Metastore commit, and TrinoException will be thrown
            delegate.commitTransaction(transactionId);
        }
        else {
            delegate.abortTransaction(transactionId);
        }
    }

    private synchronized void clearCurrentTransaction()
    {
        currentQueryId = Optional.empty();
        currentHiveTransaction = Optional.empty();
        hiveTransactionSupplier = Optional.empty();
    }

    @GuardedBy("this")
    private void commitShared()
    {
        checkHoldsLock();

        AcidTransaction transaction = getOptionalAcidTransaction();

        Committer committer = new Committer(transaction);
        try {
            for (Entry<SchemaTableName, Action<TableAndMore>> entry : tableActions.entrySet()) {
                SchemaTableName schemaTableName = entry.getKey();
                Action<TableAndMore> action = entry.getValue();
                switch (action.type()) {
                    case DROP -> committer.prepareDropTable(schemaTableName);
                    case ALTER -> committer.prepareAlterTable(action.identity(), action.queryId(), action.data());
                    case ADD -> committer.prepareAddTable(action.identity(), action.queryId(), action.data());
                    case INSERT_EXISTING -> committer.prepareInsertExistingTable(action.identity(), action.queryId(), action.data());
                    case MERGE -> committer.prepareMergeExistingTable(action.identity(), action.data());
                    case DROP_PRESERVE_DATA -> throw new IllegalArgumentException("Unsupported action type: " + action.type());
                }
            }
            for (Entry<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> tableEntry : partitionActions.entrySet()) {
                SchemaTableName schemaTableName = tableEntry.getKey();
                for (Entry<List<String>, Action<PartitionAndMore>> partitionEntry : tableEntry.getValue().entrySet()) {
                    List<String> partitionValues = partitionEntry.getKey();
                    Action<PartitionAndMore> action = partitionEntry.getValue();
                    switch (action.type()) {
                        case DROP -> committer.prepareDropPartition(schemaTableName, partitionValues, true);
                        case DROP_PRESERVE_DATA -> committer.prepareDropPartition(schemaTableName, partitionValues, false);
                        case ALTER -> committer.prepareAlterPartition(action.identity(), action.queryId(), action.data());
                        case ADD -> committer.prepareAddPartition(action.identity(), action.queryId(), action.data());
                        case INSERT_EXISTING, MERGE -> committer.prepareInsertExistingPartition(action.identity(), action.queryId(), action.data());
                    }
                }
            }

            // Wait for all file system operations for "INSERT_EXISTING" and "ADD" action to finish
            committer.waitForAsyncFileSystemOperations();

            // At this point, all file system operations, whether asynchronously issued or not, have completed successfully.
            // We are moving on to metastore operations now.

            committer.executeAddTableOperations(transaction);
            committer.executeAlterTableOperations();
            committer.executeAlterPartitionOperations();
            committer.executeAddPartitionOperations(transaction);
            committer.executeUpdateStatisticsOperations(transaction);
        }
        catch (Throwable t) {
            log.warn("Rolling back due to metastore commit failure: %s", t.getMessage());
            try {
                committer.cancelUnstartedAsyncFileSystemOperations();
                committer.undoUpdateStatisticsOperations(transaction);
                committer.undoAddPartitionOperations();
                committer.undoAddTableOperations();

                committer.waitForAsyncFileSystemOperationSuppressThrowable();

                // fileSystemFutures must all come back before any file system cleanups are carried out.
                // Otherwise, files that should be deleted may be created after cleanup is done.
                committer.executeCleanupTasksForAbort(declaredIntentionsToWrite);

                committer.executeRenameTasksForAbort();

                // Partition directory must be put back before the relevant metastore operation can be undone
                committer.undoAlterTableOperations();
                committer.undoAlterPartitionOperations();

                rollbackShared();
            }
            catch (RuntimeException e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }

            throw t;
        }
        finally {
            committer.executeTableInvalidationCallback();
        }

        try {
            // After this line, operations are no longer reversible.
            // The next section will deal with "dropping table/partition". Commit may still fail in
            // this section. Even if the commit fails, cleanups, instead of rollbacks, will be executed.

            committer.executeIrreversibleMetastoreOperations();

            // If control flow reached this point, this commit is considered successful no matter
            // what happens later. The only operations that haven't been carried out yet
            // are cleanup operations.

            // The program control flow will go to finally next. And cleanup will run because
            // moveForwardInFinally has been set to false.
        }
        finally {
            // In this method, all operations are best-effort cleanup operations.
            // If any operation fails, the error will be logged and ignored.
            // Additionally, other cleanup operations should still be attempted.

            // Execute deletion tasks
            committer.executeDeletionTasksForFinish();

            // Clean up staging directories (that may recursively contain empty directories or stale files from failed attempts)
            committer.pruneAndDeleteStagingDirectories(declaredIntentionsToWrite);
        }
    }

    private class Committer
    {
        private final AtomicBoolean fileSystemOperationsCancelled = new AtomicBoolean(false);
        private final List<CompletableFuture<?>> fileSystemOperationFutures = new ArrayList<>();

        // File system
        // For file system changes, only operations outside the writing paths (as specified in declared intentions to write)
        // need to MOVE_BACKWARD tasks scheduled. Files in writing paths are handled by rollbackShared().
        private final List<DirectoryDeletionTask> deletionTasksForFinish = new ArrayList<>();
        private final List<DirectoryRenameTask> renameTasksForAbort = new ArrayList<>();
        private final Queue<DirectoryCleanUpTask> cleanUpTasksForAbort = new ConcurrentLinkedQueue<>();

        // Notify callback about changes on the schema tables / partitions
        private final Set<Table> tablesToInvalidate = new LinkedHashSet<>();
        private final Set<Partition> partitionsToInvalidate = new LinkedHashSet<>();

        // Metastore
        private final List<CreateTableOperation> addTableOperations = new ArrayList<>();
        private final List<AlterTableOperation> alterTableOperations = new ArrayList<>();
        private final Map<SchemaTableName, PartitionAdder> partitionAdders = new HashMap<>();
        private final List<AlterPartitionOperation> alterPartitionOperations = new ArrayList<>();
        private final List<UpdateStatisticsOperation> updateStatisticsOperations = new ArrayList<>();
        private final List<IrreversibleMetastoreOperation> metastoreDeleteOperations = new ArrayList<>();

        private final AcidTransaction transaction;

        // Flag for better error message
        private boolean deleteOnly = true;

        Committer(AcidTransaction transaction)
        {
            this.transaction = transaction;
        }

        private void prepareDropTable(SchemaTableName schemaTableName)
        {
            metastoreDeleteOperations.add(new IrreversibleMetastoreOperation(
                    format("drop table %s", schemaTableName),
                    () -> {
                        Optional<Table> droppedTable = delegate.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
                        try {
                            delegate.dropTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), true);
                        }
                        finally {
                            // perform explicit invalidation for the table in irreversible metastore operation
                            droppedTable.ifPresent(tableInvalidationCallback::invalidate);
                        }
                    }));
        }

        private void prepareAlterTable(ConnectorIdentity identity, String queryId, TableAndMore tableAndMore)
        {
            deleteOnly = false;

            Table table = tableAndMore.getTable();
            Location targetLocation = Location.of(table.getStorage().getLocation());
            Table oldTable = delegate.getTable(table.getDatabaseName(), table.getTableName())
                    .orElseThrow(() -> new TrinoException(TRANSACTION_CONFLICT, "The table that this transaction modified was deleted in another transaction. " + table.getSchemaTableName()));
            Location oldTableLocation = Location.of(oldTable.getStorage().getLocation());
            tablesToInvalidate.add(oldTable);

            cleanExtraOutputFiles(identity, queryId, tableAndMore);

            // The location of the old table and the new table can be different because we allow arbitrary directories through LocationService.
            // If the location of the old table is the same as the location of the new table:
            // * Rename the old data directory to a temporary path with a special suffix
            // * Remember we will need to delete that directory at the end if transaction successfully commits
            // * Remember we will need to undo the rename if transaction aborts
            // Otherwise,
            // * Remember we will need to delete the location of the old partition at the end if transaction successfully commits
            if (targetLocation.equals(oldTableLocation)) {
                Location location = asFileLocation(oldTableLocation);
                Location oldTableStagingPath = location.parentDirectory().appendPath("_temp_" + location.fileName() + "_" + queryId);
                renameDirectory(
                        fileSystemFactory.create(identity),
                        oldTableLocation,
                        oldTableStagingPath,
                        () -> renameTasksForAbort.add(new DirectoryRenameTask(identity, oldTableStagingPath, oldTableLocation)));
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(identity, oldTableStagingPath));
                }
            }
            else {
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(identity, oldTableLocation));
                }
            }

            Location currentLocation = tableAndMore.getCurrentLocation()
                    .orElseThrow(() -> new IllegalArgumentException("location should be present for alter table"));
            if (!targetLocation.equals(currentLocation)) {
                renameDirectory(
                        fileSystemFactory.create(identity),
                        currentLocation,
                        targetLocation,
                        () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetLocation, true)));
            }
            // Partition alter must happen regardless of whether the original and current location is the same
            // because metadata might change: e.g., storage format, column types, etc.
            alterTableOperations.add(new AlterTableOperation(tableAndMore.getTable(), oldTable, tableAndMore.getPrincipalPrivileges()));

            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    table.getSchemaTableName(),
                    Optional.empty(),
                    tableAndMore.getStatisticsUpdate(),
                    false));
        }

        private void prepareAddTable(ConnectorIdentity identity, String queryId, TableAndMore tableAndMore)
        {
            deleteOnly = false;

            cleanExtraOutputFiles(identity, queryId, tableAndMore);

            Table table = tableAndMore.getTable();
            if (table.getTableType().equals(MANAGED_TABLE.name())) {
                Optional<Location> targetLocation = table.getStorage().getOptionalLocation().map(Location::of);
                if (targetLocation.isPresent()) {
                    Optional<Location> currentLocation = tableAndMore.getCurrentLocation();
                    Location targetPath = targetLocation.get();
                    TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
                    if (table.getPartitionColumns().isEmpty() && currentLocation.isPresent()) {
                        // CREATE TABLE AS SELECT unpartitioned table
                        if (targetPath.equals(currentLocation.get())) {
                            // Target path and current path are the same. Therefore, directory move is not needed.
                        }
                        else {
                            renameDirectory(
                                    fileSystem,
                                    currentLocation.get(),
                                    targetPath,
                                    () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, true)));
                        }
                    }
                    else {
                        // CREATE TABLE AS SELECT partitioned table, or
                        // CREATE TABLE partitioned/unpartitioned table (without data)
                        if (directoryExists(fileSystem, targetPath)) {
                            if (currentLocation.isPresent() && currentLocation.get().equals(targetPath)) {
                                // It is okay to skip directory creation when currentLocation is equal to targetPath
                                // because the directory may have been created when creating partition directories.
                                // However, it is important to note that the two being equal does not guarantee
                                // a directory had been created.
                            }
                            else {
                                throw new TrinoException(
                                        HIVE_PATH_ALREADY_EXISTS,
                                        format("Unable to create directory %s: target directory already exists", targetPath));
                            }
                        }
                        else {
                            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, true));
                            createDirectory(fileSystem, targetPath);
                        }
                    }
                }
                // if targetLocation is not set in table, we assume HMS creates table directory
            }
            addTableOperations.add(new CreateTableOperation(table, tableAndMore.getPrincipalPrivileges(), tableAndMore.isIgnoreExisting(), tableAndMore.getStatisticsUpdate()));
        }

        private void prepareInsertExistingTable(ConnectorIdentity identity, String queryId, TableAndMore tableAndMore)
        {
            deleteOnly = false;
            Table table = tableAndMore.getTable();
            Location targetPath = Location.of(table.getStorage().getLocation());
            tablesToInvalidate.add(table);
            Location currentPath = tableAndMore.getCurrentLocation().orElseThrow();
            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, false));

            if (!targetPath.equals(currentPath)) {
                // if staging directory is used, we cherry-pick files to be moved
                TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
                asyncRename(fileSystem, fileSystemExecutor, fileSystemOperationsCancelled, fileSystemOperationFutures, currentPath, targetPath, tableAndMore.getFileNames().orElseThrow());
            }
            else {
                // if we inserted directly into table directory, we need to remove extra output files which should not be part of the table
                cleanExtraOutputFiles(identity, queryId, tableAndMore);
            }
            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    table.getSchemaTableName(),
                    Optional.empty(),
                    tableAndMore.getStatisticsUpdate(),
                    true));

            if (isAcidTransactionRunning()) {
                AcidTransaction transaction = getRequiredAcidTransaction();
                updateTableWriteId(table.getDatabaseName(), table.getTableName(), transaction.getAcidTransactionId(), transaction.getWriteId(), OptionalLong.empty());
            }
        }

        private void prepareMergeExistingTable(ConnectorIdentity identity, TableAndMore tableAndMore)
        {
            AcidTransaction transaction = getRequiredAcidTransaction();
            checkArgument(transaction.isMerge(), "transaction should be merge, but is %s", transaction);

            deleteOnly = false;
            Table table = tableAndMore.getTable();
            Location targetPath = Location.of(table.getStorage().getLocation());
            Location currentPath = tableAndMore.getCurrentLocation().orElseThrow();
            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, false));
            if (!targetPath.equals(currentPath)) {
                TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
                asyncRename(fileSystem, fileSystemExecutor, fileSystemOperationsCancelled, fileSystemOperationFutures, currentPath, targetPath, tableAndMore.getFileNames().orElseThrow());
            }
            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    table.getSchemaTableName(),
                    Optional.empty(),
                    tableAndMore.getStatisticsUpdate(),
                    true));

            updateTableWriteId(table.getDatabaseName(), table.getTableName(), transaction.getAcidTransactionId(), transaction.getWriteId(), OptionalLong.empty());
        }

        private void prepareDropPartition(SchemaTableName schemaTableName, List<String> partitionValues, boolean deleteData)
        {
            metastoreDeleteOperations.add(new IrreversibleMetastoreOperation(
                    format("drop partition %s.%s %s", schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionValues),
                    () -> {
                        Optional<Partition> droppedPartition = getOptionalPartition(delegate, schemaTableName, partitionValues);
                        try {
                            delegate.dropPartition(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionValues, deleteData);
                        }
                        finally {
                            // perform explicit invalidation for the partition in irreversible metastore operation
                            droppedPartition.ifPresent(tableInvalidationCallback::invalidate);
                        }
                    }));
        }

        private void prepareAlterPartition(ConnectorIdentity identity, String queryId, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.partition();
            partitionsToInvalidate.add(partition);
            String targetLocation = partition.getStorage().getLocation();
            Partition oldPartition = getOptionalPartition(delegate, partition.getSchemaTableName(), partition.getValues())
                    .orElseThrow(() -> new TrinoException(
                            TRANSACTION_CONFLICT,
                            format("The partition that this transaction modified was deleted in another transaction. %s %s", partition.getTableName(), partition.getValues())));
            String partitionName = getPartitionName(partition.getDatabaseName(), partition.getTableName(), partition.getValues());
            PartitionStatistics oldPartitionStatistics = getExistingPartitionStatistics(partition, partitionName);
            String oldPartitionLocation = oldPartition.getStorage().getLocation();
            Location oldPartitionPath = asFileLocation(Location.of(oldPartitionLocation));

            cleanExtraOutputFiles(identity, queryId, partitionAndMore);

            // The location of the old partition and the new partition can be different because we allow arbitrary directories through LocationService.
            // If the location of the old partition is the same as the location of the new partition:
            // * Rename the old data directory to a temporary path with a special suffix
            // * Remember we will need to delete that directory at the end if transaction successfully commits
            // * Remember we will need to undo the rename if transaction aborts
            // Otherwise,
            // * Remember we will need to delete the location of the old partition at the end if transaction successfully commits
            if (targetLocation.equals(oldPartitionLocation)) {
                Location oldPartitionStagingPath = oldPartitionPath.sibling("_temp_" + oldPartitionPath.fileName() + "_" + queryId);
                renameDirectory(
                        fileSystemFactory.create(identity),
                        oldPartitionPath,
                        oldPartitionStagingPath,
                        () -> renameTasksForAbort.add(new DirectoryRenameTask(identity, oldPartitionStagingPath, oldPartitionPath)));
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(identity, oldPartitionStagingPath));
                }
            }
            else {
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(identity, oldPartitionPath));
                }
            }

            Location currentPath = partitionAndMore.currentLocation();
            Location targetPath = Location.of(targetLocation);
            if (!targetPath.equals(currentPath)) {
                renameDirectory(
                        fileSystemFactory.create(identity),
                        currentPath,
                        targetPath,
                        () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, true)));
            }
            // Partition alter must happen regardless of whether the original and current location is the same
            // because metadata might change: e.g., storage format, column types, etc.
            alterPartitionOperations.add(new AlterPartitionOperation(
                    new PartitionWithStatistics(partition, partitionName, partitionAndMore.statisticsUpdate()),
                    new PartitionWithStatistics(oldPartition, partitionName, oldPartitionStatistics)));
        }

        private void cleanExtraOutputFiles(ConnectorIdentity identity, String queryId, PartitionAndMore partitionAndMore)
        {
            if (!partitionAndMore.cleanExtraOutputFilesOnCommit()) {
                return;
            }
            verify(partitionAndMore.hasFileNames(), "fileNames expected to be set if isCleanExtraOutputFilesOnCommit is true");

            TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
            SemiTransactionalHiveMetastore.cleanExtraOutputFiles(fileSystem, queryId, partitionAndMore.currentLocation(), ImmutableSet.copyOf(partitionAndMore.getFileNames()));
        }

        private void cleanExtraOutputFiles(ConnectorIdentity identity, String queryId, TableAndMore tableAndMore)
        {
            if (!tableAndMore.isCleanExtraOutputFilesOnCommit()) {
                return;
            }
            TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
            Location tableLocation = tableAndMore.getCurrentLocation().orElseThrow(() ->
                    new IllegalArgumentException("currentLocation expected to be set if isCleanExtraOutputFilesOnCommit is true"));
            List<String> files = tableAndMore.getFileNames().orElseThrow(() -> new IllegalArgumentException("fileNames expected to be set if isCleanExtraOutputFilesOnCommit is true"));
            SemiTransactionalHiveMetastore.cleanExtraOutputFiles(fileSystem, queryId, tableLocation, ImmutableSet.copyOf(files));
        }

        private PartitionStatistics getExistingPartitionStatistics(Partition partition, String partitionName)
        {
            try {
                HiveBasicStatistics basicStatistics = getHiveBasicStatistics(partition.getParameters());
                Map<String, HiveColumnStatistics> columnStatistics = delegate.getPartitionColumnStatistics(
                                partition.getDatabaseName(),
                                partition.getTableName(),
                                ImmutableSet.of(partitionName),
                                partition.getColumns().stream().map(Column::getName).collect(toImmutableSet()))
                        .get(partitionName);
                if (columnStatistics == null) {
                    throw new TrinoException(
                            TRANSACTION_CONFLICT,
                            format("The partition that this transaction modified was deleted in another transaction. %s %s", partition.getTableName(), partition.getValues()));
                }
                return new PartitionStatistics(basicStatistics, columnStatistics);
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode())) {
                    log.warn(
                            e,
                            "Corrupted statistics found when altering partition. Table: %s.%s. Partition: %s",
                            partition.getDatabaseName(),
                            partition.getTableName(),
                            partition.getValues());
                    return PartitionStatistics.empty();
                }
                throw e;
            }
        }

        private void prepareAddPartition(ConnectorIdentity identity, String queryId, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.partition();
            String targetLocation = partition.getStorage().getLocation();
            Location currentPath = partitionAndMore.currentLocation();
            Location targetPath = Location.of(targetLocation);

            cleanExtraOutputFiles(identity, queryId, partitionAndMore);

            PartitionAdder partitionAdder = partitionAdders.computeIfAbsent(
                    partition.getSchemaTableName(),
                    _ -> new PartitionAdder(partition.getDatabaseName(), partition.getTableName(), delegate, PARTITION_COMMIT_BATCH_SIZE));

            fileSystemOperationFutures.add(CompletableFuture.runAsync(() -> {
                if (fileSystemOperationsCancelled.get()) {
                    return;
                }
                TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
                if (directoryExists(fileSystem, currentPath)) {
                    if (!targetPath.equals(currentPath)) {
                        renameDirectory(
                                fileSystem,
                                currentPath,
                                targetPath,
                                () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, true)));
                    }
                }
                else {
                    cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, true));
                    createDirectory(fileSystem, targetPath);
                }
            }, fileSystemExecutor));

            String partitionName = getPartitionName(partition.getDatabaseName(), partition.getTableName(), partition.getValues());
            partitionAdder.addPartition(new PartitionWithStatistics(partition, partitionName, partitionAndMore.statisticsUpdate()));
        }

        private void prepareInsertExistingPartition(ConnectorIdentity identity, String queryId, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.partition();
            partitionsToInvalidate.add(partition);
            Location targetPath = Location.of(partition.getStorage().getLocation());
            Location currentPath = partitionAndMore.currentLocation();
            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(identity, targetPath, false));

            if (!targetPath.equals(currentPath)) {
                // if staging directory is used, we cherry-pick files to be moved
                TrinoFileSystem fileSystem = fileSystemFactory.create(identity);
                asyncRename(fileSystem, fileSystemExecutor, fileSystemOperationsCancelled, fileSystemOperationFutures, currentPath, targetPath, partitionAndMore.getFileNames());
            }
            else {
                // if we inserted directly into partition directory, we need to remove extra output files which should not be part of the table
                cleanExtraOutputFiles(identity, queryId, partitionAndMore);
            }

            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    partition.getSchemaTableName(),
                    Optional.of(getPartitionName(partition.getDatabaseName(), partition.getTableName(), partition.getValues())),
                    partitionAndMore.statisticsUpdate(),
                    true));
        }

        private void executeCleanupTasksForAbort(Collection<DeclaredIntentionToWrite> declaredIntentionsToWrite)
        {
            Set<String> queryIds = declaredIntentionsToWrite.stream()
                    .map(DeclaredIntentionToWrite::queryId)
                    .collect(toImmutableSet());
            for (DirectoryCleanUpTask cleanUpTask : cleanUpTasksForAbort) {
                recursiveDeleteFilesAndLog(cleanUpTask.identity(), cleanUpTask.location(), queryIds, cleanUpTask.deleteEmptyDirectory(), "temporary directory commit abort");
            }
        }

        private void executeDeletionTasksForFinish()
        {
            for (DirectoryDeletionTask deletionTask : deletionTasksForFinish) {
                TrinoFileSystem fileSystem = fileSystemFactory.create(deletionTask.identity());
                try {
                    fileSystem.deleteDirectory(deletionTask.location());
                }
                catch (IOException e) {
                    logCleanupFailure(e, "Error deleting directory: %s", deletionTask.location());
                }
            }
        }

        private void executeRenameTasksForAbort()
        {
            for (DirectoryRenameTask directoryRenameTask : renameTasksForAbort) {
                try {
                    // Ignore the task if the source directory doesn't exist.
                    // This is probably because the original rename that we are trying to undo here never succeeded.
                    TrinoFileSystem fileSystem = fileSystemFactory.create(directoryRenameTask.identity());
                    if (directoryExists(fileSystem, directoryRenameTask.renameFrom())) {
                        renameDirectory(fileSystem, directoryRenameTask.renameFrom(), directoryRenameTask.renameTo(), () -> {});
                    }
                }
                catch (Throwable throwable) {
                    logCleanupFailure(throwable, "failed to undo rename of partition directory: %s to %s", directoryRenameTask.renameFrom(), directoryRenameTask.renameTo());
                }
            }
        }

        private void pruneAndDeleteStagingDirectories(List<DeclaredIntentionToWrite> declaredIntentionsToWrite)
        {
            for (DeclaredIntentionToWrite declaredIntentionToWrite : declaredIntentionsToWrite) {
                if (declaredIntentionToWrite.mode() != WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY) {
                    continue;
                }

                Set<String> queryIds = declaredIntentionsToWrite.stream()
                        .map(DeclaredIntentionToWrite::queryId)
                        .collect(toImmutableSet());

                Location path = declaredIntentionToWrite.rootPath();
                recursiveDeleteFilesAndLog(declaredIntentionToWrite.identity(), path, queryIds, true, "staging directory cleanup");
            }
        }

        private void waitForAsyncFileSystemOperations()
        {
            for (CompletableFuture<?> future : fileSystemOperationFutures) {
                getFutureValue(future, TrinoException.class);
            }
        }

        private void waitForAsyncFileSystemOperationSuppressThrowable()
        {
            for (CompletableFuture<?> future : fileSystemOperationFutures) {
                try {
                    future.get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                catch (Throwable t) {
                    // ignore
                }
            }
        }

        private void cancelUnstartedAsyncFileSystemOperations()
        {
            fileSystemOperationsCancelled.set(true);
        }

        private void executeAddTableOperations(AcidTransaction transaction)
        {
            for (CreateTableOperation addTableOperation : addTableOperations) {
                addTableOperation.run(delegate, transaction);
            }
        }

        private void executeAlterTableOperations()
        {
            for (AlterTableOperation alterTableOperation : alterTableOperations) {
                alterTableOperation.run(delegate, transaction);
            }
        }

        private void executeAlterPartitionOperations()
        {
            for (AlterPartitionOperation alterPartitionOperation : alterPartitionOperations) {
                alterPartitionOperation.run(delegate);
            }
        }

        private void executeAddPartitionOperations(AcidTransaction transaction)
        {
            for (PartitionAdder partitionAdder : partitionAdders.values()) {
                partitionAdder.execute(transaction);
            }
        }

        private void executeUpdateStatisticsOperations(AcidTransaction transaction)
        {
            ImmutableList.Builder<CompletableFuture<?>> executeUpdateFutures = ImmutableList.builder();
            List<String> failedUpdateStatisticsOperationDescriptions = new ArrayList<>();
            List<Throwable> suppressedExceptions = new ArrayList<>();
            for (UpdateStatisticsOperation operation : updateStatisticsOperations) {
                executeUpdateFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        operation.run(delegate, transaction);
                    }
                    catch (Throwable t) {
                        synchronized (failedUpdateStatisticsOperationDescriptions) {
                            addSuppressedExceptions(suppressedExceptions, t, failedUpdateStatisticsOperationDescriptions, operation.getDescription());
                        }
                    }
                }, updateExecutor));
            }

            for (CompletableFuture<?> executeUpdateFuture : executeUpdateFutures.build()) {
                getFutureValue(executeUpdateFuture);
            }
            if (!suppressedExceptions.isEmpty()) {
                StringBuilder message = new StringBuilder();
                message.append("All operations other than the following update operations were completed: ");
                Joiner.on("; ").appendTo(message, failedUpdateStatisticsOperationDescriptions);
                TrinoException trinoException = new TrinoException(HIVE_METASTORE_ERROR, message.toString());
                suppressedExceptions.forEach(trinoException::addSuppressed);
                throw trinoException;
            }
        }

        private void executeTableInvalidationCallback()
        {
            tablesToInvalidate.forEach(tableInvalidationCallback::invalidate);
            partitionsToInvalidate.forEach(tableInvalidationCallback::invalidate);
        }

        private void undoAddPartitionOperations()
        {
            for (PartitionAdder partitionAdder : partitionAdders.values()) {
                List<List<String>> partitionsFailedToRollback = partitionAdder.rollback();
                if (!partitionsFailedToRollback.isEmpty()) {
                    logCleanupFailure("Failed to rollback: add_partition for partitions %s.%s %s",
                            partitionAdder.getSchemaName(),
                            partitionAdder.getTableName(),
                            partitionsFailedToRollback);
                }
            }
        }

        private void undoAddTableOperations()
        {
            for (CreateTableOperation addTableOperation : addTableOperations) {
                try {
                    addTableOperation.undo(delegate);
                }
                catch (Throwable throwable) {
                    logCleanupFailure(throwable, "failed to rollback: %s", addTableOperation.getDescription());
                }
            }
        }

        private void undoAlterTableOperations()
        {
            for (AlterTableOperation alterTableOperation : alterTableOperations) {
                try {
                    alterTableOperation.undo(delegate, transaction);
                }
                catch (Throwable throwable) {
                    logCleanupFailure(throwable, "failed to rollback: %s", alterTableOperation.getDescription());
                }
            }
        }

        private void undoAlterPartitionOperations()
        {
            for (AlterPartitionOperation alterPartitionOperation : alterPartitionOperations) {
                try {
                    alterPartitionOperation.undo(delegate);
                }
                catch (Throwable throwable) {
                    logCleanupFailure(throwable, "failed to rollback: %s", alterPartitionOperation.getDescription());
                }
            }
        }

        private void undoUpdateStatisticsOperations(AcidTransaction transaction)
        {
            ImmutableList.Builder<CompletableFuture<?>> undoUpdateFutures = ImmutableList.builder();
            for (UpdateStatisticsOperation operation : updateStatisticsOperations) {
                undoUpdateFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        operation.undo(delegate, transaction);
                    }
                    catch (Throwable throwable) {
                        logCleanupFailure(throwable, "failed to rollback: %s", operation.getDescription());
                    }
                }, updateExecutor));
            }
            for (CompletableFuture<?> undoUpdateFuture : undoUpdateFutures.build()) {
                getFutureValue(undoUpdateFuture);
            }
        }

        private void executeIrreversibleMetastoreOperations()
        {
            List<String> failedIrreversibleOperationDescriptions = new ArrayList<>();
            List<Throwable> suppressedExceptions = new ArrayList<>();
            AtomicBoolean anySucceeded = new AtomicBoolean(false);

            ImmutableList.Builder<CompletableFuture<?>> dropFutures = ImmutableList.builder();
            for (IrreversibleMetastoreOperation irreversibleMetastoreOperation : metastoreDeleteOperations) {
                dropFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        irreversibleMetastoreOperation.run();
                        anySucceeded.set(true);
                    }
                    catch (Throwable t) {
                        synchronized (failedIrreversibleOperationDescriptions) {
                            addSuppressedExceptions(suppressedExceptions, t, failedIrreversibleOperationDescriptions, irreversibleMetastoreOperation.description());
                        }
                    }
                }, dropExecutor));
            }

            for (CompletableFuture<?> dropFuture : dropFutures.build()) {
                // none of the futures should fail because all exceptions are being handled explicitly
                getFutureValue(dropFuture);
            }
            if (!suppressedExceptions.isEmpty()) {
                StringBuilder message = new StringBuilder();
                if (deleteOnly && !anySucceeded.get()) {
                    message.append("The following metastore delete operations failed: ");
                }
                else {
                    message.append("The transaction didn't commit cleanly. All operations other than the following delete operations were completed: ");
                }
                Joiner.on("; ").appendTo(message, failedIrreversibleOperationDescriptions);

                TrinoException trinoException = new TrinoException(HIVE_METASTORE_ERROR, message.toString());
                suppressedExceptions.forEach(trinoException::addSuppressed);
                throw trinoException;
            }
        }
    }

    private static Optional<Partition> getOptionalPartition(HiveMetastore metastore, SchemaTableName schemaTableName, List<String> partitionValues)
    {
        return metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .flatMap(table -> metastore.getPartition(table, partitionValues));
    }

    @GuardedBy("this")
    private void rollbackShared()
    {
        checkHoldsLock();

        for (DeclaredIntentionToWrite declaredIntentionToWrite : declaredIntentionsToWrite) {
            switch (declaredIntentionToWrite.mode()) {
                case STAGE_AND_MOVE_TO_TARGET_DIRECTORY, DIRECT_TO_TARGET_NEW_DIRECTORY -> {
                    // For STAGE_AND_MOVE_TO_TARGET_DIRECTORY, there is no need to clean up the target directory as
                    // it will only be written to during the commit call and the commit call cleans up after failures.
                    if ((declaredIntentionToWrite.mode() == DIRECT_TO_TARGET_NEW_DIRECTORY) && skipTargetCleanupOnRollback) {
                        break;
                    }

                    Location rootPath = declaredIntentionToWrite.rootPath();

                    // In the case of DIRECT_TO_TARGET_NEW_DIRECTORY, if the directory is not guaranteed to be unique
                    // for the query, it is possible that another query or compute engine may see the directory, wrote
                    // data to it, and exported it through metastore. Therefore, it may be argued that cleanup of staging
                    // directories must be carried out conservatively. To be safe, we only delete files that start or
                    // end with the query IDs in this transaction.
                    recursiveDeleteFilesAndLog(
                            declaredIntentionToWrite.identity(),
                            rootPath,
                            ImmutableSet.of(declaredIntentionToWrite.queryId()),
                            true,
                            format("staging/target_new directory rollback for table %s", declaredIntentionToWrite.schemaTableName()));
                }
                case DIRECT_TO_TARGET_EXISTING_DIRECTORY -> {
                    Set<Location> pathsToClean = new HashSet<>();

                    // Check the base directory of the declared intention
                    // * existing partition may also be in this directory
                    // * this is where new partitions are created
                    Location baseDirectory = declaredIntentionToWrite.rootPath();
                    pathsToClean.add(baseDirectory);

                    SchemaTableName schemaTableName = declaredIntentionToWrite.schemaTableName();
                    Optional<Table> table = delegate.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
                    if (table.isPresent()) {
                        // check every existing partition that is outside for the base directory
                        List<Column> partitionColumns = table.get().getPartitionColumns();
                        if (!partitionColumns.isEmpty()) {
                            List<String> partitionColumnNames = partitionColumns.stream()
                                    .map(Column::getName)
                                    .collect(toImmutableList());
                            List<String> partitionNames = getOptionalPartitions(
                                            schemaTableName.getSchemaName(),
                                            schemaTableName.getTableName(),
                                            partitionColumnNames,
                                            TupleDomain.all())
                                    .orElse(ImmutableList.of());
                            for (List<String> partitionNameBatch : Iterables.partition(partitionNames, 10)) {
                                Collection<Optional<Partition>> partitions = getOptionalPartitions(schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionNameBatch).values();
                                partitions.stream()
                                        .flatMap(Optional::stream)
                                        .map(partition -> partition.getStorage().getLocation())
                                        .filter(path -> !path.startsWith(baseDirectory.toString()))
                                        .map(Location::of)
                                        .forEach(pathsToClean::add);
                            }
                        }
                    }
                    else {
                        logCleanupFailure(
                                "Error rolling back write to table %s.%s. Data directory may contain temporary data. Table was dropped in another transaction.",
                                schemaTableName.getSchemaName(),
                                schemaTableName.getTableName());
                    }

                    // delete any file that starts or ends with the query ID
                    for (Location path : pathsToClean) {
                        // We cannot delete any of the directories here since we do not know who created them.
                        recursiveDeleteFilesAndLog(
                                declaredIntentionToWrite.identity(),
                                path,
                                ImmutableSet.of(declaredIntentionToWrite.queryId()),
                                false,
                                format("target_existing directory rollback for table %s", schemaTableName));
                    }
                }
            }
        }
    }

    @VisibleForTesting
    public synchronized void testOnlyCheckIsReadOnly()
    {
        if (state != State.EMPTY) {
            throw new AssertionError("Test did not commit or rollback");
        }
    }

    @GuardedBy("this")
    private synchronized void checkReadable()
    {
        checkHoldsLock();

        switch (state) {
            case EMPTY, SHARED_OPERATION_BUFFERED -> {}
            case EXCLUSIVE_OPERATION_BUFFERED -> throw new TrinoException(NOT_SUPPORTED, "Unsupported combination of operations in a single transaction");
            case FINISHED -> throw new IllegalStateException("Tried to access metastore after transaction has been committed/aborted");
        }
    }

    @GuardedBy("this")
    private synchronized void setShared()
    {
        checkHoldsLock();

        checkReadable();
        state = State.SHARED_OPERATION_BUFFERED;
    }

    @GuardedBy("this")
    private synchronized void setExclusive(ExclusiveOperation exclusiveOperation)
    {
        checkHoldsLock();

        if (state != State.EMPTY) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported combination of operations in a single transaction");
        }
        state = State.EXCLUSIVE_OPERATION_BUFFERED;
        bufferedExclusiveOperation = exclusiveOperation;
    }

    @GuardedBy("this")
    private void checkNoPartitionAction(String databaseName, String tableName)
    {
        checkHoldsLock();

        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.get(new SchemaTableName(databaseName, tableName));
        if (partitionActionsOfTable != null && !partitionActionsOfTable.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot make schema changes to a table/view with modified partitions in the same transaction");
        }
    }

    @FormatMethod
    private static void logCleanupFailure(String format, Object... args)
    {
        log.warn(format, args);
    }

    @FormatMethod
    private static void logCleanupFailure(Throwable t, String format, Object... args)
    {
        log.warn(t, format, args);
    }

    private static void addSuppressedExceptions(List<Throwable> suppressedExceptions, Throwable t, List<String> descriptions, String description)
    {
        descriptions.add(description);
        // A limit is needed to avoid having a huge exception object. 5 was chosen arbitrarily.
        if (suppressedExceptions.size() < 5) {
            suppressedExceptions.add(t);
        }
    }

    private static void asyncRename(
            TrinoFileSystem fileSystem,
            Executor executor,
            AtomicBoolean cancelled,
            List<CompletableFuture<?>> fileRenameFutures,
            Location currentPath,
            Location targetPath,
            List<String> fileNames)
    {
        for (String fileName : fileNames) {
            Location source = currentPath.appendPath(fileName);
            Location target = targetPath.appendPath(fileName);
            fileRenameFutures.add(CompletableFuture.runAsync(() -> {
                if (cancelled.get()) {
                    return;
                }
                try {
                    fileSystem.renameFile(source, target);
                }
                catch (IOException e) {
                    throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error moving data files from %s to final location %s", source, target), e);
                }
            }, executor));
        }
    }

    private void recursiveDeleteFilesAndLog(ConnectorIdentity identity, Location directory, Set<String> queryIds, boolean deleteEmptyDirectories, String reason)
    {
        RecursiveDeleteResult recursiveDeleteResult = recursiveDeleteFiles(
                fileSystemFactory.create(identity),
                directory,
                queryIds,
                deleteEmptyDirectories);
        if (!recursiveDeleteResult.notDeletedEligibleItems().isEmpty()) {
            logCleanupFailure(
                    "Error deleting directory %s for %s. Some eligible items cannot be deleted: %s.",
                    directory.toString(),
                    reason,
                    recursiveDeleteResult.notDeletedEligibleItems());
        }
        else if (deleteEmptyDirectories && !recursiveDeleteResult.directoryNoLongerExists()) {
            logCleanupFailure(
                    "Error deleting directory %s for %s. Cannot delete the directory.",
                    directory.toString(),
                    reason);
        }
    }

    /**
     * Attempt to recursively remove eligible files and/or directories in {@code directory}.
     * <p>
     * When {@code queryIds} is not present, all files (but not necessarily directories) will be
     * ineligible. If all files shall be deleted, you can use an empty string as {@code queryIds}.
     * <p>
     * When {@code deleteEmptySubDirectory} is true, any empty directory (including directories that
     * were originally empty, and directories that become empty after files prefixed or suffixed with
     * {@code queryIds} are deleted) will be eligible.
     * <p>
     * This method will not delete anything that's neither a directory nor a file.
     *
     * @param queryIds prefix or suffix of files that should be deleted
     * @param deleteEmptyDirectories whether empty directories should be deleted
     */
    private static RecursiveDeleteResult recursiveDeleteFiles(TrinoFileSystem fileSystem, Location directory, Set<String> queryIds, boolean deleteEmptyDirectories)
    {
        try {
            if (!fileSystem.directoryExists(directory).orElse(false)) {
                return new RecursiveDeleteResult(true, ImmutableList.of());
            }
        }
        catch (IOException e) {
            ImmutableList.Builder<String> notDeletedItems = ImmutableList.builder();
            notDeletedItems.add(directory.toString() + "/**");
            return new RecursiveDeleteResult(false, notDeletedItems.build());
        }

        return doRecursiveDeleteFiles(fileSystem, directory, queryIds, deleteEmptyDirectories);
    }

    private static RecursiveDeleteResult doRecursiveDeleteFiles(TrinoFileSystem fileSystem, Location directory, Set<String> queryIds, boolean deleteEmptyDirectories)
    {
        // don't delete hidden Trino directories use by FileHiveMetastore
        directory = asFileLocation(directory);
        if (directory.fileName().startsWith(".trino")) {
            return new RecursiveDeleteResult(false, ImmutableList.of());
        }

        // TODO: this lists recursively but only uses the first level
        List<Location> allFiles = new ArrayList<>();
        Set<Location> allDirectories;
        try {
            FileIterator iterator = fileSystem.listFiles(directory);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();
                String child = location.toString().substring(directory.toString().length());
                while (child.startsWith("/")) {
                    child = child.substring(1);
                }
                if (!child.contains("/")) {
                    allFiles.add(location);
                }
            }
            allDirectories = fileSystem.listDirectories(directory);
        }
        catch (IOException e) {
            ImmutableList.Builder<String> notDeletedItems = ImmutableList.builder();
            notDeletedItems.add(directory + "/**");
            return new RecursiveDeleteResult(false, notDeletedItems.build());
        }

        boolean allDescendentsDeleted = true;
        ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
        for (Location file : allFiles) {
            String fileName = file.fileName();
            boolean eligible = false;
            // don't delete hidden Trino directories use by FileHiveMetastore
            if (!fileName.startsWith(".trino")) {
                eligible = queryIds.stream().anyMatch(id -> isFileCreatedByQuery(fileName, id));
            }
            if (eligible) {
                if (!deleteFileIfExists(fileSystem, file)) {
                    allDescendentsDeleted = false;
                    notDeletedEligibleItems.add(file.toString());
                }
            }
            else {
                allDescendentsDeleted = false;
            }
        }
        for (Location file : allDirectories) {
            RecursiveDeleteResult subResult = doRecursiveDeleteFiles(fileSystem, file, queryIds, deleteEmptyDirectories);
            if (!subResult.directoryNoLongerExists()) {
                allDescendentsDeleted = false;
            }
            if (!subResult.notDeletedEligibleItems().isEmpty()) {
                notDeletedEligibleItems.addAll(subResult.notDeletedEligibleItems());
            }
        }
        // Unconditionally delete empty delta_ and delete_delta_ directories, because that's
        // what Hive does, and leaving them in place confuses delta file readers.
        if (allDescendentsDeleted && (deleteEmptyDirectories || isDeltaDirectory(directory))) {
            verify(notDeletedEligibleItems.build().isEmpty());
            if (!deleteEmptyDirectoryIfExists(fileSystem, directory)) {
                return new RecursiveDeleteResult(false, ImmutableList.of(directory + "/"));
            }
            return new RecursiveDeleteResult(true, ImmutableList.of());
        }
        return new RecursiveDeleteResult(false, notDeletedEligibleItems.build());
    }

    private static boolean isDeltaDirectory(Location directory)
    {
        return DELTA_DIRECTORY_MATCHER.matcher(asFileLocation(directory).fileName()).matches();
    }

    private static boolean deleteFileIfExists(TrinoFileSystem fileSystem, Location location)
    {
        try {
            fileSystem.deleteFile(location);
            return true;
        }
        catch (FileNotFoundException e) {
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    private static boolean deleteEmptyDirectoryIfExists(TrinoFileSystem fileSystem, Location location)
    {
        try {
            if (fileSystem.listFiles(location).hasNext()) {
                log.warn("Not deleting non-empty directory: %s", location);
                return false;
            }
            fileSystem.deleteDirectory(location);
            return true;
        }
        catch (IOException e) {
            try {
                return !fileSystem.directoryExists(location).orElse(false);
            }
            catch (IOException ex) {
                return false;
            }
        }
    }

    private static void renameDirectory(TrinoFileSystem fileSystem, Location source, Location target, Runnable runWhenPathDoesntExist)
    {
        if (directoryExists(fileSystem, target)) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format("Unable to rename from %s to %s: target directory already exists", source, target));
        }

        Location parent = asFileLocation(target).parentDirectory();
        if (!directoryExists(fileSystem, parent)) {
            createDirectory(fileSystem, parent);
        }

        // The runnable will assume that if rename fails, it will be okay to delete the directory (if the directory is empty).
        // This is not technically true because a race condition still exists.
        runWhenPathDoesntExist.run();

        try {
            fileSystem.renameDirectory(source, target);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Failed to rename %s to %s", source, target), e);
        }
    }

    private static void createDirectory(TrinoFileSystem fileSystem, Location directory)
    {
        try {
            fileSystem.createDirectory(directory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    private static boolean directoryExists(TrinoFileSystem fileSystem, Location directory)
    {
        try {
            return fileSystem.directoryExists(directory).orElse(false);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e);
        }
    }

    private static Optional<String> getQueryId(Database database)
    {
        return Optional.ofNullable(database.getParameters().get(TRINO_QUERY_ID_NAME));
    }

    private static Optional<String> getQueryId(Table table)
    {
        return Optional.ofNullable(table.getParameters().get(TRINO_QUERY_ID_NAME));
    }

    private static Optional<String> getQueryId(Partition partition)
    {
        return Optional.ofNullable(partition.getParameters().get(TRINO_QUERY_ID_NAME));
    }

    private static Location asFileLocation(Location location)
    {
        // TODO: this is to work around the file-only restriction of Location methods
        String value = location.toString();
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return Location.of(value);
    }

    private void checkHoldsLock()
    {
        // This method serves a similar purpose at runtime as GuardedBy on method serves during static analysis.
        // This method should not have a significant performance impact. If it does, it may be reasonably to remove this method.
        // This intentionally does not use checkState.
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    private enum State
    {
        EMPTY,
        SHARED_OPERATION_BUFFERED,
        EXCLUSIVE_OPERATION_BUFFERED,
        FINISHED,
    }

    private enum ActionType
    {
        DROP,
        DROP_PRESERVE_DATA,
        ADD,
        ALTER,
        INSERT_EXISTING,
        MERGE,
    }

    private enum TableSource
    {
        CREATED_IN_THIS_TRANSACTION,
        PRE_EXISTING_TABLE,
        // RECREATED_IN_THIS_TRANSACTION is a possible case, but it is not supported with the current implementation
    }

    private record Action<T>(ActionType type, T data, ConnectorIdentity identity, String queryId)
    {
        private Action
        {
            requireNonNull(type, "type is null");
            if (type == ActionType.DROP || type == ActionType.DROP_PRESERVE_DATA) {
                checkArgument(data == null, "data is not null");
            }
            else {
                requireNonNull(data, "data is null");
            }
            requireNonNull(identity, "identity is null");
            requireNonNull(queryId, "queryId is null");
        }

        @Override
        public T data()
        {
            checkState(type != ActionType.DROP);
            return data;
        }
    }

    private static class TableAndMore
    {
        private final Table table;
        private final Optional<PrincipalPrivileges> principalPrivileges;
        private final Optional<Location> currentLocation; // unpartitioned table only
        private final Optional<List<String>> fileNames;
        private final boolean ignoreExisting;
        private final PartitionStatistics statistics;
        private final PartitionStatistics statisticsUpdate;
        private final boolean cleanExtraOutputFilesOnCommit;

        public TableAndMore(
                Table table,
                Optional<PrincipalPrivileges> principalPrivileges,
                Optional<Location> currentLocation,
                Optional<List<String>> fileNames,
                boolean ignoreExisting,
                PartitionStatistics statistics,
                PartitionStatistics statisticsUpdate,
                boolean cleanExtraOutputFilesOnCommit)
        {
            this.table = requireNonNull(table, "table is null");
            this.principalPrivileges = requireNonNull(principalPrivileges, "principalPrivileges is null");
            this.currentLocation = requireNonNull(currentLocation, "currentLocation is null");
            this.fileNames = requireNonNull(fileNames, "fileNames is null");
            this.ignoreExisting = ignoreExisting;
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.statisticsUpdate = requireNonNull(statisticsUpdate, "statisticsUpdate is null");
            this.cleanExtraOutputFilesOnCommit = cleanExtraOutputFilesOnCommit;

            checkArgument(!table.getStorage().getOptionalLocation().orElse("").isEmpty() || currentLocation.isEmpty(), "currentLocation cannot be supplied for table without location");
            checkArgument(fileNames.isEmpty() || currentLocation.isPresent(), "fileNames can be supplied only when currentLocation is supplied");
        }

        public boolean isIgnoreExisting()
        {
            return ignoreExisting;
        }

        public Table getTable()
        {
            return table;
        }

        public PrincipalPrivileges getPrincipalPrivileges()
        {
            checkState(principalPrivileges.isPresent());
            return principalPrivileges.get();
        }

        public Optional<Location> getCurrentLocation()
        {
            return currentLocation;
        }

        public Optional<List<String>> getFileNames()
        {
            return fileNames;
        }

        public PartitionStatistics getStatistics()
        {
            return statistics;
        }

        public PartitionStatistics getStatisticsUpdate()
        {
            return statisticsUpdate;
        }

        public boolean isCleanExtraOutputFilesOnCommit()
        {
            return cleanExtraOutputFilesOnCommit;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table", table)
                    .add("principalPrivileges", principalPrivileges)
                    .add("currentLocation", currentLocation)
                    .add("fileNames", fileNames)
                    .add("ignoreExisting", ignoreExisting)
                    .add("statistics", statistics)
                    .add("statisticsUpdate", statisticsUpdate)
                    .add("cleanExtraOutputFilesOnCommit", cleanExtraOutputFilesOnCommit)
                    .toString();
        }
    }

    private static class TableAndMergeResults
            extends TableAndMore
    {
        private final List<PartitionUpdateAndMergeResults> partitionMergeResults;

        public TableAndMergeResults(Table table, Optional<PrincipalPrivileges> principalPrivileges, Optional<Location> currentLocation, List<PartitionUpdateAndMergeResults> partitionMergeResults)
        {
            super(table, principalPrivileges, currentLocation, Optional.empty(), false, PartitionStatistics.empty(), PartitionStatistics.empty(), false); // retries are not supported for transactional tables
            this.partitionMergeResults = requireNonNull(partitionMergeResults, "partitionMergeResults is null");
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table", getTable())
                    .add("partitionMergeResults", partitionMergeResults)
                    .add("principalPrivileges", getPrincipalPrivileges())
                    .add("currentLocation", getCurrentLocation())
                    .toString();
        }
    }

    private record PartitionAndMore(
            Partition partition,
            Location currentLocation,
            Optional<List<String>> fileNames,
            PartitionStatistics statistics,
            PartitionStatistics statisticsUpdate,
            boolean cleanExtraOutputFilesOnCommit)
    {
        private PartitionAndMore
        {
            requireNonNull(partition, "partition is null");
            requireNonNull(currentLocation, "currentLocation is null");
            requireNonNull(fileNames, "fileNames is null");
            requireNonNull(statistics, "statistics is null");
            requireNonNull(statisticsUpdate, "statisticsUpdate is null");
        }

        public List<String> getFileNames()
        {
            checkState(fileNames.isPresent());
            return fileNames.get();
        }

        public boolean hasFileNames()
        {
            return fileNames.isPresent();
        }

        public Partition getAugmentedPartitionForInTransactionRead()
        {
            // This method augments the location field of the partition to the staging location.
            // This way, if the partition is accessed in an ongoing transaction, staged data
            // can be found and accessed.
            Partition partition = this.partition;
            String currentLocation = this.currentLocation.toString();
            if (!currentLocation.equals(partition.getStorage().getLocation())) {
                partition = Partition.builder(partition)
                        .withStorage(storage -> storage.setLocation(currentLocation))
                        .build();
            }
            return partition;
        }
    }

    public record DeclaredIntentionToWrite(String declarationId, WriteMode mode, ConnectorIdentity identity, String queryId, Location rootPath, SchemaTableName schemaTableName)
    {
        public DeclaredIntentionToWrite
        {
            requireNonNull(declarationId, "declarationId is null");
            requireNonNull(mode, "mode is null");
            requireNonNull(identity, "identity is null");
            requireNonNull(queryId, "queryId is null");
            requireNonNull(rootPath, "rootPath is null");
            requireNonNull(schemaTableName, "schemaTableName is null");
        }
    }

    private record DirectoryCleanUpTask(ConnectorIdentity identity, Location location, boolean deleteEmptyDirectory)
    {
        private DirectoryCleanUpTask
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(location, "location is null");
        }
    }

    private record DirectoryDeletionTask(ConnectorIdentity identity, Location location)
    {
        private DirectoryDeletionTask
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(location, "location is null");
        }
    }

    private record DirectoryRenameTask(ConnectorIdentity identity, Location renameFrom, Location renameTo)
    {
        private DirectoryRenameTask
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(renameFrom, "renameFrom is null");
            requireNonNull(renameTo, "renameTo is null");
        }
    }

    private record IrreversibleMetastoreOperation(String description, Runnable action)
    {
        private IrreversibleMetastoreOperation
        {
            requireNonNull(description, "description is null");
            requireNonNull(action, "action is null");
        }

        public void run()
        {
            action.run();
        }
    }

    private static class CreateTableOperation
    {
        private final Table newTable;
        private final PrincipalPrivileges privileges;
        private boolean tableCreated;
        private final boolean ignoreExisting;
        private final PartitionStatistics statistics;
        private final String queryId;

        public CreateTableOperation(Table newTable, PrincipalPrivileges privileges, boolean ignoreExisting, PartitionStatistics statistics)
        {
            requireNonNull(newTable, "newTable is null");
            this.newTable = newTable;
            this.privileges = requireNonNull(privileges, "privileges is null");
            this.ignoreExisting = ignoreExisting;
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.queryId = getQueryId(newTable).orElseThrow(() -> new IllegalArgumentException("Query id is not present"));
        }

        public String getDescription()
        {
            return format("add table %s.%s", newTable.getDatabaseName(), newTable.getTableName());
        }

        public void run(HiveMetastore metastore, AcidTransaction transaction)
        {
            boolean created = false;
            try {
                metastore.createTable(newTable, privileges);
                created = true;
            }
            catch (RuntimeException e) {
                RuntimeException failure = e;
                try {
                    Optional<Table> existingTable = metastore.getTable(newTable.getDatabaseName(), newTable.getTableName());
                    if (existingTable.isPresent()) {
                        Table table = existingTable.get();
                        Optional<String> existingTableQueryId = getQueryId(table);
                        if (existingTableQueryId.isPresent() && existingTableQueryId.get().equals(queryId)) {
                            // ignore table if it was already created by the same query during retries
                            failure = null;
                            created = true;
                        }
                        else {
                            // If the table definition in the metastore is different from what this tx wants to create,
                            // then there is a conflict (e.g., current tx wants to create T(a: bigint),
                            // but another tx already created T(a: varchar)).
                            // This may be a problem if there is an insert after this step.
                            if (!hasTheSameSchema(newTable, table)) {
                                // produce an understandable error message
                                failure = new TrinoException(TRANSACTION_CONFLICT, format("Table already exists with a different schema: '%s'", newTable.getTableName()));
                            }
                            else if (ignoreExisting) {
                                // if the statement is "CREATE TABLE IF NOT EXISTS", then ignore the exception
                                failure = null;
                            }
                        }
                    }
                }
                catch (RuntimeException _) {
                    // When table could not be fetched from metastore, it is not known whether the table was added.
                    // Deleting the table when aborting commit has the risk of deleting a table not added in this transaction.
                    // Not deleting the table may leave garbage behind. The former is much more dangerous than the latter.
                    // Therefore, the table is not considered added.
                }

                if (failure != null) {
                    throw failure;
                }
            }
            tableCreated = true;

            if (created && !isTrinoView(newTable) && !isTrinoMaterializedView(newTable)) {
                metastore.updateTableStatistics(newTable.getDatabaseName(), newTable.getTableName(), transaction.getOptionalWriteId(), OVERWRITE_ALL, statistics);
            }
        }

        private static boolean hasTheSameSchema(Table newTable, Table existingTable)
        {
            List<Column> newTableColumns = newTable.getDataColumns();
            List<Column> existingTableColumns = existingTable.getDataColumns();

            if (newTableColumns.size() != existingTableColumns.size()) {
                return false;
            }

            for (Column existingColumn : existingTableColumns) {
                if (newTableColumns.stream()
                        .noneMatch(newColumn -> newColumn.getName().equals(existingColumn.getName())
                                && newColumn.getType().equals(existingColumn.getType()))) {
                    return false;
                }
            }
            return true;
        }

        public void undo(HiveMetastore metastore)
        {
            if (!tableCreated) {
                return;
            }
            metastore.dropTable(newTable.getDatabaseName(), newTable.getTableName(), false);
        }
    }

    private static class AlterTableOperation
    {
        private final Table newTable;
        private final Table oldTable;
        private final PrincipalPrivileges principalPrivileges;
        private boolean undo;

        public AlterTableOperation(Table newTable, Table oldTable, PrincipalPrivileges principalPrivileges)
        {
            this.newTable = requireNonNull(newTable, "newTable is null");
            this.oldTable = requireNonNull(oldTable, "oldTable is null");
            this.principalPrivileges = requireNonNull(principalPrivileges, "principalPrivileges is null");
            checkArgument(newTable.getDatabaseName().equals(oldTable.getDatabaseName()));
            checkArgument(newTable.getTableName().equals(oldTable.getTableName()));
        }

        public String getDescription()
        {
            return format(
                    "alter table %s.%s",
                    newTable.getDatabaseName(),
                    newTable.getTableName());
        }

        public void run(HiveMetastore metastore, AcidTransaction transaction)
        {
            undo = true;
            if (transaction.isTransactional()) {
                metastore.alterTransactionalTable(newTable, transaction.getAcidTransactionId(), transaction.getWriteId(), principalPrivileges);
            }
            else {
                metastore.replaceTable(newTable.getDatabaseName(), newTable.getTableName(), newTable, principalPrivileges);
            }
        }

        public void undo(HiveMetastore metastore, AcidTransaction transaction)
        {
            if (!undo) {
                return;
            }

            if (transaction.isTransactional()) {
                metastore.alterTransactionalTable(oldTable, transaction.getAcidTransactionId(), transaction.getWriteId(), principalPrivileges);
            }
            else {
                metastore.replaceTable(oldTable.getDatabaseName(), oldTable.getTableName(), oldTable, principalPrivileges);
            }
        }
    }

    private static class AlterPartitionOperation
    {
        private final PartitionWithStatistics newPartition;
        private final PartitionWithStatistics oldPartition;
        private boolean undo;

        public AlterPartitionOperation(PartitionWithStatistics newPartition, PartitionWithStatistics oldPartition)
        {
            this.newPartition = requireNonNull(newPartition, "newPartition is null");
            this.oldPartition = requireNonNull(oldPartition, "oldPartition is null");
            checkArgument(newPartition.getPartition().getDatabaseName().equals(oldPartition.getPartition().getDatabaseName()));
            checkArgument(newPartition.getPartition().getTableName().equals(oldPartition.getPartition().getTableName()));
            checkArgument(newPartition.getPartition().getValues().equals(oldPartition.getPartition().getValues()));
        }

        public String getDescription()
        {
            return format(
                    "alter partition %s.%s %s",
                    newPartition.getPartition().getDatabaseName(),
                    newPartition.getPartition().getTableName(),
                    newPartition.getPartition().getValues());
        }

        public void run(HiveMetastore metastore)
        {
            undo = true;
            metastore.alterPartition(newPartition.getPartition().getDatabaseName(), newPartition.getPartition().getTableName(), newPartition);
        }

        public void undo(HiveMetastore metastore)
        {
            if (!undo) {
                return;
            }
            metastore.alterPartition(oldPartition.getPartition().getDatabaseName(), oldPartition.getPartition().getTableName(), oldPartition);
        }
    }

    private static class UpdateStatisticsOperation
    {
        private final SchemaTableName tableName;
        private final Optional<String> partitionName;
        private final PartitionStatistics statistics;
        private final boolean merge;

        private boolean done;

        public UpdateStatisticsOperation(SchemaTableName tableName, Optional<String> partitionName, PartitionStatistics statistics, boolean merge)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.partitionName = requireNonNull(partitionName, "partitionName is null");
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HiveMetastore metastore, AcidTransaction transaction)
        {
            StatisticsUpdateMode mode = merge ? MERGE_INCREMENTAL : OVERWRITE_ALL;
            if (partitionName.isPresent()) {
                metastore.updatePartitionStatistics(
                        metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                                .orElseThrow(() -> new TableNotFoundException(tableName)),
                        mode,
                        ImmutableMap.of(partitionName.get(), statistics));
            }
            else {
                metastore.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), transaction.getOptionalWriteId(), mode, statistics);
            }
            done = true;
        }

        public void undo(HiveMetastore metastore, AcidTransaction transaction)
        {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                metastore.updatePartitionStatistics(
                        metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                                .orElseThrow(() -> new TableNotFoundException(tableName)),
                        UNDO_MERGE_INCREMENTAL,
                        ImmutableMap.of(partitionName.get(), statistics));
            }
            else {
                metastore.updateTableStatistics(tableName.getSchemaName(), tableName.getTableName(), transaction.getOptionalWriteId(), UNDO_MERGE_INCREMENTAL, statistics);
            }
        }

        public String getDescription()
        {
            if (partitionName.isPresent()) {
                return format("replace partition parameters %s %s", tableName, partitionName.get());
            }
            return format("replace table parameters %s", tableName);
        }
    }

    private static class PartitionAdder
    {
        private final String schemaName;
        private final String tableName;
        private final HiveMetastore metastore;
        private final int batchSize;
        private final List<PartitionWithStatistics> partitions;
        private List<List<String>> createdPartitionValues = new ArrayList<>();

        public PartitionAdder(String schemaName, String tableName, HiveMetastore metastore, int batchSize)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.metastore = metastore;
            this.batchSize = batchSize;
            this.partitions = new ArrayList<>(batchSize);
        }

        public String getSchemaName()
        {
            return schemaName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public void addPartition(PartitionWithStatistics partition)
        {
            checkArgument(getQueryId(partition.getPartition()).isPresent());
            partitions.add(partition);
        }

        public void execute(AcidTransaction transaction)
        {
            List<List<PartitionWithStatistics>> batchedPartitions = Lists.partition(partitions, batchSize);
            for (List<PartitionWithStatistics> batch : batchedPartitions) {
                try {
                    metastore.addPartitions(schemaName, tableName, batch);
                    for (PartitionWithStatistics partition : batch) {
                        createdPartitionValues.add(partition.getPartition().getValues());
                    }
                }
                catch (Throwable t) {
                    // Add partition to the created list conservatively.
                    // Some metastore implementations are known to violate the "all or none" guarantee for add_partitions call.
                    boolean batchCompletelyAdded = true;
                    for (PartitionWithStatistics partition : batch) {
                        try {
                            Optional<Partition> remotePartition = getOptionalPartition(metastore, new SchemaTableName(schemaName, tableName), partition.getPartition().getValues());
                            // getQueryId(partition) is guaranteed to be non-empty. It is asserted in PartitionAdder.addPartition.
                            if (remotePartition.isPresent() && getQueryId(remotePartition.get()).equals(getQueryId(partition.getPartition()))) {
                                createdPartitionValues.add(partition.getPartition().getValues());
                            }
                            else {
                                batchCompletelyAdded = false;
                            }
                        }
                        catch (Throwable _) {
                            // When partition could not be fetched from metastore, it is not known whether the partition was added.
                            // Deleting the partition when aborting commit has the risk of deleting partition not added in this transaction.
                            // Not deleting the partition may leave garbage behind. The former is much more dangerous than the latter.
                            // Therefore, the partition is not added to the createdPartitionValues list here.
                            batchCompletelyAdded = false;
                        }
                    }
                    // If all the partitions were added successfully, the add_partition operation was actually successful.
                    // For some reason, it threw an exception (communication failure, retry failure after communication failure, etc.).
                    // But we would consider it successful regardless.
                    if (!batchCompletelyAdded) {
                        if (t instanceof TableNotFoundException) {
                            throw new TrinoException(HIVE_TABLE_DROPPED_DURING_QUERY, t);
                        }
                        throw t;
                    }
                }
            }
            if (transaction.isAcidTransactionRunning()) {
                List<String> partitionNames = partitions.stream().map(PartitionWithStatistics::getPartitionName).toList();
                metastore.addDynamicPartitions(schemaName, tableName, partitionNames, transaction.getAcidTransactionId(), transaction.getWriteId(), transaction.getOperation());
            }
            partitions.clear();
        }

        public List<List<String>> rollback()
        {
            // drop created partitions
            List<List<String>> partitionsFailedToRollback = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    metastore.dropPartition(schemaName, tableName, createdPartitionValue, false);
                }
                catch (PartitionNotFoundException e) {
                    // Maybe someone deleted the partition we added.
                    // Anyway, we are good because the partition is not there anymore.
                }
                catch (Throwable t) {
                    partitionsFailedToRollback.add(createdPartitionValue);
                }
            }
            createdPartitionValues = partitionsFailedToRollback;
            return partitionsFailedToRollback;
        }
    }

    private record RecursiveDeleteResult(boolean directoryNoLongerExists, List<String> notDeletedEligibleItems) {}

    private interface ExclusiveOperation
    {
        void execute(HiveMetastore delegate);
    }

    private long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(dbName, tableName, transactionId);
    }

    private void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            AcidOperation operation,
            boolean isPartitioned)
    {
        delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isPartitioned);
    }

    private void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    public static void cleanExtraOutputFiles(TrinoFileSystem fileSystem, String queryId, Location path, Set<String> filesToKeep)
    {
        List<Location> filesToDelete = new ArrayList<>();
        try {
            Failsafe.with(DELETE_RETRY_POLICY).run(() -> {
                log.debug("Deleting failed attempt files from %s for query %s", path, queryId);

                filesToDelete.clear();
                FileIterator iterator = fileSystem.listFiles(path);
                while (iterator.hasNext()) {
                    Location file = iterator.next().location();
                    if (isFileCreatedByQuery(file.fileName(), queryId) && !filesToKeep.contains(file.fileName())) {
                        filesToDelete.add(file);
                    }
                }

                log.debug("Found %s failed attempt file(s) to delete for query %s", filesToDelete.size(), queryId);
                fileSystem.deleteFiles(filesToDelete);
            });
        }
        catch (FailsafeException e) {
            // If we fail here, the query will be rolled back. The optimal outcome would be for rollback to complete successfully and clean up everything for the query.
            // Yet if we have a problem here, probably rollback will also fail.
            //
            // Thrown exception is listing files which we could not delete. So the user can clean up those later manually.
            // Note it is not a bullet-proof solution.
            // The rollback routine will still fire and try to clean up any changes the query made. It will clean up some, and probably leave some behind.
            //
            // Still, we cannot do much better for non-transactional Hive tables.
            throw new TrinoException(
                    HIVE_FILESYSTEM_ERROR,
                    format("Error deleting failed retry attempt files from %s; remaining files %s; manual cleanup may be required", path, filesToDelete),
                    e);
        }
    }

    public record PartitionUpdateInfo(List<String> partitionValues, Location currentLocation, List<String> fileNames, PartitionStatistics statisticsUpdate)
    {
        public PartitionUpdateInfo
        {
            requireNonNull(partitionValues, "partitionValues is null");
            requireNonNull(currentLocation, "currentLocation is null");
            requireNonNull(fileNames, "fileNames is null");
            requireNonNull(statisticsUpdate, "statisticsUpdate is null");
        }
    }
}
