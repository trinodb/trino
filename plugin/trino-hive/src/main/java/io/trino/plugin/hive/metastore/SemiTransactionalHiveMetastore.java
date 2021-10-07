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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveUpdateProcessor;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.PartitionAndStatementId;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.security.SqlStandardAccessControlMetadataMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_DROPPED_DURING_QUERY;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.NUM_ROWS;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.plugin.hive.util.HiveWriteUtils.createDirectory;
import static io.trino.plugin.hive.util.HiveWriteUtils.isFileCreatedByQuery;
import static io.trino.plugin.hive.util.HiveWriteUtils.pathExists;
import static io.trino.plugin.hive.util.Statistics.ReduceOperator.SUBTRACT;
import static io.trino.plugin.hive.util.Statistics.merge;
import static io.trino.plugin.hive.util.Statistics.reduce;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.TXN_TIMEOUT;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getTimeVar;

public class SemiTransactionalHiveMetastore
        implements SqlStandardAccessControlMetadataMetastore
{
    private static final Logger log = Logger.get(SemiTransactionalHiveMetastore.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 8;
    private static final Pattern DELTA_DIRECTORY_MATCHER = Pattern.compile("(delete_)?delta_[\\d]+_[\\d]+_[\\d]+$");

    private final HiveMetastoreClosure delegate;
    private final HdfsEnvironment hdfsEnvironment;
    private final Executor renameExecutor;
    private final Executor dropExecutor;
    private final Executor updateExecutor;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final ScheduledExecutorService heartbeatExecutor;
    private final Optional<Duration> configuredTransactionHeartbeatInterval;

    private boolean throwOnCleanupFailure;

    @GuardedBy("this")
    private final Map<SchemaTableName, Action<TableAndMore>> tableActions = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> partitionActions = new HashMap<>();
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
            HdfsEnvironment hdfsEnvironment,
            HiveMetastoreClosure delegate,
            Executor renameExecutor,
            Executor dropExecutor,
            Executor updateExecutor,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            ScheduledExecutorService heartbeatService)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.renameExecutor = requireNonNull(renameExecutor, "renameExecutor is null");
        this.dropExecutor = requireNonNull(dropExecutor, "dropExecutor is null");
        this.updateExecutor = requireNonNull(updateExecutor, "updateExecutor is null");
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.heartbeatExecutor = heartbeatService;
        this.configuredTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");
    }

    public synchronized List<String> getAllDatabases()
    {
        checkReadable();
        return delegate.getAllDatabases();
    }

    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        checkReadable();
        return delegate.getDatabase(databaseName);
    }

    public synchronized List<String> getAllTables(String databaseName)
    {
        checkReadable();
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("Listing all tables after adding/dropping/altering tables/views in a transaction is not supported");
        }
        return delegate.getAllTables(databaseName);
    }

    public synchronized Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        checkReadable();
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return delegate.getTable(identity, databaseName, tableName);
        }
        switch (tableAction.getType()) {
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                return Optional.of(tableAction.getData().getTable());
            case DROP:
                return Optional.empty();
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return delegate.getSupportedColumnStatistics(type);
    }

    public synchronized PartitionStatistics getTableStatistics(HiveIdentity identity, String databaseName, String tableName)
    {
        checkReadable();
        Action<TableAndMore> tableAction = tableActions.get(new SchemaTableName(databaseName, tableName));
        if (tableAction == null) {
            return delegate.getTableStatistics(identity, databaseName, tableName);
        }
        switch (tableAction.getType()) {
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                return tableAction.getData().getStatistics();
            case DROP:
                return PartitionStatistics.empty();
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, String databaseName, String tableName, Set<String> partitionNames)
    {
        checkReadable();
        Optional<Table> table = getTable(identity, databaseName, tableName);
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
                    case PRE_EXISTING_TABLE:
                        partitionNamesToQuery.add(partitionName);
                        break;
                    case CREATED_IN_THIS_TRANSACTION:
                        resultBuilder.put(partitionName, PartitionStatistics.empty());
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown table source");
                }
            }
            else {
                resultBuilder.put(partitionName, partitionAction.getData().getStatistics());
            }
        }

        Map<String, PartitionStatistics> delegateResult = delegate.getPartitionStatistics(identity, databaseName, tableName, partitionNamesToQuery.build());
        if (!delegateResult.isEmpty()) {
            resultBuilder.putAll(delegateResult);
        }
        else {
            partitionNamesToQuery.build().forEach(partitionName -> resultBuilder.put(partitionName, PartitionStatistics.empty()));
        }
        return resultBuilder.build();
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
        switch (tableAction.getType()) {
            case ADD:
                return TableSource.CREATED_IN_THIS_TRANSACTION;
            case DROP:
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                return TableSource.PRE_EXISTING_TABLE;
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized HivePageSinkMetadata generatePageSinkMetadata(HiveIdentity identity, SchemaTableName schemaTableName)
    {
        checkReadable();
        Optional<Table> table = getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
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
            for (Map.Entry<List<String>, Action<PartitionAndMore>> entry : partitionActionMap.entrySet()) {
                modifiedPartitionMapBuilder.put(entry.getKey(), getPartitionFromPartitionAction(entry.getValue()));
            }
            modifiedPartitionMap = modifiedPartitionMapBuilder.build();
        }
        return new HivePageSinkMetadata(
                schemaTableName,
                table,
                modifiedPartitionMap);
    }

    public synchronized List<String> getAllViews(String databaseName)
    {
        checkReadable();
        if (!tableActions.isEmpty()) {
            throw new UnsupportedOperationException("Listing all tables after adding/dropping/altering tables/views in a transaction is not supported");
        }
        return delegate.getAllViews(databaseName);
    }

    public synchronized void createDatabase(HiveIdentity identity, Database database)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.createDatabase(identity, database));
    }

    public synchronized void dropDatabase(HiveIdentity identity, String schemaName)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.dropDatabase(identity, schemaName));
    }

    public synchronized void renameDatabase(HiveIdentity identity, String source, String target)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.renameDatabase(identity, source, target));
    }

    public synchronized void setDatabaseOwner(HiveIdentity identity, String source, HivePrincipal principal)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.setDatabaseOwner(identity, source, principal));
    }

    // TODO: Allow updating statistics for 2 tables in the same transaction
    public synchronized void setTableStatistics(HiveIdentity identity, Table table, PartitionStatistics tableStatistics)
    {
        AcidTransaction transaction = currentHiveTransaction.isPresent() ? currentHiveTransaction.get().getTransaction() : NO_ACID_TRANSACTION;
        setExclusive((delegate, hdfsEnvironment) ->
                delegate.updateTableStatistics(identity, table.getDatabaseName(), table.getTableName(), transaction, statistics -> updatePartitionStatistics(statistics, tableStatistics)));
    }

    // TODO: Allow updating statistics for 2 tables in the same transaction
    public synchronized void setPartitionStatistics(HiveIdentity identity, Table table, Map<List<String>, PartitionStatistics> partitionStatisticsMap)
    {
        Map<String, Function<PartitionStatistics, PartitionStatistics>> updates = partitionStatisticsMap.entrySet().stream().collect(
                toImmutableMap(
                        entry -> getPartitionName(table, entry.getKey()),
                        entry -> oldPartitionStats -> updatePartitionStatistics(oldPartitionStats, entry.getValue())));
        setExclusive((delegate, hdfsEnvironment) ->
                delegate.updatePartitionStatistics(
                        identity,
                        table.getDatabaseName(),
                        table.getTableName(),
                        updates));
    }

    // For HiveBasicStatistics, we only overwrite the original statistics if the new one is not empty.
    // For HiveColumnStatistics, only overwrite the original statistics for columns present in the new ones and preserve the others.
    private PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
    {
        HiveBasicStatistics oldBasicStatistics = oldPartitionStats.getBasicStatistics();
        HiveBasicStatistics newBasicStatistics = newPartitionStats.getBasicStatistics();
        HiveBasicStatistics updatedBasicStatistics = new HiveBasicStatistics(
                firstPresent(newBasicStatistics.getFileCount(), oldBasicStatistics.getFileCount()),
                firstPresent(newBasicStatistics.getRowCount(), oldBasicStatistics.getRowCount()),
                firstPresent(newBasicStatistics.getInMemoryDataSizeInBytes(), oldBasicStatistics.getInMemoryDataSizeInBytes()),
                firstPresent(newBasicStatistics.getOnDiskDataSizeInBytes(), oldBasicStatistics.getOnDiskDataSizeInBytes()));
        Map<String, HiveColumnStatistics> updatedColumnStatistics =
                updateColumnStatistics(oldPartitionStats.getColumnStatistics(), newPartitionStats.getColumnStatistics());
        return new PartitionStatistics(updatedBasicStatistics, updatedColumnStatistics);
    }

    private Map<String, HiveColumnStatistics> updateColumnStatistics(Map<String, HiveColumnStatistics> oldColumnStats, Map<String, HiveColumnStatistics> newColumnStats)
    {
        Map<String, HiveColumnStatistics> result = new HashMap<>(oldColumnStats);
        result.putAll(newColumnStats);
        return ImmutableMap.copyOf(result);
    }

    private static OptionalLong firstPresent(OptionalLong first, OptionalLong second)
    {
        return first.isPresent() ? first : second;
    }

    /**
     * {@code currentLocation} needs to be supplied if a writePath exists for the table.
     */
    public synchronized void createTable(
            ConnectorSession session,
            Table table,
            PrincipalPrivileges principalPrivileges,
            Optional<Path> currentPath,
            boolean ignoreExisting,
            PartitionStatistics statistics)
    {
        setShared();
        // When creating a table, it should never have partition actions. This is just a sanity check.
        checkNoPartitionAction(table.getDatabaseName(), table.getTableName());
        Action<TableAndMore> oldTableAction = tableActions.get(table.getSchemaTableName());
        HiveIdentity identity = new HiveIdentity(session);
        TableAndMore tableAndMore = new TableAndMore(table, identity, Optional.of(principalPrivileges), currentPath, Optional.empty(), ignoreExisting, statistics, statistics);
        if (oldTableAction == null) {
            HdfsContext hdfsContext = new HdfsContext(session);
            tableActions.put(table.getSchemaTableName(), new Action<>(ActionType.ADD, tableAndMore, hdfsContext, identity, session.getQueryId()));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                if (!oldTableAction.getHdfsContext().getIdentity().getUser().equals(session.getUser())) {
                    throw new TrinoException(TRANSACTION_CONFLICT, "Operation on the same table with different user in the same transaction is not supported");
                }
                HdfsContext hdfsContext = new HdfsContext(session);
                tableActions.put(table.getSchemaTableName(), new Action<>(ActionType.ALTER, tableAndMore, hdfsContext, identity, session.getQueryId()));
                return;

            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new TableAlreadyExistsException(table.getSchemaTableName());
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void dropTable(ConnectorSession session, String databaseName, String tableName)
    {
        setShared();
        // Dropping table with partition actions requires cleaning up staging data, which is not implemented yet.
        checkNoPartitionAction(databaseName, tableName);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null || oldTableAction.getType() == ActionType.ALTER) {
            HdfsContext hdfsContext = new HdfsContext(session);
            HiveIdentity identity = new HiveIdentity(session);
            tableActions.put(schemaTableName, new Action<>(ActionType.DROP, null, hdfsContext, identity, session.getQueryId()));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new UnsupportedOperationException("dropping a table added/modified in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table table, PrincipalPrivileges principalPrivileges)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.replaceTable(identity, databaseName, tableName, table, principalPrivileges));
    }

    public synchronized void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.renameTable(identity, databaseName, tableName, newDatabaseName, newTableName));
    }

    public synchronized void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.commentTable(identity, databaseName, tableName, comment));
    }

    public synchronized void setTableOwner(HiveIdentity identity, String schema, String table, HivePrincipal principal)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.setTableOwner(identity, schema, table, principal));
    }

    public synchronized void commentColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.commentColumn(identity, databaseName, tableName, columnName, comment));
    }

    public synchronized void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.addColumn(identity, databaseName, tableName, columnName, columnType, columnComment));
    }

    public synchronized void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.renameColumn(identity, databaseName, tableName, oldColumnName, newColumnName));
    }

    public synchronized void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.dropColumn(identity, databaseName, tableName, columnName));
    }

    public synchronized void finishInsertIntoExistingTable(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Path currentLocation,
            List<String> fileNames,
            PartitionStatistics statisticsUpdate)
    {
        // Data can only be inserted into partitions and unpartitioned tables. They can never be inserted into a partitioned table.
        // Therefore, this method assumes that the table is unpartitioned.
        setShared();
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null) {
            Table table = getExistingTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (isAcidTransactionRunning()) {
                table = Table.builder(table).setWriteId(OptionalLong.of(currentHiveTransaction.get().getTransaction().getWriteId())).build();
            }
            PartitionStatistics currentStatistics = getTableStatistics(identity, databaseName, tableName);
            HdfsContext hdfsContext = new HdfsContext(session);
            tableActions.put(
                    schemaTableName,
                    new Action<>(
                            ActionType.INSERT_EXISTING,
                            new TableAndMore(
                                    table,
                                    identity,
                                    Optional.empty(),
                                    Optional.of(currentLocation),
                                    Optional.of(fileNames),
                                    false,
                                    merge(currentStatistics, statisticsUpdate),
                                    statisticsUpdate),
                            hdfsContext,
                            identity,
                            session.getQueryId()));
            return;
        }

        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new UnsupportedOperationException("Inserting into an unpartitioned table that were added, altered, or inserted into in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    private boolean isAcidTransactionRunning()
    {
        return currentHiveTransaction.isPresent() && currentHiveTransaction.get().getTransaction().isAcidTransactionRunning();
    }

    public synchronized void truncateUnpartitionedTable(ConnectorSession session, String databaseName, String tableName)
    {
        checkReadable();
        Optional<Table> table = getTable(new HiveIdentity(session), databaseName, tableName);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        if (table.isEmpty()) {
            throw new TableNotFoundException(schemaTableName);
        }
        if (!table.get().getTableType().equals(MANAGED_TABLE.toString())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot delete from non-managed Hive table");
        }
        if (!table.get().getPartitionColumns().isEmpty()) {
            throw new IllegalArgumentException("Table is partitioned");
        }

        Path path = new Path(table.get().getStorage().getLocation());
        HdfsContext context = new HdfsContext(session);
        setExclusive((delegate, hdfsEnvironment) -> {
            RecursiveDeleteResult recursiveDeleteResult = recursiveDeleteFiles(hdfsEnvironment, context, path, ImmutableSet.of(""), false);
            if (!recursiveDeleteResult.getNotDeletedEligibleItems().isEmpty()) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, format(
                        "Error deleting from unpartitioned table %s. These items cannot be deleted: %s",
                        schemaTableName,
                        recursiveDeleteResult.getNotDeletedEligibleItems()));
            }
        });
    }

    public synchronized void finishRowLevelDelete(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Path currentLocation,
            List<PartitionAndStatementId> partitionAndStatementIds)
    {
        if (partitionAndStatementIds.isEmpty()) {
            return;
        }
        setShared();
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null) {
            Table table = getExistingTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            HdfsContext hdfsContext = new HdfsContext(session);
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());
            tableActions.put(
                    schemaTableName,
                    new Action<>(
                            ActionType.DELETE_ROWS,
                            new TableAndAcidDirectories(
                                    table,
                                    identity,
                                    Optional.of(principalPrivileges),
                                    Optional.of(currentLocation),
                                    partitionAndStatementIds),
                            hdfsContext,
                            identity,
                            session.getQueryId()));
            return;
        }

        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new UnsupportedOperationException("Inserting or deleting in an unpartitioned table that were added, altered, or inserted into in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void finishUpdate(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Path currentLocation,
            List<PartitionAndStatementId> partitionAndStatementIds)
    {
        if (partitionAndStatementIds.isEmpty()) {
            return;
        }
        setShared();
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> oldTableAction = tableActions.get(schemaTableName);
        if (oldTableAction == null) {
            Table table = getExistingTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
            HdfsContext hdfsContext = new HdfsContext(session);
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());
            tableActions.put(
                    schemaTableName,
                    new Action<>(
                            ActionType.UPDATE,
                            new TableAndAcidDirectories(
                                    table,
                                    identity,
                                    Optional.of(principalPrivileges),
                                    Optional.of(currentLocation),
                                    partitionAndStatementIds),
                            hdfsContext,
                            identity,
                            session.getQueryId()));
            return;
        }

        switch (oldTableAction.getType()) {
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new UnsupportedOperationException("Inserting, updating or deleting in a table that was added, altered, inserted into, updated or deleted from in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        Optional<Table> table = getTable(identity, databaseName, tableName);

        if (table.isEmpty()) {
            return Optional.empty();
        }

        List<String> columnNames = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        return doGetPartitionNames(identity, databaseName, tableName, columnNames, TupleDomain.all());
    }

    public synchronized Optional<List<String>> getPartitionNamesByFilter(
            HiveIdentity identity,
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return doGetPartitionNames(identity, databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @GuardedBy("this")
    private Optional<List<String>> doGetPartitionNames(
            HiveIdentity identity,
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

        Optional<Table> table = getTable(identity, databaseName, tableName);
        if (table.isEmpty()) {
            return Optional.empty();
        }
        List<String> partitionNames;
        TableSource tableSource = getTableSource(databaseName, tableName);
        switch (tableSource) {
            case CREATED_IN_THIS_TRANSACTION:
                partitionNames = ImmutableList.of();
                break;
            case PRE_EXISTING_TABLE:
                Optional<List<String>> partitionNameResult = delegate.getPartitionNamesByFilter(identity, databaseName, tableName, columnNames, partitionKeysFilter);
                if (partitionNameResult.isEmpty()) {
                    throw new TrinoException(TRANSACTION_CONFLICT, format("Table '%s.%s' was dropped by another transaction", databaseName, tableName));
                }
                partitionNames = partitionNameResult.get();
                break;
            default:
                throw new UnsupportedOperationException("Unknown table source");
        }
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(table.get().getSchemaTableName(), k -> new HashMap<>());
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        // alter/remove newly-altered/dropped partitions from the results from underlying metastore
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            Action<PartitionAndMore> partitionAction = partitionActionsOfTable.get(partitionValues);
            if (partitionAction == null) {
                resultBuilder.add(partitionName);
                continue;
            }
            switch (partitionAction.getType()) {
                case ADD:
                    throw new TrinoException(TRANSACTION_CONFLICT, format("Another transaction created partition %s in table %s.%s", partitionValues, databaseName, tableName));
                case DROP:
                case DROP_PRESERVE_DATA:
                    // do nothing
                    break;
                case ALTER:
                case INSERT_EXISTING:
                case DELETE_ROWS:
                case UPDATE:
                    resultBuilder.add(partitionName);
                    break;
                default:
                    throw new IllegalStateException("Unknown action type");
            }
        }
        // add newly-added partitions to the results from underlying metastore.
        if (!partitionActionsOfTable.isEmpty()) {
            for (Action<PartitionAndMore> partitionAction : partitionActionsOfTable.values()) {
                if (partitionAction.getType() == ActionType.ADD) {
                    List<String> values = partitionAction.getData().getPartition().getValues();
                    resultBuilder.add(makePartName(columnNames, values));
                }
            }
        }
        return Optional.of(resultBuilder.build());
    }

    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
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
                    case PRE_EXISTING_TABLE:
                        partitionNamesToQueryBuilder.add(partitionName);
                        break;
                    case CREATED_IN_THIS_TRANSACTION:
                        resultBuilder.put(partitionName, Optional.empty());
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown table source");
                }
            }
            else {
                resultBuilder.put(partitionName, getPartitionFromPartitionAction(partitionAction));
            }
        }

        List<String> partitionNamesToQuery = partitionNamesToQueryBuilder.build();
        if (!partitionNamesToQuery.isEmpty()) {
            Map<String, Optional<Partition>> delegateResult = delegate.getPartitionsByNames(
                    identity,
                    databaseName,
                    tableName,
                    partitionNamesToQuery);
            resultBuilder.putAll(delegateResult);
        }

        return resultBuilder.build();
    }

    private static Optional<Partition> getPartitionFromPartitionAction(Action<PartitionAndMore> partitionAction)
    {
        switch (partitionAction.getType()) {
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                return Optional.of(partitionAction.getData().getAugmentedPartitionForInTransactionRead());
            case DROP:
            case DROP_PRESERVE_DATA:
                return Optional.empty();
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void addPartition(
            ConnectorSession session,
            String databaseName,
            String tableName,
            Partition partition,
            Path currentLocation,
            PartitionStatistics statistics)
    {
        setShared();
        checkArgument(getPrestoQueryId(partition).isPresent());
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partition.getValues());
        HdfsContext hdfsContext = new HdfsContext(session);
        HiveIdentity identity = new HiveIdentity(session);
        if (oldPartitionAction == null) {
            partitionActionsOfTable.put(
                    partition.getValues(),
                    new Action<>(ActionType.ADD, new PartitionAndMore(identity, partition, currentLocation, Optional.empty(), statistics, statistics), hdfsContext, identity, session.getQueryId()));
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                if (!oldPartitionAction.getHdfsContext().getIdentity().getUser().equals(session.getUser())) {
                    throw new TrinoException(TRANSACTION_CONFLICT, "Operation on the same partition with different user in the same transaction is not supported");
                }
                partitionActionsOfTable.put(
                        partition.getValues(),
                        new Action<>(ActionType.ALTER, new PartitionAndMore(identity, partition, currentLocation, Optional.empty(), statistics, statistics), hdfsContext, identity, session.getQueryId()));
                return;
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new TrinoException(ALREADY_EXISTS, format("Partition already exists for table '%s.%s': %s", databaseName, tableName, partition.getValues()));
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void dropPartition(ConnectorSession session, String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        setShared();
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(new SchemaTableName(databaseName, tableName), k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partitionValues);
        if (oldPartitionAction == null) {
            HdfsContext hdfsContext = new HdfsContext(session);
            HiveIdentity identity = new HiveIdentity(session);
            if (deleteData) {
                partitionActionsOfTable.put(partitionValues, new Action<>(ActionType.DROP, null, hdfsContext, identity, session.getQueryId()));
            }
            else {
                partitionActionsOfTable.put(partitionValues, new Action<>(ActionType.DROP_PRESERVE_DATA, null, hdfsContext, identity, session.getQueryId()));
            }
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new TrinoException(
                        NOT_SUPPORTED,
                        format("dropping a partition added in the same transaction is not supported: %s %s %s", databaseName, tableName, partitionValues));
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized void finishInsertIntoExistingPartition(
            ConnectorSession session,
            String databaseName,
            String tableName,
            List<String> partitionValues,
            Path currentLocation,
            List<String> fileNames,
            PartitionStatistics statisticsUpdate)
    {
        setShared();
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.computeIfAbsent(schemaTableName, k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsOfTable.get(partitionValues);
        if (oldPartitionAction == null) {
            Partition partition = delegate.getPartition(identity, databaseName, tableName, partitionValues)
                    .orElseThrow(() -> new PartitionNotFoundException(schemaTableName, partitionValues));
            String partitionName = getPartitionName(identity, databaseName, tableName, partitionValues);
            PartitionStatistics currentStatistics = delegate.getPartitionStatistics(identity, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName);
            if (currentStatistics == null) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "currentStatistics is null");
            }
            HdfsContext context = new HdfsContext(session);
            partitionActionsOfTable.put(
                    partitionValues,
                    new Action<>(
                            ActionType.INSERT_EXISTING,
                            new PartitionAndMore(
                                    identity,
                                    partition,
                                    currentLocation,
                                    Optional.of(fileNames),
                                    merge(currentStatistics, statisticsUpdate),
                                    statisticsUpdate),
                            context,
                            identity,
                            session.getQueryId()));
            return;
        }

        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                throw new PartitionNotFoundException(schemaTableName, partitionValues);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                throw new UnsupportedOperationException("Inserting into a partition that were added, altered, or inserted into in the same transaction is not supported");
        }
        throw new IllegalStateException("Unknown action type");
    }

    private synchronized AcidTransaction getCurrentAcidTransaction()
    {
        return currentHiveTransaction.map(HiveTransaction::getTransaction)
                .orElseThrow(() -> new IllegalStateException("currentHiveTransaction not present"));
    }

    private String getPartitionName(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        Table table = getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        return getPartitionName(table, partitionValues);
    }

    private String getPartitionName(Table table, List<String> partitionValues)
    {
        List<String> columnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        return makePartName(columnNames, partitionValues);
    }

    @Override
    public synchronized void createRole(String role, String grantor)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.createRole(role, grantor));
    }

    @Override
    public synchronized void dropRole(String role)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.dropRole(role));
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
        setExclusive((delegate, hdfsEnvironment) -> delegate.grantRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public synchronized void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.revokeRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public synchronized Set<RoleGrant> listGrantedPrincipals(String role)
    {
        checkReadable();
        return delegate.listGrantedPrincipals(role);
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        checkReadable();
        return delegate.listRoleGrants(principal);
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(HiveIdentity identity, String databaseName, String tableName, Optional<HivePrincipal> principal)
    {
        checkReadable();
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Action<TableAndMore> tableAction = tableActions.get(schemaTableName);
        if (tableAction == null) {
            return delegate.listTablePrivileges(databaseName, tableName, getTableOwner(identity, databaseName, tableName), principal);
        }
        switch (tableAction.getType()) {
            case ADD:
            case ALTER:
                if (principal.isPresent() && principal.get().getType() == PrincipalType.ROLE) {
                    return ImmutableSet.of();
                }
                String owner = tableAction.getData().getTable().getOwner();
                if (principal.isPresent() && !principal.get().getName().equals(owner)) {
                    return ImmutableSet.of();
                }
                Collection<HivePrivilegeInfo> privileges = tableAction.getData().getPrincipalPrivileges().getUserPrivileges().get(owner);
                return ImmutableSet.<HivePrivilegeInfo>builder()
                        .addAll(privileges)
                        .add(new HivePrivilegeInfo(OWNERSHIP, true, new HivePrincipal(USER, owner), new HivePrincipal(USER, owner)))
                        .build();
            case INSERT_EXISTING:
            case DELETE_ROWS:
            case UPDATE:
                return delegate.listTablePrivileges(databaseName, tableName, getTableOwner(identity, databaseName, tableName), principal);
            case DROP:
                throw new TableNotFoundException(schemaTableName);
            case DROP_PRESERVE_DATA:
                // TODO
                break;
        }
        throw new IllegalStateException("Unknown action type");
    }

    public synchronized String getTableOwner(HiveIdentity identity, String databaseName, String tableName)
    {
        return getExistingTable(identity, databaseName, tableName).getOwner();
    }

    private Table getExistingTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return delegate.getTable(identity, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public synchronized void grantTablePrivileges(HiveIdentity identity, String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.grantTablePrivileges(databaseName, tableName, getTableOwner(identity, databaseName, tableName), grantee, privileges));
    }

    @Override
    public synchronized void revokeTablePrivileges(HiveIdentity identity, String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        setExclusive((delegate, hdfsEnvironment) -> delegate.revokeTablePrivileges(databaseName, tableName, getTableOwner(identity, databaseName, tableName), grantee, privileges));
    }

    public synchronized void declareIntentionToWrite(ConnectorSession session, WriteMode writeMode, Path stagingPathRoot, SchemaTableName schemaTableName)
    {
        setShared();
        if (writeMode == WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
            Map<List<String>, Action<PartitionAndMore>> partitionActionsOfTable = partitionActions.get(schemaTableName);
            if (partitionActionsOfTable != null && !partitionActionsOfTable.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot insert into a table with a partition that has been modified in the same transaction when Trinoto is configured to skip temporary directories.");
            }
        }
        HdfsContext hdfsContext = new HdfsContext(session);
        HiveIdentity identity = new HiveIdentity(session);
        declaredIntentionsToWrite.add(new DeclaredIntentionToWrite(writeMode, hdfsContext, identity, session.getQueryId(), stagingPathRoot, schemaTableName));
    }

    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    public synchronized void commit()
    {
        try {
            switch (state) {
                case EMPTY:
                    return;
                case SHARED_OPERATION_BUFFERED:
                    commitShared();
                    return;
                case EXCLUSIVE_OPERATION_BUFFERED:
                    requireNonNull(bufferedExclusiveOperation, "bufferedExclusiveOperation is null");
                    bufferedExclusiveOperation.execute(delegate, hdfsEnvironment);
                    return;
                case FINISHED:
                    throw new IllegalStateException("Tried to commit buffered metastore operations after transaction has been committed/aborted");
            }
            throw new IllegalStateException("Unknown state: " + state);
        }
        finally {
            state = State.FINISHED;
        }
    }

    public synchronized void rollback()
    {
        try {
            switch (state) {
                case EMPTY:
                case EXCLUSIVE_OPERATION_BUFFERED:
                    return;
                case SHARED_OPERATION_BUFFERED:
                    rollbackShared();
                    return;
                case FINISHED:
                    throw new IllegalStateException("Tried to rollback buffered metastore operations after transaction has been committed/aborted");
            }
            throw new IllegalStateException("Unknown state: " + state);
        }
        finally {
            state = State.FINISHED;
        }
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
        return beginOperation(session, table, AcidOperation.INSERT, DataOperationType.INSERT, Optional.empty());
    }

    public AcidTransaction beginDelete(ConnectorSession session, Table table)
    {
        return beginOperation(session, table, AcidOperation.DELETE, DataOperationType.DELETE, Optional.empty());
    }

    public AcidTransaction beginUpdate(ConnectorSession session, Table table, HiveUpdateProcessor updateProcessor)
    {
        return beginOperation(session, table, AcidOperation.UPDATE, DataOperationType.UPDATE, Optional.of(updateProcessor));
    }

    private AcidTransaction beginOperation(ConnectorSession session, Table table, AcidOperation operation, DataOperationType hiveOperation, Optional<HiveUpdateProcessor> updateProcessor)
    {
        String queryId = session.getQueryId();

        synchronized (this) {
            currentQueryId = Optional.of(queryId);

            // We start the transaction immediately, and allocate the write lock and the writeId,
            // because we need the writeId in order to write the delta files.
            HiveTransaction hiveTransaction = makeHiveTransaction(session, transactionId -> {
                acquireTableWriteLock(
                        new HiveIdentity(session),
                        queryId,
                        transactionId,
                        table.getDatabaseName(),
                        table.getTableName(),
                        hiveOperation,
                        !table.getPartitionColumns().isEmpty());
                long writeId = allocateWriteId(new HiveIdentity(session), table.getDatabaseName(), table.getTableName(), transactionId);
                return new AcidTransaction(operation, transactionId, writeId, updateProcessor);
            });
            hiveTransactionSupplier = Optional.of(() -> hiveTransaction);
            currentHiveTransaction = Optional.of(hiveTransaction);
            return hiveTransaction.getTransaction();
        }
    }

    private HiveTransaction makeHiveTransaction(ConnectorSession session, Function<Long, AcidTransaction> transactionMaker)
    {
        String queryId = session.getQueryId();
        HiveIdentity identity = new HiveIdentity(session);

        long heartbeatInterval = configuredTransactionHeartbeatInterval
                .map(Duration::toMillis)
                .orElseGet(this::getServerExpectedHeartbeatIntervalMillis);
        long transactionId = delegate.openTransaction(identity);
        log.debug("Using hive transaction %s for %s", transactionId, queryId);

        ScheduledFuture<?> heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(
                () -> delegate.sendTransactionHeartbeat(identity, transactionId),
                0,
                heartbeatInterval,
                MILLISECONDS);

        AcidTransaction transaction = transactionMaker.apply(transactionId);
        return new HiveTransaction(identity, queryId, transactionId, heartbeatTask, transaction);
    }

    private long getServerExpectedHeartbeatIntervalMillis()
    {
        String hiveServerTransactionTimeout = delegate.getConfigValue(TXN_TIMEOUT.getVarname()).orElseGet(() -> TXN_TIMEOUT.getDefaultVal().toString());
        Configuration configuration = new Configuration(false);
        configuration.set(TXN_TIMEOUT.toString(), hiveServerTransactionTimeout);
        return getTimeVar(configuration, TXN_TIMEOUT, MILLISECONDS) / 2;
    }

    public synchronized Optional<ValidTxnWriteIdList> getValidWriteIds(ConnectorSession session, HiveTableHandle tableHandle)
    {
        String queryId = session.getQueryId();
        checkState(currentQueryId.equals(Optional.of(queryId)), "Invalid query id %s while current query is", queryId, currentQueryId);
        if (!AcidUtils.isTransactionalTable(tableHandle.getTableParameters().orElseThrow(() -> new IllegalStateException("tableParameters missing")))) {
            return Optional.empty();
        }

        if (currentHiveTransaction.isEmpty()) {
            currentHiveTransaction = Optional.of(hiveTransactionSupplier
                    .orElseThrow(() -> new IllegalStateException("hiveTransactionSupplier is not set"))
                    .get());
        }

        return Optional.of(currentHiveTransaction.get().getValidWriteIds(delegate, tableHandle));
    }

    public synchronized void cleanupQuery(ConnectorSession session)
    {
        String queryId = session.getQueryId();
        HiveIdentity identity = new HiveIdentity(session);
        checkState(currentQueryId.equals(Optional.of(queryId)), "Invalid query id %s while current query is", queryId, currentQueryId);
        Optional<HiveTransaction> transaction = currentHiveTransaction;

        if (transaction.isEmpty()) {
            clearCurrentTransaction();
            return;
        }

        commit();

        clearCurrentTransaction();
        long transactionId = transaction.get().getTransactionId();
        ScheduledFuture<?> heartbeatTask = transaction.get().getHeartbeatTask();
        heartbeatTask.cancel(true);

        // Any failure around aborted transactions, etc would be handled by Hive Metastore commit and TrinoException will be thrown
        delegate.commitTransaction(identity, transactionId);
    }

    @GuardedBy("this")
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

        AcidTransaction transaction = currentHiveTransaction.isEmpty() ? NO_ACID_TRANSACTION : currentHiveTransaction.get().getTransaction();

        Committer committer = new Committer(transaction);
        try {
            for (Map.Entry<SchemaTableName, Action<TableAndMore>> entry : tableActions.entrySet()) {
                SchemaTableName schemaTableName = entry.getKey();
                Action<TableAndMore> action = entry.getValue();
                switch (action.getType()) {
                    case DROP:
                        committer.prepareDropTable(action.getIdentity(), schemaTableName);
                        break;
                    case ALTER:
                        committer.prepareAlterTable(action.getHdfsContext(), action.getIdentity(), action.getQueryId(), action.getData());
                        break;
                    case ADD:
                        committer.prepareAddTable(action.getHdfsContext(), action.getData());
                        break;
                    case INSERT_EXISTING:
                        committer.prepareInsertExistingTable(action.getHdfsContext(), action.getData());
                        break;
                    case DELETE_ROWS:
                        committer.prepareDeleteRowsFromExistingTable(action.getHdfsContext(), action.getData());
                        break;
                    case UPDATE:
                        committer.prepareUpdateExistingTable(action.getHdfsContext(), action.getData());
                        break;
                    default:
                        throw new IllegalStateException("Unknown action type");
                }
            }
            for (Map.Entry<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> tableEntry : partitionActions.entrySet()) {
                SchemaTableName schemaTableName = tableEntry.getKey();
                for (Map.Entry<List<String>, Action<PartitionAndMore>> partitionEntry : tableEntry.getValue().entrySet()) {
                    List<String> partitionValues = partitionEntry.getKey();
                    Action<PartitionAndMore> action = partitionEntry.getValue();
                    switch (action.getType()) {
                        case DROP:
                            committer.prepareDropPartition(action.getIdentity(), schemaTableName, partitionValues, true);
                            break;
                        case DROP_PRESERVE_DATA:
                            committer.prepareDropPartition(action.getIdentity(), schemaTableName, partitionValues, false);
                            break;
                        case ALTER:
                            committer.prepareAlterPartition(action.getHdfsContext(), action.getIdentity(), action.getQueryId(), action.getData());
                            break;
                        case ADD:
                            committer.prepareAddPartition(action.getHdfsContext(), action.getIdentity(), action.getData());
                            break;
                        case INSERT_EXISTING:
                            committer.prepareInsertExistingPartition(action.getHdfsContext(), action.getIdentity(), action.getData());
                            break;
                        case UPDATE:
                        case DELETE_ROWS:
                            break;
                        default:
                            throw new IllegalStateException("Unknown action type");
                    }
                }
            }

            // Wait for all renames submitted for "INSERT_EXISTING" action to finish
            committer.waitForAsyncRenames();

            // At this point, all file system operations, whether asynchronously issued or not, have completed successfully.
            // We are moving on to metastore operations now.

            committer.executeAddTableOperations();
            committer.executeAlterTableOperations();
            committer.executeAlterPartitionOperations();
            committer.executeAddPartitionOperations(transaction);
            committer.executeUpdateStatisticsOperations(transaction);
        }
        catch (Throwable t) {
            log.warn("Rolling back due to metastore commit failure: %s", t.getMessage());
            try {
                committer.cancelUnstartedAsyncRenames();

                committer.undoUpdateStatisticsOperations(transaction);
                committer.undoAddPartitionOperations();
                committer.undoAddTableOperations();

                committer.waitForAsyncRenamesSuppressThrowables();

                // fileRenameFutures must all come back before any file system cleanups are carried out.
                // Otherwise, files that should be deleted may be created after cleanup is done.
                committer.executeCleanupTasksForAbort(declaredIntentionsToWrite);

                committer.executeRenameTasksForAbort();

                // Partition directory must be put back before relevant metastore operation can be undone
                committer.undoAlterTableOperations();
                committer.undoAlterPartitionOperations();

                rollbackShared();
            }
            catch (RuntimeException e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }

            throw t;
        }

        try {
            // After this line, operations are no longer reversible.
            // The next section will deal with "dropping table/partition". Commit may still fail in
            // this section. Even if commit fails, cleanups, instead of rollbacks, will be executed.

            committer.executeIrreversibleMetastoreOperations();

            // If control flow reached this point, this commit is considered successful no matter
            // what happens later. The only kind of operations that haven't been carried out yet
            // are cleanups.

            // The program control flow will go to finally next. And cleanup will run because
            // moveForwardInFinally has been set to false.
        }
        finally {
            // In this method, all operations are best-effort clean up operations.
            // If any operation fails, the error will be logged and ignored.
            // Additionally, other clean up operations should still be attempted.

            // Execute deletion tasks
            committer.executeDeletionTasksForFinish();

            // Clean up empty staging directories (that may recursively contain empty directories)
            committer.deleteEmptyStagingDirectories(declaredIntentionsToWrite);
        }
    }

    private class Committer
    {
        private final AtomicBoolean fileRenameCancelled = new AtomicBoolean(false);
        private final List<CompletableFuture<?>> fileRenameFutures = new ArrayList<>();

        // File system
        // For file system changes, only operations outside of writing paths (as specified in declared intentions to write)
        // need to MOVE_BACKWARD tasks scheduled. Files in writing paths are handled by rollbackShared().
        private final List<DirectoryDeletionTask> deletionTasksForFinish = new ArrayList<>();
        private final List<DirectoryCleanUpTask> cleanUpTasksForAbort = new ArrayList<>();
        private final List<DirectoryRenameTask> renameTasksForAbort = new ArrayList<>();

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

        private void prepareDropTable(HiveIdentity identity, SchemaTableName schemaTableName)
        {
            metastoreDeleteOperations.add(new IrreversibleMetastoreOperation(
                    format("drop table %s", schemaTableName),
                    () -> delegate.dropTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName(), true)));
        }

        private void prepareAlterTable(HdfsContext hdfsContext, HiveIdentity identity, String queryId, TableAndMore tableAndMore)
        {
            deleteOnly = false;

            Table table = tableAndMore.getTable();
            String targetLocation = table.getStorage().getLocation();
            Table oldTable = delegate.getTable(identity, table.getDatabaseName(), table.getTableName())
                    .orElseThrow(() -> new TrinoException(TRANSACTION_CONFLICT, "The table that this transaction modified was deleted in another transaction. " + table.getSchemaTableName()));
            String oldTableLocation = oldTable.getStorage().getLocation();
            Path oldTablePath = new Path(oldTableLocation);

            // Location of the old table and the new table can be different because we allow arbitrary directories through LocationService.
            // If the location of the old table is the same as the location of the new table:
            // * Rename the old data directory to a temporary path with a special suffix
            // * Remember we will need to delete that directory at the end if transaction successfully commits
            // * Remember we will need to undo the rename if transaction aborts
            // Otherwise,
            // * Remember we will need to delete the location of the old partition at the end if transaction successfully commits
            if (targetLocation.equals(oldTableLocation)) {
                Path oldTableStagingPath = new Path(oldTablePath.getParent(), "_temp_" + oldTablePath.getName() + "_" + queryId);
                renameDirectory(
                        hdfsContext,
                        hdfsEnvironment,
                        oldTablePath,
                        oldTableStagingPath,
                        () -> renameTasksForAbort.add(new DirectoryRenameTask(hdfsContext, oldTableStagingPath, oldTablePath)));
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(hdfsContext, oldTableStagingPath));
                }
            }
            else {
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(hdfsContext, oldTablePath));
                }
            }

            Path currentPath = tableAndMore.getCurrentLocation()
                    .orElseThrow(() -> new IllegalArgumentException("location should be present for alter table"));
            Path targetPath = new Path(targetLocation);
            if (!targetPath.equals(currentPath)) {
                renameDirectory(
                        hdfsContext,
                        hdfsEnvironment,
                        currentPath,
                        targetPath,
                        () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(hdfsContext, targetPath, true)));
            }
            // Partition alter must happen regardless of whether original and current location is the same
            // because metadata might change: e.g. storage format, column types, etc
            alterTableOperations.add(new AlterTableOperation(tableAndMore.getIdentity(), tableAndMore.getTable(), oldTable, tableAndMore.getPrincipalPrivileges()));

            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    tableAndMore.getIdentity(),
                    table.getSchemaTableName(),
                    Optional.empty(),
                    tableAndMore.getStatisticsUpdate(),
                    false));
        }

        private void prepareAddTable(HdfsContext context, TableAndMore tableAndMore)
        {
            deleteOnly = false;

            Table table = tableAndMore.getTable();
            if (table.getTableType().equals(MANAGED_TABLE.name())) {
                Optional<String> targetLocation = table.getStorage().getOptionalLocation();
                if (targetLocation.isPresent()) {
                    checkArgument(!targetLocation.get().isEmpty(), "target location is empty");
                    Optional<Path> currentPath = tableAndMore.getCurrentLocation();
                    Path targetPath = new Path(targetLocation.get());
                    if (table.getPartitionColumns().isEmpty() && currentPath.isPresent()) {
                        // CREATE TABLE AS SELECT unpartitioned table
                        if (targetPath.equals(currentPath.get())) {
                            // Target path and current path are the same. Therefore, directory move is not needed.
                        }
                        else {
                            renameDirectory(
                                    context,
                                    hdfsEnvironment,
                                    currentPath.get(),
                                    targetPath,
                                    () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(context, targetPath, true)));
                        }
                    }
                    else {
                        // CREATE TABLE AS SELECT partitioned table, or
                        // CREATE TABLE partitioned/unpartitioned table (without data)
                        if (pathExists(context, hdfsEnvironment, targetPath)) {
                            if (currentPath.isPresent() && currentPath.get().equals(targetPath)) {
                                // It is okay to skip directory creation when currentPath is equal to targetPath
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
                            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(context, targetPath, true));
                            createDirectory(context, hdfsEnvironment, targetPath);
                        }
                    }
                }
                // if targetLocation is not set in table we assume table directory is created by HMS
            }
            addTableOperations.add(new CreateTableOperation(tableAndMore.getIdentity(), table, tableAndMore.getPrincipalPrivileges(), tableAndMore.isIgnoreExisting()));
            if (!isPrestoView(table)) {
                updateStatisticsOperations.add(new UpdateStatisticsOperation(
                        tableAndMore.getIdentity(),
                        table.getSchemaTableName(),
                        Optional.empty(),
                        tableAndMore.getStatisticsUpdate(),
                        false));
            }
        }

        private void prepareInsertExistingTable(HdfsContext context, TableAndMore tableAndMore)
        {
            deleteOnly = false;
            Table table = tableAndMore.getTable();
            Path targetPath = new Path(table.getStorage().getLocation());
            Path currentPath = tableAndMore.getCurrentLocation().get();
            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(context, targetPath, false));
            if (!targetPath.equals(currentPath)) {
                asyncRename(hdfsEnvironment, renameExecutor, fileRenameCancelled, fileRenameFutures, context, currentPath, targetPath, tableAndMore.getFileNames().get());
            }
            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    tableAndMore.getIdentity(),
                    table.getSchemaTableName(),
                    Optional.empty(),
                    tableAndMore.getStatisticsUpdate(),
                    true));

            if (isAcidTransactionRunning()) {
                AcidTransaction transaction = getCurrentAcidTransaction();
                updateTableWriteId(tableAndMore.getIdentity(), table.getDatabaseName(), table.getTableName(), transaction.getAcidTransactionId(), transaction.getWriteId(), OptionalLong.empty());
            }
        }

        private void prepareDeleteRowsFromExistingTable(HdfsContext context, TableAndMore tableAndMore)
        {
            TableAndAcidDirectories deletionState = (TableAndAcidDirectories) tableAndMore;
            List<PartitionAndStatementId> partitionAndStatementIds = deletionState.getPartitionAndStatementIds();
            checkArgument(!partitionAndStatementIds.isEmpty(), "partitionAndStatementIds is empty");

            deleteOnly = false;
            Table table = deletionState.getTable();
            checkArgument(currentHiveTransaction.isPresent(), "currentHiveTransaction isn't present");
            AcidTransaction transaction = currentHiveTransaction.get().getTransaction();
            checkArgument(transaction.isDelete(), "transaction should be delete, but is %s", transaction);

            cleanUpTasksForAbort.addAll(deletionState.getPartitionAndStatementIds().stream()
                    .map(ps -> new DirectoryCleanUpTask(context, new Path(ps.getDeleteDeltaDirectory()), true))
                    .collect(toImmutableList()));

            Map<String, Long> partitionRowCounts = new HashMap<>(partitionAndStatementIds.size());
            int totalRowsDeleted = 0;
            for (PartitionAndStatementId ps : partitionAndStatementIds) {
                long rowCount = ps.getRowCount();
                partitionRowCounts.compute(ps.getPartitionName(), (k, count) -> count != null ? count + rowCount : rowCount);
            }

            HiveIdentity identity = deletionState.getIdentity();
            String databaseName = table.getDatabaseName();
            String tableName = table.getTableName();

            // Update the table statistics
            PartitionStatistics tableStatistics = getTableStatistics(identity, databaseName, tableName);
            HiveBasicStatistics basicStatistics = tableStatistics.getBasicStatistics();
            if (basicStatistics.getRowCount().isPresent()) {
                tableStatistics = tableStatistics.withAdjustedRowCount(-totalRowsDeleted);
                updateStatisticsOperations.add(new UpdateStatisticsOperation(identity, table.getSchemaTableName(), Optional.empty(), tableStatistics, true));
            }
            // Decrement the numRows of the table itself, if it has the numRows parameter
            if (table.getParameters() != null && table.getParameters().get(NUM_ROWS) != null) {
                Table updatedTable = Table.builder(table)
                        .setParameters(MetastoreUtil.adjustRowCount(table.getParameters(), "decrement table rows", -totalRowsDeleted))
                        .setWriteId(OptionalLong.of(transaction.getWriteId()))
                        .build();
                alterTableOperations.add(new AlterTableOperation(identity, updatedTable, table, tableAndMore.getPrincipalPrivileges()));
            }

            Map<String, Optional<Partition>> partitionsOptionalMap = getPartitionsByNames(identity, databaseName, tableName, new ArrayList<>(partitionRowCounts.keySet()));
            Map<String, Partition> updatedPartitions = new HashMap<>(partitionsOptionalMap.size());

            // Collect the partitions that have deletions and also have the numRows parameter
            for (Map.Entry<String, Optional<Partition>> entry : partitionsOptionalMap.entrySet()) {
                String partitionId = entry.getKey();
                if (entry.getValue().isPresent()) {
                    Partition partition = entry.getValue().get();
                    if (partition.getParameters().containsKey(NUM_ROWS)) {
                        Long rowCounter = partitionRowCounts.get(partitionId);
                        requireNonNull(rowCounter, "rowCounter is null");
                        updatedPartitions.put(partitionId, partition.withAdjustedRowCount(partitionId, -rowCounter));
                    }
                }
            }

            // Decrement the row counts in the Partitions and PartitionStatistics
            Map<String, PartitionStatistics> allPartitionStatistics = getPartitionStatistics(identity, databaseName, tableName, updatedPartitions.keySet());
            allPartitionStatistics.forEach((partitionId, statistics) -> {
                Long rowCount = partitionRowCounts.get(partitionId);
                requireNonNull(rowCount, "rowCount is null");
                PartitionStatistics updatedStatistics = statistics.withAdjustedRowCount(-rowCount);
                requireNonNull(updatedStatistics, "updatedStatistics is null");
                Partition updatedPartition = updatedPartitions.get(partitionId);
                requireNonNull(updatedPartition, "updatedPartition is null");
                Partition originalPartition = partitionsOptionalMap.get(partitionId).get();
                alterPartitionOperations.add(new AlterPartitionOperation(
                        identity,
                        new PartitionWithStatistics(updatedPartition, partitionId, updatedStatistics),
                        new PartitionWithStatistics(originalPartition, partitionId, statistics)));
                updateStatisticsOperations.add(new UpdateStatisticsOperation(identity, table.getSchemaTableName(), Optional.of(partitionId), updatedStatistics, false));
            });

            // Finally, tell the metastore what has changed
            long writeId = transaction.getWriteId();
            long transactionId = transaction.getAcidTransactionId();
            if (!updatedPartitions.isEmpty()) {
                alterPartitions(identity, databaseName, tableName, ImmutableList.copyOf(updatedPartitions.values()), writeId);
                addDynamicPartitions(identity, databaseName, tableName, ImmutableList.copyOf(updatedPartitions.keySet()), transactionId, writeId, AcidOperation.DELETE);
            }
            updateTableWriteId(identity, databaseName, tableName, transactionId, writeId, OptionalLong.of(-totalRowsDeleted));
        }

        private void prepareUpdateExistingTable(HdfsContext context, TableAndMore tableAndMore)
        {
            TableAndAcidDirectories updateState = (TableAndAcidDirectories) tableAndMore;
            List<PartitionAndStatementId> partitionAndStatementIds = updateState.getPartitionAndStatementIds();
            checkArgument(!partitionAndStatementIds.isEmpty(), "partitionAndStatementIds is empty");

            Table table = updateState.getTable();
            checkArgument(currentHiveTransaction.isPresent(), "currentHiveTransaction isn't present");
            AcidTransaction transaction = currentHiveTransaction.get().getTransaction();
            checkArgument(transaction.isUpdate(), "transaction should be update, but is %s", transaction);

            updateState.getPartitionAndStatementIds().stream()
                    .flatMap(ps -> ps.getAllDirectories().stream())
                    .forEach(directory ->
                            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(context, new Path(directory), true)));

            HiveIdentity identity = updateState.getIdentity();
            String databaseName = table.getDatabaseName();
            String tableName = table.getTableName();

            // Finally, tell the metastore what has changed
            long writeId = transaction.getWriteId();
            long transactionId = transaction.getAcidTransactionId();
            updateTableWriteId(identity, databaseName, tableName, transactionId, writeId, OptionalLong.empty());
        }

        private void prepareDropPartition(HiveIdentity identity, SchemaTableName schemaTableName, List<String> partitionValues, boolean deleteData)
        {
            metastoreDeleteOperations.add(new IrreversibleMetastoreOperation(
                    format("drop partition %s.%s %s", schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionValues),
                    () -> delegate.dropPartition(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionValues, deleteData)));
        }

        private void prepareAlterPartition(HdfsContext hdfsContext, HiveIdentity identity, String queryId, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.getPartition();
            String targetLocation = partition.getStorage().getLocation();
            Optional<Partition> oldPartition = delegate.getPartition(identity, partition.getDatabaseName(), partition.getTableName(), partition.getValues());
            if (oldPartition.isEmpty()) {
                throw new TrinoException(
                        TRANSACTION_CONFLICT,
                        format("The partition that this transaction modified was deleted in another transaction. %s %s", partition.getTableName(), partition.getValues()));
            }
            String partitionName = getPartitionName(identity, partition.getDatabaseName(), partition.getTableName(), partition.getValues());
            PartitionStatistics oldPartitionStatistics = getExistingPartitionStatistics(identity, partition, partitionName);
            String oldPartitionLocation = oldPartition.get().getStorage().getLocation();
            Path oldPartitionPath = new Path(oldPartitionLocation);

            // Location of the old partition and the new partition can be different because we allow arbitrary directories through LocationService.
            // If the location of the old partition is the same as the location of the new partition:
            // * Rename the old data directory to a temporary path with a special suffix
            // * Remember we will need to delete that directory at the end if transaction successfully commits
            // * Remember we will need to undo the rename if transaction aborts
            // Otherwise,
            // * Remember we will need to delete the location of the old partition at the end if transaction successfully commits
            if (targetLocation.equals(oldPartitionLocation)) {
                Path oldPartitionStagingPath = new Path(oldPartitionPath.getParent(), "_temp_" + oldPartitionPath.getName() + "_" + queryId);
                renameDirectory(
                        hdfsContext,
                        hdfsEnvironment,
                        oldPartitionPath,
                        oldPartitionStagingPath,
                        () -> renameTasksForAbort.add(new DirectoryRenameTask(hdfsContext, oldPartitionStagingPath, oldPartitionPath)));
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(hdfsContext, oldPartitionStagingPath));
                }
            }
            else {
                if (!skipDeletionForAlter) {
                    deletionTasksForFinish.add(new DirectoryDeletionTask(hdfsContext, oldPartitionPath));
                }
            }

            Path currentPath = partitionAndMore.getCurrentLocation();
            Path targetPath = new Path(targetLocation);
            if (!targetPath.equals(currentPath)) {
                renameDirectory(
                        hdfsContext,
                        hdfsEnvironment,
                        currentPath,
                        targetPath,
                        () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(hdfsContext, targetPath, true)));
            }
            // Partition alter must happen regardless of whether original and current location is the same
            // because metadata might change: e.g. storage format, column types, etc
            alterPartitionOperations.add(new AlterPartitionOperation(
                    partitionAndMore.getIdentity(),
                    new PartitionWithStatistics(partition, partitionName, partitionAndMore.getStatisticsUpdate()),
                    new PartitionWithStatistics(oldPartition.get(), partitionName, oldPartitionStatistics)));
        }

        private PartitionStatistics getExistingPartitionStatistics(HiveIdentity identity, Partition partition, String partitionName)
        {
            try {
                PartitionStatistics statistics = delegate.getPartitionStatistics(identity, partition.getDatabaseName(), partition.getTableName(), ImmutableSet.of(partitionName))
                        .get(partitionName);
                if (statistics == null) {
                    throw new TrinoException(
                            TRANSACTION_CONFLICT,
                            format("The partition that this transaction modified was deleted in another transaction. %s %s", partition.getTableName(), partition.getValues()));
                }
                return statistics;
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

        private void prepareAddPartition(HdfsContext hdfsContext, HiveIdentity identity, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.getPartition();
            String targetLocation = partition.getStorage().getLocation();
            Path currentPath = partitionAndMore.getCurrentLocation();
            Path targetPath = new Path(targetLocation);

            PartitionAdder partitionAdder = partitionAdders.computeIfAbsent(
                    partition.getSchemaTableName(),
                    ignored -> new PartitionAdder(partitionAndMore.getIdentity(), partition.getDatabaseName(), partition.getTableName(), delegate, PARTITION_COMMIT_BATCH_SIZE));

            if (pathExists(hdfsContext, hdfsEnvironment, currentPath)) {
                if (!targetPath.equals(currentPath)) {
                    renameDirectory(
                            hdfsContext,
                            hdfsEnvironment,
                            currentPath,
                            targetPath,
                            () -> cleanUpTasksForAbort.add(new DirectoryCleanUpTask(hdfsContext, targetPath, true)));
                }
            }
            else {
                cleanUpTasksForAbort.add(new DirectoryCleanUpTask(hdfsContext, targetPath, true));
                createDirectory(hdfsContext, hdfsEnvironment, targetPath);
            }
            String partitionName = getPartitionName(identity, partition.getDatabaseName(), partition.getTableName(), partition.getValues());
            partitionAdder.addPartition(new PartitionWithStatistics(partition, partitionName, partitionAndMore.getStatisticsUpdate()));
        }

        private void prepareInsertExistingPartition(HdfsContext hdfsContext, HiveIdentity identity, PartitionAndMore partitionAndMore)
        {
            deleteOnly = false;

            Partition partition = partitionAndMore.getPartition();
            Path targetPath = new Path(partition.getStorage().getLocation());
            Path currentPath = partitionAndMore.getCurrentLocation();
            cleanUpTasksForAbort.add(new DirectoryCleanUpTask(hdfsContext, targetPath, false));
            if (!targetPath.equals(currentPath)) {
                asyncRename(hdfsEnvironment, renameExecutor, fileRenameCancelled, fileRenameFutures, hdfsContext, currentPath, targetPath, partitionAndMore.getFileNames());
            }

            updateStatisticsOperations.add(new UpdateStatisticsOperation(
                    partitionAndMore.getIdentity(),
                    partition.getSchemaTableName(),
                    Optional.of(getPartitionName(identity, partition.getDatabaseName(), partition.getTableName(), partition.getValues())),
                    partitionAndMore.getStatisticsUpdate(),
                    true));
        }

        private void executeCleanupTasksForAbort(Collection<DeclaredIntentionToWrite> declaredIntentionsToWrite)
        {
            Set<String> queryIds = declaredIntentionsToWrite.stream()
                    .map(DeclaredIntentionToWrite::getQueryId)
                    .collect(toImmutableSet());
            for (DirectoryCleanUpTask cleanUpTask : cleanUpTasksForAbort) {
                recursiveDeleteFilesAndLog(cleanUpTask.getContext(), cleanUpTask.getPath(), queryIds, cleanUpTask.isDeleteEmptyDirectory(), "temporary directory commit abort");
            }
        }

        private void executeDeletionTasksForFinish()
        {
            for (DirectoryDeletionTask deletionTask : deletionTasksForFinish) {
                if (!deleteRecursivelyIfExists(deletionTask.getContext(), hdfsEnvironment, deletionTask.getPath())) {
                    logCleanupFailure("Error deleting directory %s", deletionTask.getPath().toString());
                }
            }
        }

        private void executeRenameTasksForAbort()
        {
            for (DirectoryRenameTask directoryRenameTask : renameTasksForAbort) {
                try {
                    // Ignore the task if the source directory doesn't exist.
                    // This is probably because the original rename that we are trying to undo here never succeeded.
                    if (pathExists(directoryRenameTask.getContext(), hdfsEnvironment, directoryRenameTask.getRenameFrom())) {
                        renameDirectory(directoryRenameTask.getContext(), hdfsEnvironment, directoryRenameTask.getRenameFrom(), directoryRenameTask.getRenameTo(), () -> {});
                    }
                }
                catch (Throwable throwable) {
                    logCleanupFailure(throwable, "failed to undo rename of partition directory: %s to %s", directoryRenameTask.getRenameFrom(), directoryRenameTask.getRenameTo());
                }
            }
        }

        private void deleteEmptyStagingDirectories(List<DeclaredIntentionToWrite> declaredIntentionsToWrite)
        {
            for (DeclaredIntentionToWrite declaredIntentionToWrite : declaredIntentionsToWrite) {
                if (declaredIntentionToWrite.getMode() != WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY) {
                    continue;
                }
                Path path = declaredIntentionToWrite.getRootPath();
                recursiveDeleteFilesAndLog(declaredIntentionToWrite.getHdfsContext(), path, ImmutableSet.of(), true, "staging directory cleanup");
            }
        }

        private void waitForAsyncRenames()
        {
            for (CompletableFuture<?> fileRenameFuture : fileRenameFutures) {
                getFutureValue(fileRenameFuture, TrinoException.class);
            }
        }

        private void waitForAsyncRenamesSuppressThrowables()
        {
            for (CompletableFuture<?> future : fileRenameFutures) {
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

        private void cancelUnstartedAsyncRenames()
        {
            fileRenameCancelled.set(true);
        }

        private void executeAddTableOperations()
        {
            for (CreateTableOperation addTableOperation : addTableOperations) {
                addTableOperation.run(delegate);
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
                            addSuppressedExceptions(suppressedExceptions, t, failedIrreversibleOperationDescriptions, irreversibleMetastoreOperation.getDescription());
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

    @GuardedBy("this")
    private void rollbackShared()
    {
        checkHoldsLock();

        for (DeclaredIntentionToWrite declaredIntentionToWrite : declaredIntentionsToWrite) {
            switch (declaredIntentionToWrite.getMode()) {
                case STAGE_AND_MOVE_TO_TARGET_DIRECTORY:
                case DIRECT_TO_TARGET_NEW_DIRECTORY:
                    // For STAGE_AND_MOVE_TO_TARGET_DIRECTORY, there is no need to cleanup the target directory as
                    // it will only be written to during the commit call and the commit call cleans up after failures.
                    if ((declaredIntentionToWrite.getMode() == DIRECT_TO_TARGET_NEW_DIRECTORY) && skipTargetCleanupOnRollback) {
                        break;
                    }

                    Path rootPath = declaredIntentionToWrite.getRootPath();

                    // In the case of DIRECT_TO_TARGET_NEW_DIRECTORY, if the directory is not guaranteed to be unique
                    // for the query, it is possible that another query or compute engine may see the directory, wrote
                    // data to it, and exported it through metastore. Therefore it may be argued that cleanup of staging
                    // directories must be carried out conservatively. To be safe, we only delete files that start or
                    // end with the query IDs in this transaction.
                    recursiveDeleteFilesAndLog(
                            declaredIntentionToWrite.getHdfsContext(),
                            rootPath,
                            ImmutableSet.of(declaredIntentionToWrite.getQueryId()),
                            true,
                            format("staging/target_new directory rollback for table %s", declaredIntentionToWrite.getSchemaTableName()));
                    break;
                case DIRECT_TO_TARGET_EXISTING_DIRECTORY:
                    Set<Path> pathsToClean = new HashSet<>();

                    // Check the base directory of the declared intention
                    // * existing partition may also be in this directory
                    // * this is where new partitions are created
                    Path baseDirectory = declaredIntentionToWrite.getRootPath();
                    pathsToClean.add(baseDirectory);

                    HiveIdentity identity = declaredIntentionToWrite.getIdentity();
                    SchemaTableName schemaTableName = declaredIntentionToWrite.getSchemaTableName();
                    Optional<Table> table = delegate.getTable(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName());
                    if (table.isPresent()) {
                        // check every existing partition that is outside for the base directory
                        List<Column> partitionColumns = table.get().getPartitionColumns();
                        if (!partitionColumns.isEmpty()) {
                            List<String> partitionColumnNames = partitionColumns.stream()
                                    .map(Column::getName)
                                    .collect(toImmutableList());
                            List<String> partitionNames = delegate.getPartitionNamesByFilter(
                                    identity, schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionColumnNames, TupleDomain.all())
                                    .orElse(ImmutableList.of());
                            for (List<String> partitionNameBatch : Iterables.partition(partitionNames, 10)) {
                                Collection<Optional<Partition>> partitions = delegate.getPartitionsByNames(identity, schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionNameBatch).values();
                                partitions.stream()
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .map(partition -> partition.getStorage().getLocation())
                                        .map(Path::new)
                                        .filter(path -> !isSameOrParent(baseDirectory, path))
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
                    for (Path path : pathsToClean) {
                        // TODO: It is a known deficiency that some empty directory does not get cleaned up in S3.
                        // We cannot delete any of the directories here since we do not know who created them.
                        recursiveDeleteFilesAndLog(
                                declaredIntentionToWrite.getHdfsContext(),
                                path,
                                ImmutableSet.of(declaredIntentionToWrite.getQueryId()),
                                false,
                                format("target_existing directory rollback for table %s", schemaTableName));
                    }

                    break;
                default:
                    throw new UnsupportedOperationException("Unknown write mode");
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

    @VisibleForTesting
    public void testOnlyThrowOnCleanupFailures()
    {
        throwOnCleanupFailure = true;
    }

    @GuardedBy("this")
    private void checkReadable()
    {
        checkHoldsLock();

        switch (state) {
            case EMPTY:
            case SHARED_OPERATION_BUFFERED:
                return;
            case EXCLUSIVE_OPERATION_BUFFERED:
                throw new TrinoException(NOT_SUPPORTED, "Unsupported combination of operations in a single transaction");
            case FINISHED:
                throw new IllegalStateException("Tried to access metastore after transaction has been committed/aborted");
        }
    }

    @GuardedBy("this")
    private void setShared()
    {
        checkHoldsLock();

        checkReadable();
        state = State.SHARED_OPERATION_BUFFERED;
    }

    @GuardedBy("this")
    private void setExclusive(ExclusiveOperation exclusiveOperation)
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

    private static boolean isSameOrParent(Path parent, Path child)
    {
        int parentDepth = parent.depth();
        int childDepth = child.depth();
        if (parentDepth > childDepth) {
            return false;
        }
        for (int i = childDepth; i > parentDepth; i--) {
            child = child.getParent();
        }
        return parent.equals(child);
    }

    @FormatMethod
    private void logCleanupFailure(String format, Object... args)
    {
        if (throwOnCleanupFailure) {
            throw new RuntimeException(format(format, args));
        }
        log.warn(format, args);
    }

    @FormatMethod
    private void logCleanupFailure(Throwable t, String format, Object... args)
    {
        if (throwOnCleanupFailure) {
            throw new RuntimeException(format(format, args), t);
        }
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
            HdfsEnvironment hdfsEnvironment,
            Executor executor,
            AtomicBoolean cancelled,
            List<CompletableFuture<?>> fileRenameFutures,
            HdfsContext context,
            Path currentPath,
            Path targetPath,
            List<String> fileNames)
    {
        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(context, currentPath);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error moving data files to final location. Error listing directory %s", currentPath), e);
        }

        for (String fileName : fileNames) {
            Path source = new Path(currentPath, fileName);
            Path target = new Path(targetPath, fileName);
            fileRenameFutures.add(CompletableFuture.runAsync(() -> {
                if (cancelled.get()) {
                    return;
                }
                try {
                    if (fileSystem.exists(target) || !fileSystem.rename(source, target)) {
                        throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error moving data files from %s to final location %s", source, target));
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error moving data files from %s to final location %s", source, target), e);
                }
            }, executor));
        }
    }

    private void recursiveDeleteFilesAndLog(HdfsContext context, Path directory, Set<String> queryIds, boolean deleteEmptyDirectories, String reason)
    {
        RecursiveDeleteResult recursiveDeleteResult = recursiveDeleteFiles(
                hdfsEnvironment,
                context,
                directory,
                queryIds,
                deleteEmptyDirectories);
        if (!recursiveDeleteResult.getNotDeletedEligibleItems().isEmpty()) {
            logCleanupFailure(
                    "Error deleting directory %s for %s. Some eligible items cannot be deleted: %s.",
                    directory.toString(),
                    reason,
                    recursiveDeleteResult.getNotDeletedEligibleItems());
        }
        else if (deleteEmptyDirectories && !recursiveDeleteResult.isDirectoryNoLongerExists()) {
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
    private static RecursiveDeleteResult recursiveDeleteFiles(HdfsEnvironment hdfsEnvironment, HdfsContext context, Path directory, Set<String> queryIds, boolean deleteEmptyDirectories)
    {
        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(context, directory);

            if (!fileSystem.exists(directory)) {
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

    private static RecursiveDeleteResult doRecursiveDeleteFiles(FileSystem fileSystem, Path directory, Set<String> queryIds, boolean deleteEmptyDirectories)
    {
        // don't delete hidden Trino directories use by FileHiveMetastore
        if (directory.getName().startsWith(".trino")) {
            return new RecursiveDeleteResult(false, ImmutableList.of());
        }

        FileStatus[] allFiles;
        try {
            allFiles = fileSystem.listStatus(directory);
        }
        catch (IOException e) {
            ImmutableList.Builder<String> notDeletedItems = ImmutableList.builder();
            notDeletedItems.add(directory.toString() + "/**");
            return new RecursiveDeleteResult(false, notDeletedItems.build());
        }

        boolean allDescendentsDeleted = true;
        ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
        for (FileStatus fileStatus : allFiles) {
            if (fileStatus.isFile()) {
                Path filePath = fileStatus.getPath();
                String fileName = filePath.getName();
                boolean eligible = false;
                // don't delete hidden Trino directories use by FileHiveMetastore
                if (!fileName.startsWith(".trino")) {
                    eligible = queryIds.stream().anyMatch(id -> isFileCreatedByQuery(fileName, id));
                }
                if (eligible) {
                    if (!deleteIfExists(fileSystem, filePath, false)) {
                        allDescendentsDeleted = false;
                        notDeletedEligibleItems.add(filePath.toString());
                    }
                }
                else {
                    allDescendentsDeleted = false;
                }
            }
            else if (fileStatus.isDirectory()) {
                RecursiveDeleteResult subResult = doRecursiveDeleteFiles(fileSystem, fileStatus.getPath(), queryIds, deleteEmptyDirectories);
                if (!subResult.isDirectoryNoLongerExists()) {
                    allDescendentsDeleted = false;
                }
                if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                    notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
                }
            }
            else {
                allDescendentsDeleted = false;
                notDeletedEligibleItems.add(fileStatus.getPath().toString());
            }
        }
        // Unconditionally delete empty delta_ and delete_delta_ directories, because that's
        // what Hive does, and leaving them in place confuses delta file readers.
        if (allDescendentsDeleted && (deleteEmptyDirectories || DELTA_DIRECTORY_MATCHER.matcher(directory.getName()).matches())) {
            verify(notDeletedEligibleItems.build().isEmpty());
            if (!deleteIfExists(fileSystem, directory, false)) {
                return new RecursiveDeleteResult(false, ImmutableList.of(directory.toString() + "/"));
            }
            return new RecursiveDeleteResult(true, ImmutableList.of());
        }
        return new RecursiveDeleteResult(false, notDeletedEligibleItems.build());
    }

    /**
     * Attempts to remove the file or empty directory.
     *
     * @return true if the location no longer exists
     */
    private static boolean deleteIfExists(FileSystem fileSystem, Path path, boolean recursive)
    {
        try {
            // attempt to delete the path
            if (fileSystem.delete(path, recursive)) {
                return true;
            }

            // delete failed
            // check if path still exists
            return !fileSystem.exists(path);
        }
        catch (FileNotFoundException ignored) {
            // path was already removed or never existed
            return true;
        }
        catch (IOException ignored) {
        }
        return false;
    }

    /**
     * Attempts to remove the file or empty directory.
     *
     * @return true if the location no longer exists
     */
    private static boolean deleteRecursivelyIfExists(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(context, path);
        }
        catch (IOException ignored) {
            return false;
        }

        return deleteIfExists(fileSystem, path, true);
    }

    private static void renameDirectory(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path source, Path target, Runnable runWhenPathDoesntExist)
    {
        if (pathExists(context, hdfsEnvironment, target)) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS,
                    format("Unable to rename from %s to %s: target directory already exists", source, target));
        }

        if (!pathExists(context, hdfsEnvironment, target.getParent())) {
            createDirectory(context, hdfsEnvironment, target.getParent());
        }

        // The runnable will assume that if rename fails, it will be okay to delete the directory (if the directory is empty).
        // This is not technically true because a race condition still exists.
        runWhenPathDoesntExist.run();

        try {
            if (!hdfsEnvironment.getFileSystem(context, source).rename(source, target)) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Failed to rename %s to %s: rename returned false", source, target));
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Failed to rename %s to %s", source, target), e);
        }
    }

    private static Optional<String> getPrestoQueryId(Table table)
    {
        return Optional.ofNullable(table.getParameters().get(PRESTO_QUERY_ID_NAME));
    }

    private static Optional<String> getPrestoQueryId(Partition partition)
    {
        return Optional.ofNullable(partition.getParameters().get(PRESTO_QUERY_ID_NAME));
    }

    private void checkHoldsLock()
    {
        // This method serves a similar purpose at runtime as GuardedBy on method serves during static analysis.
        // This method should not have significant performance impact. If it does, it may be reasonably to remove this method.
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
        DELETE_ROWS,
        UPDATE,
    }

    private enum TableSource
    {
        CREATED_IN_THIS_TRANSACTION,
        PRE_EXISTING_TABLE,
        // RECREATED_IN_THIS_TRANSACTION is a possible case, but it is not supported with the current implementation
    }

    public static class Action<T>
    {
        private final ActionType type;
        private final T data;
        private final HdfsContext hdfsContext;
        private final HiveIdentity identity;
        private final String queryId;

        public Action(ActionType type, T data, HdfsContext hdfsContext, HiveIdentity identity, String queryId)
        {
            this.type = requireNonNull(type, "type is null");
            if (type == ActionType.DROP || type == ActionType.DROP_PRESERVE_DATA) {
                checkArgument(data == null, "data is not null");
            }
            else {
                requireNonNull(data, "data is null");
            }
            this.data = data;
            this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
            this.identity = requireNonNull(identity, "identity is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
        }

        public ActionType getType()
        {
            return type;
        }

        public T getData()
        {
            checkState(type != ActionType.DROP);
            return data;
        }

        public HdfsContext getHdfsContext()
        {
            return hdfsContext;
        }

        public String getQueryId()
        {
            return queryId;
        }

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("queryId", queryId)
                    .add("data", data)
                    .toString();
        }
    }

    private static class TableAndMore
    {
        private final Table table;
        private final HiveIdentity identity;
        private final Optional<PrincipalPrivileges> principalPrivileges;
        private final Optional<Path> currentLocation; // unpartitioned table only
        private final Optional<List<String>> fileNames;
        private final boolean ignoreExisting;
        private final PartitionStatistics statistics;
        private final PartitionStatistics statisticsUpdate;

        public TableAndMore(
                Table table,
                HiveIdentity identity,
                Optional<PrincipalPrivileges> principalPrivileges,
                Optional<Path> currentLocation,
                Optional<List<String>> fileNames,
                boolean ignoreExisting,
                PartitionStatistics statistics,
                PartitionStatistics statisticsUpdate)
        {
            this.table = requireNonNull(table, "table is null");
            this.identity = requireNonNull(identity, "identity is null");
            this.principalPrivileges = requireNonNull(principalPrivileges, "principalPrivileges is null");
            this.currentLocation = requireNonNull(currentLocation, "currentLocation is null");
            this.fileNames = requireNonNull(fileNames, "fileNames is null");
            this.ignoreExisting = ignoreExisting;
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.statisticsUpdate = requireNonNull(statisticsUpdate, "statisticsUpdate is null");

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

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        public PrincipalPrivileges getPrincipalPrivileges()
        {
            checkState(principalPrivileges.isPresent());
            return principalPrivileges.get();
        }

        public Optional<Path> getCurrentLocation()
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
                    .toString();
        }
    }

    private static class TableAndAcidDirectories
            extends TableAndMore
    {
        private final List<PartitionAndStatementId> partitionAndStatementIds;

        public TableAndAcidDirectories(Table table, HiveIdentity identity, Optional<PrincipalPrivileges> principalPrivileges, Optional<Path> currentLocation, List<PartitionAndStatementId> partitionAndStatementIds)
        {
            super(table, identity, principalPrivileges, currentLocation, Optional.empty(), false, PartitionStatistics.empty(), PartitionStatistics.empty());
            this.partitionAndStatementIds = partitionAndStatementIds;
        }

        public List<PartitionAndStatementId> getPartitionAndStatementIds()
        {
            return partitionAndStatementIds;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("table", getTable())
                    .add("partitionAndStatementIds", partitionAndStatementIds)
                    .add("principalPrivileges", getPrincipalPrivileges())
                    .add("currentLocation", getCurrentLocation())
                    .toString();
        }
    }

    private static class PartitionAndMore
    {
        private final HiveIdentity identity;
        private final Partition partition;
        private final Path currentLocation;
        private final Optional<List<String>> fileNames;
        private final PartitionStatistics statistics;
        private final PartitionStatistics statisticsUpdate;

        public PartitionAndMore(HiveIdentity identity, Partition partition, Path currentLocation, Optional<List<String>> fileNames, PartitionStatistics statistics, PartitionStatistics statisticsUpdate)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.partition = requireNonNull(partition, "partition is null");
            this.currentLocation = requireNonNull(currentLocation, "currentLocation is null");
            this.fileNames = requireNonNull(fileNames, "fileNames is null");
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.statisticsUpdate = requireNonNull(statisticsUpdate, "statisticsUpdate is null");
        }

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        public Partition getPartition()
        {
            return partition;
        }

        public Path getCurrentLocation()
        {
            return currentLocation;
        }

        public List<String> getFileNames()
        {
            checkState(fileNames.isPresent());
            return fileNames.get();
        }

        public PartitionStatistics getStatistics()
        {
            return statistics;
        }

        public PartitionStatistics getStatisticsUpdate()
        {
            return statisticsUpdate;
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

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partition", partition)
                    .add("currentLocation", currentLocation)
                    .add("fileNames", fileNames)
                    .toString();
        }
    }

    private static class DeclaredIntentionToWrite
    {
        private final WriteMode mode;
        private final HdfsContext hdfsContext;
        private final HiveIdentity identity;
        private final String queryId;
        private final Path rootPath;
        private final SchemaTableName schemaTableName;

        public DeclaredIntentionToWrite(WriteMode mode, HdfsContext hdfsContext, HiveIdentity identity, String queryId, Path stagingPathRoot, SchemaTableName schemaTableName)
        {
            this.mode = requireNonNull(mode, "mode is null");
            this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
            this.identity = requireNonNull(identity, "identity is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.rootPath = requireNonNull(stagingPathRoot, "stagingPathRoot is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public WriteMode getMode()
        {
            return mode;
        }

        public HdfsContext getHdfsContext()
        {
            return hdfsContext;
        }

        public HiveIdentity getIdentity()
        {
            return identity;
        }

        public String getQueryId()
        {
            return queryId;
        }

        public Path getRootPath()
        {
            return rootPath;
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("mode", mode)
                    .add("hdfsContext", hdfsContext)
                    .add("identity", identity)
                    .add("queryId", queryId)
                    .add("rootPath", rootPath)
                    .add("schemaTableName", schemaTableName)
                    .toString();
        }
    }

    private static class DirectoryCleanUpTask
    {
        private final HdfsContext context;
        private final Path path;
        private final boolean deleteEmptyDirectory;

        public DirectoryCleanUpTask(HdfsContext context, Path path, boolean deleteEmptyDirectory)
        {
            this.context = context;
            this.path = path;
            this.deleteEmptyDirectory = deleteEmptyDirectory;
        }

        public HdfsContext getContext()
        {
            return context;
        }

        public Path getPath()
        {
            return path;
        }

        public boolean isDeleteEmptyDirectory()
        {
            return deleteEmptyDirectory;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("context", context)
                    .add("path", path)
                    .add("deleteEmptyDirectory", deleteEmptyDirectory)
                    .toString();
        }
    }

    private static class DirectoryDeletionTask
    {
        private final HdfsContext context;
        private final Path path;

        public DirectoryDeletionTask(HdfsContext context, Path path)
        {
            this.context = context;
            this.path = path;
        }

        public HdfsContext getContext()
        {
            return context;
        }

        public Path getPath()
        {
            return path;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("context", context)
                    .add("path", path)
                    .toString();
        }
    }

    private static class DirectoryRenameTask
    {
        private final HdfsContext context;
        private final Path renameFrom;
        private final Path renameTo;

        public DirectoryRenameTask(HdfsContext context, Path renameFrom, Path renameTo)
        {
            this.context = requireNonNull(context, "context is null");
            this.renameFrom = requireNonNull(renameFrom, "renameFrom is null");
            this.renameTo = requireNonNull(renameTo, "renameTo is null");
        }

        public HdfsContext getContext()
        {
            return context;
        }

        public Path getRenameFrom()
        {
            return renameFrom;
        }

        public Path getRenameTo()
        {
            return renameTo;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("context", context)
                    .add("renameFrom", renameFrom)
                    .add("renameTo", renameTo)
                    .toString();
        }
    }

    private static class IrreversibleMetastoreOperation
    {
        private final String description;
        private final Runnable action;

        public IrreversibleMetastoreOperation(String description, Runnable action)
        {
            this.description = requireNonNull(description, "description is null");
            this.action = requireNonNull(action, "action is null");
        }

        public String getDescription()
        {
            return description;
        }

        public void run()
        {
            action.run();
        }
    }

    private static class CreateTableOperation
    {
        private final HiveIdentity identity;
        private final Table newTable;
        private final PrincipalPrivileges privileges;
        private boolean tableCreated;
        private final boolean ignoreExisting;
        private final String queryId;

        public CreateTableOperation(HiveIdentity identity, Table newTable, PrincipalPrivileges privileges, boolean ignoreExisting)
        {
            this.identity = requireNonNull(identity, "identity is null");
            requireNonNull(newTable, "newTable is null");
            this.newTable = newTable;
            this.privileges = requireNonNull(privileges, "privileges is null");
            this.ignoreExisting = ignoreExisting;
            this.queryId = getPrestoQueryId(newTable).orElseThrow(() -> new IllegalArgumentException("Query id is not present"));
        }

        public String getDescription()
        {
            return format("add table %s.%s", newTable.getDatabaseName(), newTable.getTableName());
        }

        public void run(HiveMetastoreClosure metastore)
        {
            try {
                metastore.createTable(identity, newTable, privileges);
            }
            catch (RuntimeException e) {
                boolean done = false;
                try {
                    Optional<Table> existingTable = metastore.getTable(identity, newTable.getDatabaseName(), newTable.getTableName());
                    if (existingTable.isPresent()) {
                        Table table = existingTable.get();
                        Optional<String> existingTableQueryId = getPrestoQueryId(table);
                        if (existingTableQueryId.isPresent() && existingTableQueryId.get().equals(queryId)) {
                            // ignore table if it was already created by the same query during retries
                            done = true;
                        }
                        else {
                            // If the table definition in the metastore is different than what this tx wants to create
                            // then there is a conflict (e.g., current tx wants to create T(a: bigint),
                            // but another tx already created T(a: varchar)).
                            // This may be a problem if there is an insert after this step.
                            if (!hasTheSameSchema(newTable, table)) {
                                e = new TrinoException(TRANSACTION_CONFLICT, format("Table already exists with a different schema: '%s'", newTable.getTableName()));
                            }
                            else {
                                done = ignoreExisting;
                            }
                        }
                    }
                }
                catch (RuntimeException ignored) {
                    // When table could not be fetched from metastore, it is not known whether the table was added.
                    // Deleting the table when aborting commit has the risk of deleting table not added in this transaction.
                    // Not deleting the table may leave garbage behind. The former is much more dangerous than the latter.
                    // Therefore, the table is not considered added.
                }

                if (!done) {
                    throw e;
                }
            }
            tableCreated = true;
        }

        private boolean hasTheSameSchema(Table newTable, Table existingTable)
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

        public void undo(HiveMetastoreClosure metastore)
        {
            if (!tableCreated) {
                return;
            }
            metastore.dropTable(identity, newTable.getDatabaseName(), newTable.getTableName(), false);
        }
    }

    private static class AlterTableOperation
    {
        private final HiveIdentity identity;
        private final Table newTable;
        private final Table oldTable;
        private final PrincipalPrivileges principalPrivileges;
        private boolean undo;

        public AlterTableOperation(HiveIdentity identity, Table newTable, Table oldTable, PrincipalPrivileges principalPrivileges)
        {
            this.identity = requireNonNull(identity, "identity is null");
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

        public void run(HiveMetastoreClosure metastore, AcidTransaction transaction)
        {
            undo = true;
            if (transaction.isTransactional()) {
                metastore.alterTransactionalTable(identity, newTable, transaction.getAcidTransactionId(), transaction.getWriteId(), principalPrivileges);
            }
            else {
                metastore.replaceTable(identity, newTable.getDatabaseName(), newTable.getTableName(), newTable, principalPrivileges);
            }
        }

        public void undo(HiveMetastoreClosure metastore, AcidTransaction transaction)
        {
            if (!undo) {
                return;
            }

            if (transaction.isTransactional()) {
                metastore.alterTransactionalTable(identity, oldTable, transaction.getAcidTransactionId(), transaction.getWriteId(), principalPrivileges);
            }
            else {
                metastore.replaceTable(identity, oldTable.getDatabaseName(), oldTable.getTableName(), oldTable, principalPrivileges);
            }
        }
    }

    private static class AlterPartitionOperation
    {
        private final HiveIdentity identity;
        private final PartitionWithStatistics newPartition;
        private final PartitionWithStatistics oldPartition;
        private boolean undo;

        public AlterPartitionOperation(HiveIdentity identity, PartitionWithStatistics newPartition, PartitionWithStatistics oldPartition)
        {
            this.identity = requireNonNull(identity, "identity is null");
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

        public void run(HiveMetastoreClosure metastore)
        {
            undo = true;
            metastore.alterPartition(identity, newPartition.getPartition().getDatabaseName(), newPartition.getPartition().getTableName(), newPartition);
        }

        public void undo(HiveMetastoreClosure metastore)
        {
            if (!undo) {
                return;
            }
            metastore.alterPartition(identity, oldPartition.getPartition().getDatabaseName(), oldPartition.getPartition().getTableName(), oldPartition);
        }
    }

    private static class UpdateStatisticsOperation
    {
        private final HiveIdentity identity;
        private final SchemaTableName tableName;
        private final Optional<String> partitionName;
        private final PartitionStatistics statistics;
        private final boolean merge;

        private boolean done;

        public UpdateStatisticsOperation(HiveIdentity identity, SchemaTableName tableName, Optional<String> partitionName, PartitionStatistics statistics, boolean merge)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.partitionName = requireNonNull(partitionName, "partitionName is null");
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HiveMetastoreClosure metastore, AcidTransaction transaction)
        {
            if (partitionName.isPresent()) {
                metastore.updatePartitionStatistics(identity, tableName.getSchemaName(), tableName.getTableName(), partitionName.get(), this::updateStatistics);
            }
            else {
                metastore.updateTableStatistics(identity, tableName.getSchemaName(), tableName.getTableName(), transaction, this::updateStatistics);
            }
            done = true;
        }

        public void undo(HiveMetastoreClosure metastore, AcidTransaction transaction)
        {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                metastore.updatePartitionStatistics(identity, tableName.getSchemaName(), tableName.getTableName(), partitionName.get(), this::resetStatistics);
            }
            else {
                metastore.updateTableStatistics(identity, tableName.getSchemaName(), tableName.getTableName(), transaction, this::resetStatistics);
            }
        }

        public String getDescription()
        {
            if (partitionName.isPresent()) {
                return format("replace partition parameters %s %s", tableName, partitionName.get());
            }
            return format("replace table parameters %s", tableName);
        }

        private PartitionStatistics updateStatistics(PartitionStatistics currentStatistics)
        {
            return merge ? merge(currentStatistics, statistics) : statistics;
        }

        private PartitionStatistics resetStatistics(PartitionStatistics currentStatistics)
        {
            return new PartitionStatistics(reduce(currentStatistics.getBasicStatistics(), statistics.getBasicStatistics(), SUBTRACT), ImmutableMap.of());
        }
    }

    private static class PartitionAdder
    {
        private final HiveIdentity identity;
        private final String schemaName;
        private final String tableName;
        private final HiveMetastoreClosure metastore;
        private final int batchSize;
        private final List<PartitionWithStatistics> partitions;
        private List<List<String>> createdPartitionValues = new ArrayList<>();

        public PartitionAdder(HiveIdentity identity, String schemaName, String tableName, HiveMetastoreClosure metastore, int batchSize)
        {
            this.identity = identity;
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
            checkArgument(getPrestoQueryId(partition.getPartition()).isPresent());
            partitions.add(partition);
        }

        public void execute(AcidTransaction transaction)
        {
            List<List<PartitionWithStatistics>> batchedPartitions = Lists.partition(partitions, batchSize);
            for (List<PartitionWithStatistics> batch : batchedPartitions) {
                try {
                    metastore.addPartitions(identity, schemaName, tableName, batch);
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
                            Optional<Partition> remotePartition = metastore.getPartition(identity, schemaName, tableName, partition.getPartition().getValues());
                            // getPrestoQueryId(partition) is guaranteed to be non-empty. It is asserted in PartitionAdder.addPartition.
                            if (remotePartition.isPresent() && getPrestoQueryId(remotePartition.get()).equals(getPrestoQueryId(partition.getPartition()))) {
                                createdPartitionValues.add(partition.getPartition().getValues());
                            }
                            else {
                                batchCompletelyAdded = false;
                            }
                        }
                        catch (Throwable ignored) {
                            // When partition could not be fetched from metastore, it is not known whether the partition was added.
                            // Deleting the partition when aborting commit has the risk of deleting partition not added in this transaction.
                            // Not deleting the partition may leave garbage behind. The former is much more dangerous than the latter.
                            // Therefore, the partition is not added to the createdPartitionValues list here.
                            batchCompletelyAdded = false;
                        }
                    }
                    // If all the partitions were added successfully, the add_partition operation was actually successful.
                    // For some reason, it threw an exception (communication failure, retry failure after communication failure, etc).
                    // But we would consider it successful anyways.
                    if (!batchCompletelyAdded) {
                        if (t instanceof TableNotFoundException) {
                            throw new TrinoException(HIVE_TABLE_DROPPED_DURING_QUERY, t);
                        }
                        throw t;
                    }
                }
            }
            if (transaction.isAcidTransactionRunning()) {
                List<String> partitionNames = partitions.stream().map(PartitionWithStatistics::getPartitionName).collect(Collectors.toUnmodifiableList());
                metastore.addDynamicPartitions(identity, schemaName, tableName, partitionNames, transaction.getAcidTransactionId(), transaction.getWriteId(), transaction.getOperation());
            }
            partitions.clear();
        }

        public List<List<String>> rollback()
        {
            // drop created partitions
            List<List<String>> partitionsFailedToRollback = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    metastore.dropPartition(identity, schemaName, tableName, createdPartitionValue, false);
                }
                catch (PartitionNotFoundException e) {
                    // Maybe some one deleted the partition we added.
                    // Anyways, we are good because the partition is not there anymore.
                }
                catch (Throwable t) {
                    partitionsFailedToRollback.add(createdPartitionValue);
                }
            }
            createdPartitionValues = partitionsFailedToRollback;
            return partitionsFailedToRollback;
        }
    }

    private static class RecursiveDeleteResult
    {
        private final boolean directoryNoLongerExists;
        private final List<String> notDeletedEligibleItems;

        public RecursiveDeleteResult(boolean directoryNoLongerExists, List<String> notDeletedEligibleItems)
        {
            this.directoryNoLongerExists = directoryNoLongerExists;
            this.notDeletedEligibleItems = notDeletedEligibleItems;
        }

        public boolean isDirectoryNoLongerExists()
        {
            return directoryNoLongerExists;
        }

        public List<String> getNotDeletedEligibleItems()
        {
            return notDeletedEligibleItems;
        }
    }

    private interface ExclusiveOperation
    {
        void execute(HiveMetastoreClosure delegate, HdfsEnvironment hdfsEnvironment);
    }

    public long allocateWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(identity, dbName, tableName, transactionId);
    }

    public void acquireTableWriteLock(HiveIdentity identity, String queryId, long transactionId, String dbName, String tableName, DataOperationType operation, boolean isPartitioned)
    {
        delegate.acquireTableWriteLock(identity, queryId, transactionId, dbName, tableName, operation, isPartitioned);
    }

    public void updateTableWriteId(HiveIdentity identity, String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(identity, dbName, tableName, transactionId, writeId, rowCountChange);
    }

    public void alterPartitions(HiveIdentity identity, String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        delegate.alterPartitions(identity, dbName, tableName, partitions, writeId);
    }

    public void addDynamicPartitions(HiveIdentity identity, String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        delegate.addDynamicPartitions(identity, dbName, tableName, partitionNames, transactionId, writeId, operation);
    }

    public void commitTransaction(HiveIdentity identity, long transactionId)
    {
        delegate.commitTransaction(identity, transactionId);
    }
}
