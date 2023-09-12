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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hive.thrift.metastore.AlreadyExistsException;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.ConfigValSecurityException;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.EnvironmentContext;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.HiveObjectPrivilege;
import io.trino.hive.thrift.metastore.HiveObjectRef;
import io.trino.hive.thrift.metastore.InvalidInputException;
import io.trino.hive.thrift.metastore.InvalidObjectException;
import io.trino.hive.thrift.metastore.InvalidOperationException;
import io.trino.hive.thrift.metastore.LockComponent;
import io.trino.hive.thrift.metastore.LockLevel;
import io.trino.hive.thrift.metastore.LockRequest;
import io.trino.hive.thrift.metastore.LockResponse;
import io.trino.hive.thrift.metastore.LockState;
import io.trino.hive.thrift.metastore.LockType;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.hive.thrift.metastore.NoSuchLockException;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.NoSuchTxnException;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.PrincipalType;
import io.trino.hive.thrift.metastore.PrivilegeBag;
import io.trino.hive.thrift.metastore.PrivilegeGrantInfo;
import io.trino.hive.thrift.metastore.Table;
import io.trino.hive.thrift.metastore.TxnAbortedException;
import io.trino.hive.thrift.metastore.TxnToWriteId;
import io.trino.hive.thrift.metastore.UnknownDBException;
import io.trino.hive.thrift.metastore.UnknownTableException;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.util.RetryDriver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static io.trino.hive.thrift.metastore.HiveObjectType.TABLE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_LOCK_NOT_ACQUIRED;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.plugin.hive.metastore.MetastoreUtil.adjustRowCount;
import static io.trino.plugin.hive.metastore.MetastoreUtil.partitionKeyFilterToStringList;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.createMetastoreColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiPrincipalType;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromRolePrincipalGrants;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromTrinoPrincipalType;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getBasicStatisticsWithSparkFallback;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isAvroTableWithSchemaSet;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.parsePrivilege;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiPartition;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ThriftHiveMetastore
        implements ThriftMetastore
{
    private static final Logger log = Logger.get(ThriftHiveMetastore.class);

    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final CharMatcher DOT_MATCHER = CharMatcher.is('.');

    private final Optional<ConnectorIdentity> identity;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final IdentityAwareMetastoreClientFactory metastoreClientFactory;
    private final double backoffScaleFactor;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final Duration maxRetryTime;
    private final Duration maxWaitForLock;
    private final int maxRetries;
    private final boolean deleteFilesOnDrop;
    private final boolean translateHiveViews;
    private final boolean assumeCanonicalPartitionKeys;
    private final boolean useSparkTableStatisticsFallback;
    private final boolean batchMetadataFetchEnabled;
    private final ThriftMetastoreStats stats;
    private final ExecutorService writeStatisticsExecutor;

    public ThriftHiveMetastore(
            Optional<ConnectorIdentity> identity,
            TrinoFileSystemFactory fileSystemFactory,
            IdentityAwareMetastoreClientFactory metastoreClientFactory,
            double backoffScaleFactor,
            Duration minBackoffDelay,
            Duration maxBackoffDelay,
            Duration maxRetryTime,
            Duration maxWaitForLock,
            int maxRetries,
            boolean deleteFilesOnDrop,
            boolean translateHiveViews,
            boolean assumeCanonicalPartitionKeys,
            boolean useSparkTableStatisticsFallback,
            boolean batchMetadataFetchEnabled,
            ThriftMetastoreStats stats,
            ExecutorService writeStatisticsExecutor)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.metastoreClientFactory = requireNonNull(metastoreClientFactory, "metastoreClientFactory is null");
        this.backoffScaleFactor = backoffScaleFactor;
        this.minBackoffDelay = requireNonNull(minBackoffDelay, "minBackoffDelay is null");
        this.maxBackoffDelay = requireNonNull(maxBackoffDelay, "maxBackoffDelay is null");
        this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        this.maxWaitForLock = requireNonNull(maxWaitForLock, "maxWaitForLock is null");
        this.maxRetries = maxRetries;
        this.deleteFilesOnDrop = deleteFilesOnDrop;
        this.translateHiveViews = translateHiveViews;
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        this.useSparkTableStatisticsFallback = useSparkTableStatisticsFallback;
        this.batchMetadataFetchEnabled = batchMetadataFetchEnabled;
        this.stats = requireNonNull(stats, "stats is null");
        this.writeStatisticsExecutor = requireNonNull(writeStatisticsExecutor, "writeStatisticsExecutor is null");
    }

    @VisibleForTesting
    public ThriftMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getAllDatabases();
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(client.getDatabase(databaseName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getAllTables(databaseName);
                        }
                    });
        }
        catch (NoSuchObjectException e) {
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getTablesWithParameter", stats.getGetTablesWithParameter().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getTablesWithParameter(databaseName, parameterKey, parameterValue);
                        }
                    }));
        }
        catch (UnknownDBException e) {
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(client.getTable(databaseName, tableName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        Map<String, String> parameters = table.getParameters();
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(parameters);

        if (useSparkTableStatisticsFallback && basicStatistics.getRowCount().isEmpty()) {
            PartitionStatistics sparkTableStatistics = ThriftSparkMetastoreUtil.getTableStatistics(table);
            if (sparkTableStatistics.getBasicStatistics().getRowCount().isPresent()) {
                return sparkTableStatistics;
            }
        }

        Map<String, HiveColumnStatistics> columnStatistics = getTableColumnStatistics(table.getDbName(), table.getTableName(), dataColumns, basicStatistics.getRowCount());
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, List<String> columns, OptionalLong rowCount)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getTableColumnStatistics", stats.getGetTableColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return groupStatisticsByColumn(client.getTableColumnStatistics(databaseName, tableName, columns), rowCount);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<String> partitionColumns = table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());

        Map<String, HiveBasicStatistics> partitionBasicStatistics = partitions.stream()
                .collect(toImmutableMap(
                        partition -> makePartName(partitionColumns, partition.getValues()),
                        partition -> {
                            if (useSparkTableStatisticsFallback) {
                                return getBasicStatisticsWithSparkFallback(partition.getParameters());
                            }
                            return getHiveBasicStatistics(partition.getParameters());
                        }));
        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));

        long tableRowCount = partitionRowCounts.values().stream()
                .mapToLong(count -> count.orElse(0))
                .sum();
        if (!partitionRowCounts.isEmpty() && tableRowCount == 0) {
            // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
            // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
            // used to ingest data into partitioned hive tables.
            // https://github.com/trinodb/trino/issues/18798 Hive Metastore assumes any new partition statistics to at least have all parameters that the partition used to have
            partitionBasicStatistics = partitionBasicStatistics.entrySet().stream()
                    .map(entry -> new SimpleEntry<>(
                            entry.getKey(),
                            entry.getValue().withEmptyRowCount()))
                    .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
        }

        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = getPartitionColumnStatistics(
                table.getDbName(),
                table.getTableName(),
                partitionBasicStatistics.keySet(),
                dataColumns,
                partitionRowCounts);
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (String partitionName : partitionBasicStatistics.keySet()) {
            HiveBasicStatistics basicStatistics = partitionBasicStatistics.get(partitionName);
            Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStatistics.getOrDefault(partitionName, ImmutableMap.of());
            result.put(partitionName, new PartitionStatistics(basicStatistics, columnStatistics));
        }

        return result.buildOrThrow();
    }

    @Override
    public Optional<List<FieldSchema>> getFields(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(MetaException.class, UnknownTableException.class, UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getFields", stats.getGetFields().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(ImmutableList.copyOf(client.getFields(databaseName, tableName)));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            List<String> columnNames,
            Map<String, OptionalLong> partitionRowCounts)
    {
        return getMetastorePartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames).entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
    }

    private Map<String, List<ColumnStatisticsObj>> getMetastorePartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, List<String> columnNames)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionColumnStatistics", stats.getGetPartitionColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getPartitionColumnStatistics(databaseName, tableName, ImmutableList.copyOf(partitionNames), columnNames);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private static Map<String, HiveColumnStatistics> groupStatisticsByColumn(List<ColumnStatisticsObj> statistics, OptionalLong rowCount)
    {
        Map<String, HiveColumnStatistics> statisticsByColumn = new HashMap<>();
        for (ColumnStatisticsObj stats : statistics) {
            HiveColumnStatistics newColumnStatistics = ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(stats, rowCount);
            if (statisticsByColumn.containsKey(stats.getColName())) {
                HiveColumnStatistics existingColumnStatistics = statisticsByColumn.get(stats.getColName());
                if (!newColumnStatistics.equals(existingColumnStatistics)) {
                    log.warn("Ignore inconsistent statistics in %s column: %s and %s", stats.getColName(), newColumnStatistics, existingColumnStatistics);
                }
            }
            else {
                statisticsByColumn.put(stats.getColName(), newColumnStatistics);
            }
        }
        return ImmutableMap.copyOf(statisticsByColumn);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table originalTable = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        PartitionStatistics currentStatistics = getTableStatistics(originalTable);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Table modifiedTable = originalTable.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        if (transaction.isAcidTransactionRunning()) {
            modifiedTable.setWriteId(transaction.getWriteId());
        }
        alterTable(databaseName, tableName, modifiedTable);

        io.trino.plugin.hive.metastore.Table table = fromMetastoreApiTable(modifiedTable);
        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
                .flatMap(entry -> {
                    Optional<Column> column = table.getColumn(entry.getKey());
                    if (column.isEmpty() && isAvroTableWithSchemaSet(modifiedTable)) {
                        // Avro table can have different effective schema than declared in metastore. Still, metastore does not allow
                        // to store statistics for a column it does not know about.
                        return Stream.of();
                    }

                    HiveType type = column.orElseThrow(() -> new IllegalStateException("Column not found: " + entry.getKey())).getType();
                    return Stream.of(createMetastoreColumnStatistics(entry.getKey(), type, entry.getValue(), rowCount));
                })
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setTableColumnStatistics(databaseName, tableName, metastoreColumnStatistics);
        }
        Set<String> removedColumnStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedColumnStatistics.forEach(column -> deleteTableColumnStatistics(databaseName, tableName, column));
    }

    private void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setTableColumnStatistics", stats.getSetTableColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.setTableColumnStatistics(databaseName, tableName, statistics);
                            return null;
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deleteTableColumnStatistics", stats.getDeleteTableColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.deleteTableColumnStatistics(databaseName, tableName, columnName);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        List<Partition> partitions = getPartitionsByNames(table.getDbName(), table.getTableName(), ImmutableList.of(partitionName));
        if (partitions.isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "No partition found for name: " + partitionName);
        }
        if (partitions.size() != 1) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Metastore returned multiple partitions for name: " + partitionName);
        }
        Partition originalPartition = getOnlyElement(partitions);

        PartitionStatistics currentStatistics = requireNonNull(
                getPartitionStatistics(table, partitions).get(partitionName), "getPartitionStatistics() did not return statistics for partition");
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        Partition modifiedPartition = originalPartition.deepCopy();
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedPartition.setParameters(updateStatisticsParameters(modifiedPartition.getParameters(), basicStatistics));
        alterPartitionWithoutStatistics(table.getDbName(), table.getTableName(), modifiedPartition);

        Map<String, HiveType> columns = modifiedPartition.getSd().getCols().stream()
                .collect(toImmutableMap(FieldSchema::getName, schema -> HiveType.valueOf(schema.getType())));
        setPartitionColumnStatistics(table.getDbName(), table.getTableName(), partitionName, columns, updatedStatistics.getColumnStatistics(), basicStatistics.getRowCount());

        Set<String> removedStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedStatistics.forEach(column -> deletePartitionColumnStatistics(table.getDbName(), table.getTableName(), partitionName, column));
    }

    private void setPartitionColumnStatistics(
            String databaseName,
            String tableName,
            String partitionName,
            Map<String, HiveType> columns,
            Map<String, HiveColumnStatistics> columnStatistics,
            OptionalLong rowCount)
    {
        List<ColumnStatisticsObj> metastoreColumnStatistics = columnStatistics.entrySet().stream()
                .filter(entry -> columns.containsKey(entry.getKey()))
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), columns.get(entry.getKey()), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            setPartitionColumnStatistics(databaseName, tableName, partitionName, metastoreColumnStatistics);
        }
    }

    private void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("setPartitionColumnStatistics", stats.getSetPartitionColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidObjectException.class, MetaException.class, InvalidInputException.class)
                    .stopOnIllegalExceptions()
                    .run("deletePartitionColumnStatistics", stats.getDeletePartitionColumnStatistics().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createRole", stats.getCreateRole().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.createRole(role, grantor);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropRole(String role)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropRole", stats.getDropRole().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.dropRole(role);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<String> listRoles()
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoles", stats.getListRoles().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return ImmutableSet.copyOf(client.getRoleNames());
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                grantRole(
                        role,
                        grantee.getName(), fromTrinoPrincipalType(grantee.getType()),
                        grantor.getName(), fromTrinoPrincipalType(grantor.getType()),
                        adminOption);
            }
        }
    }

    private void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("grantRole", stats.getGrantRole().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.grantRole(role, granteeName, granteeType, grantorName, grantorType, grantOption);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                revokeRole(
                        role,
                        grantee.getName(), fromTrinoPrincipalType(grantee.getType()),
                        adminOption);
            }
        }
    }

    private void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
    {
        try {
            retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("revokeRole", stats.getRevokeRole().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.revokeRole(role, granteeName, granteeType, grantOption);
                            return null;
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listPrincipals", stats.getListGrantedPrincipals().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return fromRolePrincipalGrants(client.listGrantedPrincipals(role));
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        try {
            return retry()
                    .stopOn(MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("listRoleGrants", stats.getListRoleGrants().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return fromRolePrincipalGrants(client.listRoleGrants(principal.getName(), fromTrinoPrincipalType(principal.getType())));
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            if (translateHiveViews) {
                                return client.getAllViews(databaseName);
                            }
                            return client.getTablesWithParameter(databaseName, PRESTO_VIEW_FLAG, "true");
                        }
                    }));
        }
        catch (UnknownDBException e) {
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        if (!batchMetadataFetchEnabled) {
            return Optional.empty();
        }

        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", stats.getGetAllTables().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getAllTables();
                        }
                    }));
        }
        catch (TTransportException e) {
            log.warn(e, "Failed to get all views");
            // fallback in case of HMS error
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        // Without translateHiveViews, Hive views are represented as tables in Trino,
        // and they should not be returned from ThriftHiveMetastore.getAllViews() call
        if (!translateHiveViews) {
            return Optional.empty();
        }

        if (!batchMetadataFetchEnabled) {
            return Optional.empty();
        }

        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getAllViews();
                        }
                    }));
        }
        catch (TTransportException e) {
            log.warn(e, "Failed to get all tables");
            // fallback in case of HMS error
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        validateObjectName(database.getName());
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createDatabase", stats.getCreateDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.createDatabase(database);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidOperationException.class)
                    .stopOnIllegalExceptions()
                    .run("dropDatabase", stats.getDropDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.dropDatabase(databaseName, deleteData, false);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        if (!Objects.equals(databaseName, database.getName())) {
            validateObjectName(database.getName());
        }
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterDatabase", stats.getAlterDatabase().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.alterDatabase(databaseName, database);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void createTable(Table table)
    {
        validateObjectName(table.getTableName());
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.createTable(table);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (InvalidObjectException e) {
            boolean databaseMissing;
            try {
                databaseMissing = getDatabase(table.getDbName()).isEmpty();
            }
            catch (Exception databaseCheckException) {
                e.addSuppressed(databaseCheckException);
                databaseMissing = false; // we don't know, assume it exists for the purpose of error reporting
            }
            if (databaseMissing) {
                throw new SchemaNotFoundException(table.getDbName());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            Table table = client.getTable(databaseName, tableName);
                            client.dropTable(databaseName, tableName, deleteData);
                            String tableLocation = table.getSd().getLocation();
                            if (deleteFilesOnDrop && deleteData && isManagedTable(table) && !isNullOrEmpty(tableLocation)) {
                                deleteDirRecursive(Location.of(tableLocation));
                            }
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void deleteDirRecursive(Location path)
    {
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(
                    identity.orElseGet(() -> ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER)));
            fileSystem.deleteDirectory(path);
        }
        catch (IOException | RuntimeException e) {
            // don't fail if unable to delete path
            log.warn(e, "Failed to delete path: %s", path);
        }
    }

    private static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals(MANAGED_TABLE.name());
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table table)
    {
        if (!Objects.equals(databaseName, table.getDbName())) {
            validateObjectName(table.getDbName());
        }
        if (!Objects.equals(tableName, table.getTableName())) {
            validateObjectName(table.getTableName());
        }
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            EnvironmentContext context = new EnvironmentContext();
                            // This prevents Hive 3.x from collecting basic table stats at table creation time.
                            // These stats are not useful by themselves and can take very long time to collect when creating an
                            // external table over large data set.
                            context.setProperties(ImmutableMap.of("DO_NOT_UPDATE_STATS", "true"));
                            client.alterTableWithEnvironmentContext(databaseName, tableName, table, context);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTransactionalTable", stats.getAlterTransactionalTable().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.alterTransactionalTable(table, transactionId, writeId, new EnvironmentContext());
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        checkArgument(!columnNames.isEmpty() || partitionKeysFilter.isAll(), "must pass in all columnNames or the filter must be all");

        Optional<List<String>> parts = partitionKeyFilterToStringList(columnNames, partitionKeysFilter, assumeCanonicalPartitionKeys);
        if (parts.isEmpty()) {
            return Optional.of(ImmutableList.of());
        }

        try {
            if (partitionKeysFilter.isAll()) {
                return retry()
                        .stopOn(NoSuchObjectException.class)
                        .stopOnIllegalExceptions()
                        .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                            try (ThriftMetastoreClient client = createMetastoreClient()) {
                                return Optional.of(client.getPartitionNames(databaseName, tableName));
                            }
                        }));
            }
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesByParts().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(client.getPartitionNamesFiltered(databaseName, tableName, parts.get()));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        List<Partition> partitions = partitionsWithStatistics.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiPartition)
                .collect(toImmutableList());
        addPartitionsWithoutStatistics(databaseName, tableName, partitions);

        List<Future<?>> updateStatisticsFutures = new ArrayList<>();
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            updateStatisticsFutures.add(writeStatisticsExecutor.submit(() ->
                    storePartitionColumnStatistics(
                            databaseName,
                            tableName,
                            partitionWithStatistics.getPartitionName(),
                            partitionWithStatistics)));
        }

        try {
            updateStatisticsFutures.forEach(MoreFutures::getFutureValue);
        }
        catch (Throwable failure) {
            try {
                updateStatisticsFutures.forEach(future -> future.cancel(true));
            }
            catch (RuntimeException e) {
                if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
            throw failure;
        }
    }

    private void addPartitionsWithoutStatistics(String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, TrinoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            int partitionsAdded = client.addPartitions(partitions);
                            if (partitionsAdded != partitions.size()) {
                                throw new TrinoException(HIVE_METASTORE_ERROR,
                                        format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                            }
                            return null;
                        }
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, format("One or more partitions already exist for table '%s.%s'", databaseName, tableName), e);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            Partition partition = client.getPartition(databaseName, tableName, parts);
                            client.dropPartition(databaseName, tableName, parts, deleteData);
                            String partitionLocation = partition.getSd().getLocation();
                            if (deleteFilesOnDrop && deleteData && !isNullOrEmpty(partitionLocation) && isManagedTable(client.getTable(databaseName, tableName))) {
                                deleteDirRecursive(Location.of(partitionLocation));
                            }
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        alterPartitionWithoutStatistics(databaseName, tableName, toMetastoreApiPartition(partitionWithStatistics));
        storePartitionColumnStatistics(databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
        dropExtraColumnStatisticsAfterAlterPartition(databaseName, tableName, partitionWithStatistics);
    }

    private void alterPartitionWithoutStatistics(String databaseName, String tableName, Partition partition)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            client.alterPartition(databaseName, tableName, partition);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getValues());
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private void storePartitionColumnStatistics(String databaseName, String tableName, String partitionName, PartitionWithStatistics partitionWithStatistics)
    {
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        Map<String, HiveColumnStatistics> columnStatistics = statistics.getColumnStatistics();
        if (columnStatistics.isEmpty()) {
            return;
        }
        Map<String, HiveType> columnTypes = partitionWithStatistics.getPartition().getColumns().stream()
                .collect(toImmutableMap(Column::getName, Column::getType));
        setPartitionColumnStatistics(databaseName, tableName, partitionName, columnTypes, columnStatistics, statistics.getBasicStatistics().getRowCount());
    }

    /*
     * After altering a partition metastore preserves all column statistics for that partition.
     *
     * The old statistics are supposed to be replaced by storing the new partition statistics.
     *
     * In case when the new statistics are not present for some columns, or if the table schema has changed
     * if is needed to explicitly remove the statistics from the metastore for that columns.
     */
    private void dropExtraColumnStatisticsAfterAlterPartition(
            String databaseName,
            String tableName,
            PartitionWithStatistics partitionWithStatistics)
    {
        List<String> dataColumns = partitionWithStatistics.getPartition().getColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        Set<String> columnsWithMissingStatistics = new HashSet<>(dataColumns);
        columnsWithMissingStatistics.removeAll(partitionWithStatistics.getStatistics().getColumnStatistics().keySet());

        // In case new partition had the statistics computed for all the columns, the storePartitionColumnStatistics
        // call in the alterPartition will just overwrite the old statistics. There is no need to explicitly remove anything.
        if (columnsWithMissingStatistics.isEmpty()) {
            return;
        }

        // check if statistics for the columnsWithMissingStatistics are actually stored in the metastore
        // when trying to remove any missing statistics the metastore throws NoSuchObjectException
        String partitionName = partitionWithStatistics.getPartitionName();
        List<ColumnStatisticsObj> statisticsToBeRemoved = getMetastorePartitionColumnStatistics(
                databaseName,
                tableName,
                ImmutableSet.of(partitionName),
                ImmutableList.copyOf(columnsWithMissingStatistics))
                .getOrDefault(partitionName, ImmutableList.of());

        for (ColumnStatisticsObj statistics : statisticsToBeRemoved) {
            deletePartitionColumnStatistics(databaseName, tableName, partitionName, statistics.getColName());
        }
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(partitionValues, "partitionValues is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartition", stats.getGetPartition().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return Optional.of(client.getPartition(databaseName, tableName, partitionValues));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!partitionNames.isEmpty(), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            return client.getPartitionsByNames(databaseName, tableName, partitionNames);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(privilege, grantOption, grantor, grantee))
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(toImmutableSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            Set<HivePrivilegeInfo> existingPrivileges = listTablePrivileges(databaseName, tableName, Optional.of(tableOwner), Optional.of(grantee));

                            Set<PrivilegeGrantInfo> privilegesToGrant = new HashSet<>(requestedPrivileges);
                            Iterator<PrivilegeGrantInfo> iterator = privilegesToGrant.iterator();
                            while (iterator.hasNext()) {
                                HivePrivilegeInfo requestedPrivilege = getOnlyElement(parsePrivilege(iterator.next(), Optional.empty()));

                                for (HivePrivilegeInfo existingPrivilege : existingPrivileges) {
                                    if ((requestedPrivilege.isContainedIn(existingPrivilege))) {
                                        iterator.remove();
                                    }
                                    else if (existingPrivilege.isContainedIn(requestedPrivilege)) {
                                        throw new TrinoException(NOT_SUPPORTED, format(
                                                "Granting %s WITH GRANT OPTION is not supported while %s possesses %s",
                                                requestedPrivilege.getHivePrivilege().name(),
                                                grantee,
                                                requestedPrivilege.getHivePrivilege().name()));
                                    }
                                }
                            }

                            if (privilegesToGrant.isEmpty()) {
                                return null;
                            }

                            metastoreClient.grantPrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToGrant));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        Set<PrivilegeGrantInfo> requestedPrivileges = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(privilege, grantOption, grantor, grantee))
                .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                .collect(toImmutableSet());
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("revokeTablePrivileges", stats.getRevokeTablePrivileges().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            Set<HivePrivilege> existingHivePrivileges = listTablePrivileges(databaseName, tableName, Optional.of(tableOwner), Optional.of(grantee)).stream()
                                    .map(HivePrivilegeInfo::getHivePrivilege)
                                    .collect(toImmutableSet());

                            Set<PrivilegeGrantInfo> privilegesToRevoke = requestedPrivileges.stream()
                                    .filter(privilegeGrantInfo -> existingHivePrivileges.contains(getOnlyElement(parsePrivilege(privilegeGrantInfo, Optional.empty())).getHivePrivilege()))
                                    .collect(toImmutableSet());

                            if (privilegesToRevoke.isEmpty()) {
                                return null;
                            }

                            metastoreClient.revokePrivileges(buildPrivilegeBag(databaseName, tableName, grantee, privilegesToRevoke), grantOption);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("listTablePrivileges", stats.getListTablePrivileges().wrap(() -> {
                        try (ThriftMetastoreClient client = createMetastoreClient()) {
                            ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();
                            List<HiveObjectPrivilege> hiveObjectPrivilegeList;
                            if (principal.isEmpty()) {
                                tableOwner.ifPresent(owner -> privileges.add(new HivePrivilegeInfo(OWNERSHIP, true, new HivePrincipal(USER, owner), new HivePrincipal(USER, owner))));
                                hiveObjectPrivilegeList = client.listPrivileges(
                                        null,
                                        null,
                                        new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                            }
                            else {
                                if (tableOwner.isPresent() && principal.get().getType() == USER && tableOwner.get().equals(principal.get().getName())) {
                                    privileges.add(new HivePrivilegeInfo(OWNERSHIP, true, principal.get(), principal.get()));
                                }
                                hiveObjectPrivilegeList = client.listPrivileges(
                                        principal.get().getName(),
                                        fromTrinoPrincipalType(principal.get().getType()),
                                        new HiveObjectRef(TABLE, databaseName, tableName, null, null));
                            }
                            for (HiveObjectPrivilege hiveObjectPrivilege : hiveObjectPrivilegeList) {
                                HivePrincipal grantee = new HivePrincipal(fromMetastoreApiPrincipalType(hiveObjectPrivilege.getPrincipalType()), hiveObjectPrivilege.getPrincipalName());
                                privileges.addAll(parsePrivilege(hiveObjectPrivilege.getGrantInfo(), Optional.of(grantee)));
                            }
                            return privileges.build();
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void checkSupportsTransactions() {}

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        requireNonNull(transactionOwner, "transactionOwner is null");
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("openTransaction", stats.getOpenTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            return metastoreClient.openTransaction(transactionOwner.toString());
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("commitTransaction", stats.getCommitTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.commitTransaction(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("abortTransaction", stats.getAbortTransaction().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.abortTransaction(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("sendTransactionHeartbeat", (() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.sendTransactionHeartbeat(transactionId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        acquireSharedLock(transactionOwner, queryId, transactionId, fullTables, partitions, DataOperationType.SELECT, false);
    }

    @Override
    public void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        acquireSharedLock(transactionOwner, queryId, transactionId, ImmutableList.of(new SchemaTableName(dbName, tableName)), Collections.emptyList(), operation, isDynamicPartitionWrite);
    }

    private void acquireSharedLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        requireNonNull(operation, "operation is null");
        requireNonNull(transactionOwner, "transactionOwner is null");
        requireNonNull(queryId, "queryId is null");

        if (fullTables.isEmpty() && partitions.isEmpty()) {
            return;
        }

        LockRequest request = new LockRequest()
                .setHostname(getLocalHostName())
                .setAgentInfo(queryId)
                .setUser(transactionOwner.toString())
                .setTxnid(transactionId);

        for (SchemaTableName table : fullTables) {
            request.addToComponent(createLockComponentForOperation(table, operation, isDynamicPartitionWrite, Optional.empty()));
        }

        for (HivePartition partition : partitions) {
            request.addToComponent(createLockComponentForOperation(partition.getTableName(), operation, isDynamicPartitionWrite, Optional.of(partition.getPartitionId())));
        }

        acquireLock(format("hive transaction %s for query %s", transactionId, queryId), request);
    }

    @Override
    public long acquireTableExclusiveLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            String dbName,
            String tableName)
    {
        requireNonNull(transactionOwner, "transactionOwner is null");

        LockRequest request = new LockRequest()
                .setHostname(getLocalHostName())
                .setAgentInfo(queryId)
                .setUser(transactionOwner.toString());

        request.addToComponent(new LockComponent()
                .setType(LockType.EXCLUSIVE)
                .setLevel(LockLevel.TABLE)
                .setDbname(dbName)
                .setTablename(tableName));

        return acquireLock(format("query %s", queryId), request);
    }

    private long acquireLock(String context, LockRequest lockRequest)
    {
        try {
            LockResponse response = retry()
                    .stopOn(NoSuchTxnException.class, TxnAbortedException.class, MetaException.class)
                    .run("acquireLock", stats.getAcquireLock().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            return metastoreClient.acquireLock(lockRequest);
                        }
                    }));

            long lockId = response.getLockid();
            long waitStart = nanoTime();
            while (response.getState() == LockState.WAITING) {
                if (Duration.nanosSince(waitStart).compareTo(maxWaitForLock) > 0) {
                    // timed out
                    throw unlockSuppressing(lockId, new TrinoException(HIVE_TABLE_LOCK_NOT_ACQUIRED, format("Timed out waiting for lock %d for %s", lockId, context)));
                }

                log.debug("Waiting for lock %d for %s", lockId, context);

                response = retry()
                        .stopOn(NoSuchTxnException.class, NoSuchLockException.class, TxnAbortedException.class, MetaException.class)
                        .run("checkLock", stats.getCheckLock().wrap(() -> {
                            try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                                return metastoreClient.checkLock(lockId);
                            }
                        }));
            }

            if (response.getState() != LockState.ACQUIRED) {
                throw unlockSuppressing(lockId, new TrinoException(HIVE_TABLE_LOCK_NOT_ACQUIRED, "Could not acquire lock. Lock in state " + response.getState()));
            }

            return response.getLockid();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private <T extends Exception> T unlockSuppressing(long lockId, T exception)
    {
        try {
            releaseTableLock(lockId);
        }
        catch (RuntimeException e) {
            exception.addSuppressed(e);
        }
        return exception;
    }

    @Override
    public void releaseTableLock(long lockId)
    {
        try {
            retry()
                    .stopOn(NoSuchTxnException.class, NoSuchLockException.class, TxnAbortedException.class, MetaException.class)
                    .run("unlock", stats.getUnlock().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.unlock(lockId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private static LockComponent createLockComponentForOperation(SchemaTableName table, DataOperationType operation, boolean isDynamicPartitionWrite, Optional<String> partitionName)
    {
        requireNonNull(table, "table is null");
        requireNonNull(partitionName, "partitionName is null");

        LockComponent component = new LockComponent()
                .setType(LockType.SHARED_READ)
                .setOperationType(operation)
                .setDbname(table.getSchemaName())
                .setTablename(table.getTableName())
                // acquire locks is called only for TransactionalTable
                .setIsTransactional(true)
                .setIsDynamicPartitionWrite(isDynamicPartitionWrite)
                .setLevel(LockLevel.TABLE);

        partitionName.ifPresent(name -> component
                .setPartitionname(name)
                .setLevel(LockLevel.PARTITION));

        return component;
    }

    private static String getLocalHostName()
    {
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            throw new RuntimeException("Unable to determine local host");
        }
        return hostName;
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getValidWriteIds", stats.getValidWriteIds().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            return metastoreClient.getValidWriteIds(
                                    tables.stream()
                                            .map(table -> format("%s.%s", table.getSchemaName(), table.getTableName()))
                                            .collect(toImmutableList()),
                                    currentTransactionId);
                        }
                    }));
        }
        catch (TException e) {
            // When calling Hive metastore < 3, the call fails with
            // Required field 'open_txns' is unset! Struct:GetOpenTxnsResponse(txn_high_water_mark:4, open_txns:null, min_open_txn:4, abortedBits:null)
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to open transaction. Transactional tables support requires Hive metastore version at least 3.0", e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getConfigValueFromServer", () -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            return Optional.ofNullable(metastoreClient.getConfigValue(name, null));
                        }
                    });
        }
        catch (ConfigValSecurityException e) {
            log.debug(e, "Could not fetch value for config '%s' from Hive", name);
            return Optional.empty();
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("allocateWriteId", stats.getAllocateWriteId().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            List<TxnToWriteId> list = metastoreClient.allocateTableWriteIds(dbName, tableName, ImmutableList.of(transactionId));
                            return getOnlyElement(list).getWriteId();
                        }
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        checkArgument(transactionId > 0, "transactionId should be a positive integer, but was %s", transactionId);
        requireNonNull(dbName, "dbName is null");
        requireNonNull(tableName, "tableName is null");
        checkArgument(writeId > 0, "writeId should be a positive integer, but was %s", writeId);
        try {
            Table table = getTable(dbName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(dbName, tableName)));
            rowCountChange.ifPresent(rowCount ->
                    table.setParameters(adjustRowCount(table.getParameters(), tableName, rowCount)));
            retry()
                    .stopOnIllegalExceptions()
                    .run("updateTableWriteId", stats.getUpdateTableWriteId().wrap(() -> {
                        alterTransactionalTable(table, transactionId, writeId);
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        checkArgument(writeId > 0, "writeId should be a positive integer, but was %s", writeId);
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("alterPartitions", stats.getAlterPartitions().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.alterPartitions(dbName, tableName, partitions, writeId);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        checkArgument(writeId > 0, "writeId should be a positive integer, but was %s", writeId);
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!partitionNames.isEmpty(), "partitionNames is empty");
        try {
            retry()
                    .stopOnIllegalExceptions()
                    .run("alterPartitions", stats.getAddDynamicPartitions().wrap(() -> {
                        try (ThriftMetastoreClient metastoreClient = createMetastoreClient()) {
                            metastoreClient.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation);
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private static PrivilegeBag buildPrivilegeBag(
            String databaseName,
            String tableName,
            HivePrincipal grantee,
            Set<PrivilegeGrantInfo> privilegeGrantInfos)
    {
        ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
        for (PrivilegeGrantInfo privilegeGrantInfo : privilegeGrantInfos) {
            privilegeBagBuilder.add(
                    new HiveObjectPrivilege(
                            new HiveObjectRef(TABLE, databaseName, tableName, null, null),
                            grantee.getName(),
                            fromTrinoPrincipalType(grantee.getType()),
                            privilegeGrantInfo,
                            "SQL"));
        }
        return new PrivilegeBag(privilegeBagBuilder.build());
    }

    private static boolean containsAllPrivilege(Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        return requestedPrivileges.stream()
                .anyMatch(privilege -> privilege.getPrivilege().equalsIgnoreCase("all"));
    }

    private ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        return metastoreClientFactory.createMetastoreClientFor(identity);
    }

    private RetryDriver retry()
    {
        return RetryDriver.retry()
                .exponentialBackoff(minBackoffDelay, maxBackoffDelay, maxRetryTime, backoffScaleFactor)
                .maxAttempts(maxRetries + 1)
                .stopOn(TrinoException.class);
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }

    private static void validateObjectName(String objectName)
    {
        if (isNullOrEmpty(objectName)) {
            throw new IllegalArgumentException("The provided objectName cannot be null or empty");
        }
        if (DOT_MATCHER.matchesAllOf(objectName)) {
            // '.' or '..' object names can cause the object to have an inaccurate location on the object storage
            throw new TrinoException(GENERIC_USER_ERROR, format("Invalid object name: '%s'", objectName));
        }
        if (objectName.contains("/")) {
            // Older HMS instances may allow names like 'foo/bar' which can cause managed tables to be
            // saved in a different location than its intended schema directory
            throw new TrinoException(GENERIC_USER_ERROR, format("Invalid object name: '%s'", objectName));
        }
    }
}
