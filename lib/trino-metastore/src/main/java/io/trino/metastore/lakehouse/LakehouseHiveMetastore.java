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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
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
import io.trino.metastore.TableInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.metastore.Partitions.toPartitionValues;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.lakehouse.LakehouseErrorTranslator.notSupported;
import static java.util.Objects.requireNonNull;

/**
 * Read-path {@link HiveMetastore} backed by a Lakehouse Hive catalog.
 *
 * <p>The adapter stays thin: every Lakehouse call goes through {@link LakehouseClient}, and every write
 * or DDL operation fails with an explicit error instead of acting as a successful no-op.
 */
public class LakehouseHiveMetastore
        implements HiveMetastore
{
    private final Supplier<LakehouseClient> client;

    /**
     * @param client supplies the Lakehouse client on first use, so that creating the client, which
     *         resolves Google credentials, does not happen while a catalog loads
     */
    public LakehouseHiveMetastore(Supplier<LakehouseClient> client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    // Schema discovery

    @Override
    public List<String> getAllDatabases()
    {
        return client.get().listDatabases().stream()
                .map(DatabaseMapper::databaseName)
                .collect(toImmutableList());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return client.get().getDatabase(databaseName).map(DatabaseMapper::toTrino);
    }

    // Table discovery

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return client.get().listTables(databaseName).stream()
                .map(table -> new TableInfo(
                        new SchemaTableName(databaseName, TableMapper.tableName(table)),
                        relationType(table)))
                .collect(toImmutableList());
    }

    private static TableInfo.ExtendedRelationType relationType(HiveTable table)
    {
        return TableInfo.ExtendedRelationType.fromTableTypeAndComment(
                TableMapper.tableType(table),
                table.getParametersMap().get(TABLE_COMMENT));
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return client.get().getTable(databaseName, tableName).map(TableMapper::toTrino);
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        // This adapter does not look up tables by parameter value, so the search finds nothing
        return ImmutableList.of();
    }

    // Partition discovery

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return onePartition(table, partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        if (partitionKeysFilter.isNone()) {
            return Optional.of(ImmutableList.of());
        }

        String filter = PartitionFilter.toLakehouseFilter(columnNames, partitionKeysFilter);
        // An absent result tells Trino to report a missing table
        return client.get().listPartitions(databaseName, tableName, filter)
                .map(partitions -> partitions.stream()
                        // Lakehouse returns partition values, not names, so this adapter builds the names
                        .map(partition -> makePartName(columnNames, partition.getValuesList()))
                        .collect(toImmutableList()));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> partitions =
                ImmutableMap.builderWithExpectedSize(partitionNames.size());
        for (String partitionName : partitionNames) {
            partitions.put(partitionName, onePartition(table, toPartitionValues(partitionName)));
        }
        return partitions.buildKeepingLast();
    }

    private Optional<Partition> onePartition(Table table, List<String> partitionValues)
    {
        String filter = PartitionFilter.toLakehouseFilter(table.getPartitionColumns(), partitionValues);
        boolean exact = PartitionFilter.matchesExactly(partitionValues);
        return client.get()
                .listPartitions(table.getDatabaseName(), table.getTableName(), filter)
                // A filter that left a value out lists more than one partition
                .flatMap(partitions -> partitions.stream()
                        .filter(partition -> exact || partition.getValuesList().equals(partitionValues))
                        .findFirst())
                .map(partition -> PartitionMapper.toTrino(partition, table.getDatabaseName(), table.getTableName()));
    }

    // Statistics: the Lakehouse Hive API exposes none, so the planner works without them

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    // Security and functions: empty results let read queries plan, and nothing here writes

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(
            String databaseName,
            String tableName,
            Optional<String> tableOwner,
            Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return ImmutableList.of();
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return ImmutableList.of();
    }

    // Unsupported: every write and DDL path fails explicitly

    @Override
    public void createDatabase(Database database)
    {
        throw notSupported("creating databases");
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw notSupported("dropping databases");
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw notSupported("renaming databases");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw notSupported("setting database owners");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw notSupported("creating tables");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        throw notSupported("dropping tables");
    }

    @Override
    public void replaceTable(
            String databaseName,
            String tableName,
            Table newTable,
            PrincipalPrivileges principalPrivileges,
            Map<String, String> environmentContext)
    {
        throw notSupported("replacing tables");
    }

    @Override
    public void renameTable(
            String databaseName,
            String tableName,
            String newDatabaseName,
            String newTableName)
    {
        throw notSupported("renaming tables");
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw notSupported("commenting tables");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw notSupported("setting table owners");
    }

    @Override
    public void commentColumn(
            String databaseName,
            String tableName,
            String columnName,
            Optional<String> comment)
    {
        throw notSupported("commenting columns");
    }

    @Override
    public void addColumn(
            String databaseName,
            String tableName,
            String columnName,
            HiveType columnType,
            String columnComment)
    {
        throw notSupported("adding columns");
    }

    @Override
    public void renameColumn(
            String databaseName,
            String tableName,
            String oldColumnName,
            String newColumnName)
    {
        throw notSupported("renaming columns");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw notSupported("dropping columns");
    }

    @Override
    public void addPartitions(
            String databaseName,
            String tableName,
            List<PartitionWithStatistics> partitions)
    {
        throw notSupported("adding partitions");
    }

    @Override
    public void dropPartition(
            String databaseName,
            String tableName,
            List<String> parts,
            boolean deleteData)
    {
        throw notSupported("dropping partitions");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw notSupported("altering partitions");
    }

    @Override
    public void updateTableStatistics(
            String databaseName,
            String tableName,
            OptionalLong acidWriteId,
            StatisticsUpdateMode mode,
            PartitionStatistics statisticsUpdate)
    {
        throw notSupported("updating table statistics");
    }

    @Override
    public void updatePartitionStatistics(
            Table table,
            StatisticsUpdateMode mode,
            Map<String, PartitionStatistics> partitionUpdates)
    {
        throw notSupported("updating partition statistics");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw notSupported("creating roles");
    }

    @Override
    public void dropRole(String role)
    {
        throw notSupported("dropping roles");
    }

    @Override
    public void grantRoles(
            Set<String> roles,
            Set<HivePrincipal> grantees,
            boolean adminOption,
            HivePrincipal grantor)
    {
        throw notSupported("granting roles");
    }

    @Override
    public void revokeRoles(
            Set<String> roles,
            Set<HivePrincipal> grantees,
            boolean adminOption,
            HivePrincipal grantor)
    {
        throw notSupported("revoking roles");
    }

    @Override
    public void grantTablePrivileges(
            String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilege> privileges,
            boolean grantOption)
    {
        throw notSupported("granting table privileges");
    }

    @Override
    public void revokeTablePrivileges(
            String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilege> privileges,
            boolean grantOption)
    {
        throw notSupported("revoking table privileges");
    }

    @Override
    public void checkSupportsTransactions()
    {
        throw notSupported("ACID tables");
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw notSupported("transactions");
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        throw notSupported("transactions");
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        throw notSupported("transactions");
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        throw notSupported("transactions");
    }

    @Override
    public void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        throw notSupported("transactions");
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw notSupported("transactions");
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        throw notSupported("transactions");
    }

    @Override
    public void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            AcidOperation operation,
            boolean isDynamicPartitionWrite)
    {
        throw notSupported("transactions");
    }

    @Override
    public void updateTableWriteId(
            String dbName,
            String tableName,
            long transactionId,
            long writeId,
            OptionalLong rowCountChange)
    {
        throw notSupported("transactions");
    }

    @Override
    public void addDynamicPartitions(
            String dbName,
            String tableName,
            List<String> partitionNames,
            long transactionId,
            long writeId,
            AcidOperation operation)
    {
        throw notSupported("transactions");
    }

    @Override
    public void alterTransactionalTable(
            Table table,
            long transactionId,
            long writeId,
            PrincipalPrivileges principalPrivileges)
    {
        throw notSupported("transactions");
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw notSupported("creating functions");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw notSupported("replacing functions");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw notSupported("dropping functions");
    }
}
