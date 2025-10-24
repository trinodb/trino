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
package io.trino.metastore;

import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.TDigest;
import io.trino.plugin.base.metrics.DistributionSnapshot;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class MeasuredHiveMetastore
        implements HiveMetastore
{
    private final HiveMetastore delegate;
    private final MetastoreApiCallStats allApiCallsStats = new MetastoreApiCallStats();
    private final Map<String, MetastoreApiCallStats> apiCallStats = new ConcurrentHashMap<>();
    private final Ticker ticker = Ticker.systemTicker();

    public MeasuredHiveMetastore(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public static HiveMetastoreFactory factory(HiveMetastoreFactory metastoreFactory)
    {
        return new MeasuredMetastoreFactory(metastoreFactory);
    }

    @Override
    public Metrics getMetrics()
    {
        ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.builder();
        allApiCallsStats.storeTo(metrics, "metastore.all");
        for (Map.Entry<String, MetastoreApiCallStats> callStats : apiCallStats.entrySet()) {
            callStats.getValue().storeTo(metrics, "metastore.%s".formatted(callStats.getKey()));
        }
        return new Metrics(metrics.buildOrThrow());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return wrap("getDatabase", () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        return wrap("getAllDatabases", delegate::getAllDatabases);
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return wrap("getTable", () -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return wrap("getTableColumnStatistics", () -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return wrap("getPartitionColumnStatistics", () -> delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames));
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return wrap("useSparkTableStatistics", delegate::useSparkTableStatistics);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        wrap("updateTableStatistics", () -> delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate));
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        wrap("updatePartitionStatistics", () -> delegate.updatePartitionStatistics(table, mode, partitionUpdates));
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return wrap("getTables", () -> delegate.getTables(databaseName));
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return wrap("getTableNamesWithParameters", () -> delegate.getTableNamesWithParameters(databaseName, parameterKey, parameterValues));
    }

    @Override
    public void createDatabase(Database database)
    {
        wrap("createDatabase", () -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        wrap("dropDatabase", () -> delegate.dropDatabase(databaseName, deleteData));
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        wrap("renameDatabase", () -> delegate.renameDatabase(databaseName, newDatabaseName));
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        wrap("setDatabaseOwner", () -> delegate.setDatabaseOwner(databaseName, principal));
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        wrap("createTable", () -> delegate.createTable(table, principalPrivileges));
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        wrap("dropTable", () -> delegate.dropTable(databaseName, tableName, deleteData));
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        wrap("replaceTable", () -> delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges, environmentContext));
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        wrap("renameTable", () -> delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName));
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        wrap("commentTable", () -> delegate.commentTable(databaseName, tableName, comment));
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        wrap("setTableOwner", () -> delegate.setTableOwner(databaseName, tableName, principal));
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        wrap("commentColumn", () -> delegate.commentColumn(databaseName, tableName, columnName, comment));
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        wrap("addColumn", () -> delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment));
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        wrap("renameColumn", () -> delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName));
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        wrap("dropColumn", () -> delegate.dropColumn(databaseName, tableName, columnName));
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return wrap("getPartition", () -> delegate.getPartition(table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return wrap("getPartitionNamesByFilter", () -> delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return wrap("getPartitionsByNames", () -> delegate.getPartitionsByNames(table, partitionNames));
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        wrap("addPartitions", () -> delegate.addPartitions(databaseName, tableName, partitions));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        wrap("dropPartition", () -> delegate.dropPartition(databaseName, tableName, parts, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        wrap("alterPartition", () -> delegate.alterPartition(databaseName, tableName, partition));
    }

    @Override
    public void createRole(String role, String grantor)
    {
        wrap("createRole", () -> delegate.createRole(role, grantor));
    }

    @Override
    public void dropRole(String role)
    {
        wrap("dropRole", () -> delegate.dropRole(role));
    }

    @Override
    public Set<String> listRoles()
    {
        return wrap("listRoles", delegate::listRoles);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        wrap("grantRoles", () -> delegate.grantRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        wrap("revokeRoles", () -> delegate.revokeRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return wrap("listRoleGrants", () -> delegate.listRoleGrants(principal));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        wrap("grantTablePrivileges", () -> delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        wrap("revokeTablePrivileges", () -> delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return wrap("listTablePrivileges", () -> delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal));
    }

    @Override
    public void checkSupportsTransactions()
    {
        wrap("checkSupportsTransactions", () -> delegate.checkSupportsTransactions());
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return wrap("openTransaction", () -> delegate.openTransaction(transactionOwner));
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        wrap("commitTransaction", () -> delegate.commitTransaction(transactionId));
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        wrap("abortTransaction", () -> delegate.abortTransaction(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        wrap("sendTransactionHeartbeat", () -> delegate.sendTransactionHeartbeat(transactionId));
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        wrap("acquireSharedReadLock", () -> delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions));
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return wrap("getValidWriteIds", () -> delegate.getValidWriteIds(tables, currentTransactionId));
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return wrap("getConfigValue", () -> delegate.getConfigValue(name));
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return wrap("allocateWriteId", () -> delegate.allocateWriteId(dbName, tableName, transactionId));
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        wrap("acquireTableWriteLock", () -> delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite));
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        wrap("updateTableWriteId", () -> delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange));
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        wrap("addDynamicPartitions", () -> delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation));
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        wrap("alterTransactionalTable", () -> delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges));
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return wrap("functionExists", () -> delegate.functionExists(databaseName, functionName, signatureToken));
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return wrap("getAllFunctions", () -> delegate.getAllFunctions(databaseName));
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return wrap("getFunctions", () -> delegate.getFunctions(databaseName, functionName));
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        wrap("createFunction", () -> delegate.createFunction(databaseName, functionName, function));
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        wrap("replaceFunction", () -> delegate.replaceFunction(databaseName, functionName, function));
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        wrap("dropFunction", () -> delegate.dropFunction(databaseName, functionName, signatureToken));
    }

    private void wrap(String callName, Runnable call)
    {
        wrap(callName, () -> {
            call.run();
            return null;
        });
    }

    private <T> T wrap(String callName, Callable<T> call)
    {
        MetastoreApiCallStats callStats = apiCallStats.computeIfAbsent(callName, _ -> new MetastoreApiCallStats());
        long start = ticker.read();
        try {
            return call.call();
        }
        catch (Exception e) {
            callStats.recordFailure();
            allApiCallsStats.recordFailure();
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            long timeNanos = ticker.read() - start;
            callStats.record(timeNanos);
            allApiCallsStats.record(timeNanos);
        }
    }

    @ThreadSafe
    static class MetastoreApiCallStats
    {
        private final TDigest timeNanosDistribution = new TDigest();
        private long totalTimeNanos;
        private long totalFailures;

        public synchronized void record(long nanos)
        {
            timeNanosDistribution.add(nanos);
            totalTimeNanos += nanos;
        }

        public synchronized void recordFailure()
        {
            totalFailures++;
        }

        public synchronized void storeTo(ImmutableMap.Builder<String, Metric<?>> metrics, String prefix)
        {
            // DistributionSnapshot does not retain reference to the histogram
            metrics.put(prefix + ".time.distribution", DistributionSnapshot.fromDistribution(new TDigestHistogram(timeNanosDistribution)));
            metrics.put(prefix + ".time.total", new LongCount(totalTimeNanos));

            // do not add redundant 0 failures to make the metrics more concise
            if (totalFailures > 0) {
                metrics.put(prefix + ".failures", new LongCount(totalFailures));
            }
        }
    }

    public static class MeasuredMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastoreFactory metastoreFactory;

        public MeasuredMetastoreFactory(HiveMetastoreFactory metastoreFactory)
        {
            this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        }

        @Override
        public boolean isImpersonationEnabled()
        {
            return metastoreFactory.isImpersonationEnabled();
        }

        @Override
        public boolean hasBuiltInCaching()
        {
            return metastoreFactory.hasBuiltInCaching();
        }

        @Override
        public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
        {
            return new MeasuredHiveMetastore(metastoreFactory.createMetastore(identity));
        }
    }
}
