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
package io.trino.metastore.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
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

import static io.trino.metastore.tracing.MetastoreAttributes.ACID_TRANSACTION;
import static io.trino.metastore.tracing.MetastoreAttributes.FUNCTION;
import static io.trino.metastore.tracing.MetastoreAttributes.FUNCTION_RESPONSE_COUNT;
import static io.trino.metastore.tracing.MetastoreAttributes.PARTITION;
import static io.trino.metastore.tracing.MetastoreAttributes.PARTITION_REQUEST_COUNT;
import static io.trino.metastore.tracing.MetastoreAttributes.PARTITION_RESPONSE_COUNT;
import static io.trino.metastore.tracing.MetastoreAttributes.SCHEMA;
import static io.trino.metastore.tracing.MetastoreAttributes.SCHEMA_RESPONSE_COUNT;
import static io.trino.metastore.tracing.MetastoreAttributes.TABLE;
import static io.trino.metastore.tracing.MetastoreAttributes.TABLE_RESPONSE_COUNT;
import static io.trino.metastore.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

public class TracingHiveMetastore
        implements HiveMetastore
{
    private final Tracer tracer;
    private final HiveMetastore delegate;

    public TracingHiveMetastore(Tracer tracer, HiveMetastore delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getDatabase")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        return withTracing(span, () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        Span span = tracer.spanBuilder("HiveMetastore.getAllDatabases")
                .startSpan();
        return withTracing(span, () -> {
            List<String> databases = delegate.getAllDatabases();
            span.setAttribute(SCHEMA_RESPONSE_COUNT, databases.size());
            return databases;
        });
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getTable")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        return withTracing(span, () -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getTableColumnStatistics")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        return withTracing(span, () -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getPartitionColumnStatistics")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .setAttribute(PARTITION_REQUEST_COUNT, (long) partitionNames.size())
                .startSpan();
        return withTracing(span, () -> {
            var partitionColumnStatistics = delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
            span.setAttribute(PARTITION_RESPONSE_COUNT, partitionColumnStatistics.size());
            return partitionColumnStatistics;
        });
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return delegate.useSparkTableStatistics();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        Span span = tracer.spanBuilder("HiveMetastore.updateTableStatistics")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        acidWriteId.ifPresent(id -> span.setAttribute(ACID_TRANSACTION, id));
        withTracing(span, () -> delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate));
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        Span span = tracer.spanBuilder("HiveMetastore.updatePartitionStatistics")
                .setAttribute(SCHEMA, table.getDatabaseName())
                .setAttribute(TABLE, table.getTableName())
                .startSpan();
        withTracing(span, () -> delegate.updatePartitionStatistics(table, mode, partitionUpdates));
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getTables")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        return withTracing(span, () -> {
            List<TableInfo> tables = delegate.getTables(databaseName);
            span.setAttribute(TABLE_RESPONSE_COUNT, tables.size());
            return tables;
        });
    }

    @Override
    public void createDatabase(Database database)
    {
        Span span = tracer.spanBuilder("HiveMetastore.createDatabase")
                .setAttribute(SCHEMA, database.getDatabaseName())
                .startSpan();
        withTracing(span, () -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropDatabase")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        withTracing(span, () -> delegate.dropDatabase(databaseName, deleteData));
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.renameDatabase")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        withTracing(span, () -> delegate.renameDatabase(databaseName, newDatabaseName));
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        Span span = tracer.spanBuilder("HiveMetastore.setDatabaseOwner")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        withTracing(span, () -> delegate.setDatabaseOwner(databaseName, principal));
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        Span span = tracer.spanBuilder("HiveMetastore.createTable")
                .setAttribute(SCHEMA, table.getDatabaseName())
                .setAttribute(TABLE, table.getTableName())
                .startSpan();
        withTracing(span, () -> delegate.createTable(table, principalPrivileges));
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropTable")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.dropTable(databaseName, tableName, deleteData));
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        Span span = tracer.spanBuilder("HiveMetastore.replaceTable")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges));
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.renameTable")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName));
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        Span span = tracer.spanBuilder("HiveMetastore.commentTable")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.commentTable(databaseName, tableName, comment));
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        Span span = tracer.spanBuilder("HiveMetastore.setTableOwner")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.setTableOwner(databaseName, tableName, principal));
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        Span span = tracer.spanBuilder("HiveMetastore.commentColumn")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.commentColumn(databaseName, tableName, columnName, comment));
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Span span = tracer.spanBuilder("HiveMetastore.addColumn")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment));
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.renameColumn")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName));
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropColumn")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.dropColumn(databaseName, tableName, columnName));
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getPartition")
                .setAttribute(SCHEMA, table.getDatabaseName())
                .setAttribute(TABLE, table.getTableName())
                .startSpan();
        return withTracing(span, () -> delegate.getPartition(table, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getPartitionNamesByFilter")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        return withTracing(span, () -> {
            Optional<List<String>> partitionNames = delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
            partitionNames.ifPresent(partitions -> span.setAttribute(PARTITION_RESPONSE_COUNT, partitions.size()));
            return partitionNames;
        });
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getPartitionsByNames")
                .setAttribute(SCHEMA, table.getDatabaseName())
                .setAttribute(TABLE, table.getTableName())
                .setAttribute(PARTITION_REQUEST_COUNT, (long) partitionNames.size())
                .startSpan();
        return withTracing(span, () -> {
            Map<String, Optional<Partition>> partitions = delegate.getPartitionsByNames(table, partitionNames);
            span.setAttribute(PARTITION_RESPONSE_COUNT, partitions.size());
            return partitions;
        });
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        Span span = tracer.spanBuilder("HiveMetastore.addPartitions")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .setAttribute(PARTITION_REQUEST_COUNT, (long) partitions.size())
                .startSpan();
        withTracing(span, () -> delegate.addPartitions(databaseName, tableName, partitions));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropPartition")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.dropPartition(databaseName, tableName, parts, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        Span span = tracer.spanBuilder("HiveMetastore.alterPartition")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .setAttribute(PARTITION, partition.getPartitionName())
                .startSpan();
        withTracing(span, () -> delegate.alterPartition(databaseName, tableName, partition));
    }

    @Override
    public void createRole(String role, String grantor)
    {
        Span span = tracer.spanBuilder("HiveMetastore.createRole")
                .startSpan();
        withTracing(span, () -> delegate.createRole(role, grantor));
    }

    @Override
    public void dropRole(String role)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropRole")
                .startSpan();
        withTracing(span, () -> delegate.dropRole(role));
    }

    @Override
    public Set<String> listRoles()
    {
        Span span = tracer.spanBuilder("HiveMetastore.listRoles")
                .startSpan();
        return withTracing(span, delegate::listRoles);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        Span span = tracer.spanBuilder("HiveMetastore.grantRoles")
                .startSpan();
        withTracing(span, () -> delegate.grantRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        Span span = tracer.spanBuilder("HiveMetastore.revokeRoles")
                .startSpan();
        withTracing(span, () -> delegate.revokeRoles(roles, grantees, adminOption, grantor));
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        Span span = tracer.spanBuilder("HiveMetastore.listRoleGrants")
                .startSpan();
        return withTracing(span, () -> delegate.listRoleGrants(principal));
    }

    @Override
    public void grantTablePrivileges(
            String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilegeInfo.HivePrivilege> privileges,
            boolean grantOption)
    {
        Span span = tracer.spanBuilder("HiveMetastore.grantTablePrivileges")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public void revokeTablePrivileges(
            String databaseName,
            String tableName,
            String tableOwner,
            HivePrincipal grantee,
            HivePrincipal grantor,
            Set<HivePrivilegeInfo.HivePrivilege> privileges,
            boolean grantOption)
    {
        Span span = tracer.spanBuilder("HiveMetastore.revokeTablePrivileges")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        withTracing(span, () -> delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption));
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        Span span = tracer.spanBuilder("HiveMetastore.listTablePrivileges")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(TABLE, tableName)
                .startSpan();
        return withTracing(span, () -> delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal));
    }

    @Override
    public void checkSupportsTransactions()
    {
        // Tracing is not necessary
        delegate.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        Span span = tracer.spanBuilder("HiveMetastore.openTransaction")
                .startSpan();
        return withTracing(span, () -> {
            long transactionId = delegate.openTransaction(transactionOwner);
            span.setAttribute(ACID_TRANSACTION, transactionId);
            return transactionId;
        });
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        Span span = tracer.spanBuilder("HiveMetastore.commitTransaction")
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.commitTransaction(transactionId));
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        Span span = tracer.spanBuilder("HiveMetastore.abortTransaction")
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.abortTransaction(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        Span span = tracer.spanBuilder("HiveMetastore.sendTransactionHeartbeat")
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.sendTransactionHeartbeat(transactionId));
    }

    @Override
    public void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        Span span = tracer.spanBuilder("HiveMetastore.acquireSharedReadLock")
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions));
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getValidWriteIds")
                .setAttribute(ACID_TRANSACTION, currentTransactionId)
                .startSpan();
        return withTracing(span, () -> delegate.getValidWriteIds(tables, currentTransactionId));
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getConfigValue")
                .startSpan();
        return withTracing(span, () -> delegate.getConfigValue(name));
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        Span span = tracer.spanBuilder("HiveMetastore.allocateWriteId")
                .setAttribute(SCHEMA, dbName)
                .setAttribute(TABLE, tableName)
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        return withTracing(span, () -> delegate.allocateWriteId(dbName, tableName, transactionId));
    }

    @Override
    public void acquireTableWriteLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, String dbName, String tableName, AcidOperation operation, boolean isDynamicPartitionWrite)
    {
        Span span = tracer.spanBuilder("HiveMetastore.acquireTableWriteLock")
                .setAttribute(SCHEMA, dbName)
                .setAttribute(TABLE, tableName)
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, operation, isDynamicPartitionWrite));
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        Span span = tracer.spanBuilder("HiveMetastore.updateTableWriteId")
                .setAttribute(SCHEMA, dbName)
                .setAttribute(TABLE, tableName)
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange));
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        Span span = tracer.spanBuilder("HiveMetastore.addDynamicPartitions")
                .setAttribute(SCHEMA, dbName)
                .setAttribute(TABLE, tableName)
                .setAttribute(ACID_TRANSACTION, transactionId)
                .setAttribute(PARTITION_REQUEST_COUNT, (long) partitionNames.size())
                .startSpan();
        withTracing(span, () -> delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, operation));
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        Span span = tracer.spanBuilder("HiveMetastore.alterTransactionalTable")
                .setAttribute(SCHEMA, table.getDatabaseName())
                .setAttribute(TABLE, table.getTableName())
                .setAttribute(ACID_TRANSACTION, transactionId)
                .startSpan();
        withTracing(span, () -> delegate.alterTransactionalTable(table, transactionId, writeId, principalPrivileges));
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        Span span = tracer.spanBuilder("HiveMetastore.functionExists")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(FUNCTION, functionName)
                .startSpan();
        return withTracing(span, () -> delegate.functionExists(databaseName, functionName, signatureToken));
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getAllFunctions")
                .setAttribute(SCHEMA, databaseName)
                .startSpan();
        return withTracing(span, () -> {
            Collection<LanguageFunction> functions = delegate.getAllFunctions(databaseName);
            span.setAttribute(FUNCTION_RESPONSE_COUNT, functions.size());
            return functions;
        });
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getFunctions")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(FUNCTION, functionName)
                .startSpan();
        return withTracing(span, () -> {
            Collection<LanguageFunction> functions = delegate.getFunctions(databaseName, functionName);
            span.setAttribute(FUNCTION_RESPONSE_COUNT, functions.size());
            return functions;
        });
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        Span span = tracer.spanBuilder("HiveMetastore.createFunction")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(FUNCTION, functionName)
                .startSpan();
        withTracing(span, () -> delegate.createFunction(databaseName, functionName, function));
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        Span span = tracer.spanBuilder("HiveMetastore.replaceFunction")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(FUNCTION, functionName)
                .startSpan();
        withTracing(span, () -> delegate.replaceFunction(databaseName, functionName, function));
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        Span span = tracer.spanBuilder("HiveMetastore.dropFunction")
                .setAttribute(SCHEMA, databaseName)
                .setAttribute(FUNCTION, functionName)
                .startSpan();
        withTracing(span, () -> delegate.dropFunction(databaseName, functionName, signatureToken));
    }
}
