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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.FieldSchema;
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
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.metastore.MetastoreUtil.isAvroTableWithSchemaSet;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.csvSchemaFields;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiDatabase;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiTable;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isAvroTableWithSchemaSet;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isCsvPartition;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.isCsvTable;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toDataOperationType;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiFunction;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiTable;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class BridgingHiveMetastore
        implements HiveMetastore
{
    private final ThriftMetastore delegate;

    public BridgingHiveMetastore(ThriftMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName).map(ThriftMetastoreUtil::fromMetastoreApiDatabase);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName).map(table -> {
            if (isAvroTableWithSchemaSet(table)) {
                return fromMetastoreApiTable(table, delegate.getFields(databaseName, tableName).orElseThrow());
            }
            if (isCsvTable(table)) {
                return fromMetastoreApiTable(table, csvSchemaFields(table.getSd().getCols()));
            }
            return fromMetastoreApiTable(table);
        });
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        return delegate.getTableColumnStatistics(databaseName, tableName, columnNames);
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        return delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
    }

    @Override
    public boolean useSparkTableStatistics()
    {
        return delegate.useSparkTableStatistics();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        delegate.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate);
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        io.trino.hive.thrift.metastore.Table metastoreTable = toMetastoreApiTable(table);
        partitionUpdates.forEach((partitionName, update) -> delegate.updatePartitionStatistics(metastoreTable, partitionName, mode, update));
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return delegate.getTables(databaseName).stream()
                .map(table -> new TableInfo(
                        new SchemaTableName(table.getDbName(), table.getTableName()),
                        TableInfo.ExtendedRelationType.fromTableTypeAndComment(table.getTableType(), table.getComments())))
                .collect(toImmutableList());
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            delegate.createDatabase(toMetastoreApiDatabase(database));
        }
        catch (SchemaAlreadyExistsException e) {
            // Ignore SchemaAlreadyExistsException when this query has already created the database.
            // This may happen when an actually successful metastore create call is retried
            // because of a timeout on our side.
            String expectedQueryId = database.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getDatabase(database.getDatabaseName())
                        .map(Database::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (!expectedQueryId.equals(existingQueryId)) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        io.trino.hive.thrift.metastore.Database database = delegate.getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
        database.setName(newDatabaseName);
        delegate.alterDatabase(databaseName, database);

        delegate.getDatabase(databaseName).ifPresent(newDatabase -> {
            if (newDatabase.getName().equals(databaseName)) {
                throw new TrinoException(NOT_SUPPORTED, "Hive metastore does not support renaming schemas");
            }
        });
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        Database database = fromMetastoreApiDatabase(delegate.getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName)));

        Database newDatabase = Database.builder(database)
                .setOwnerName(Optional.of(principal.getName()))
                .setOwnerType(Optional.of(principal.getType()))
                .build();

        delegate.alterDatabase(databaseName, toMetastoreApiDatabase(newDatabase));
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            delegate.createTable(toMetastoreApiTable(table, principalPrivileges));
        }
        catch (TableAlreadyExistsException e) {
            // Ignore TableAlreadyExistsException when this query has already created the table.
            // This may happen when an actually successful metastore create call is retried
            // because of a timeout on our side.
            String expectedQueryId = table.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getTable(table.getDatabaseName(), table.getTableName())
                        .map(Table::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (!expectedQueryId.equals(existingQueryId)) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        alterTable(databaseName, tableName, toMetastoreApiTable(newTable, principalPrivileges));
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.setDbName(newDatabaseName);
        table.setTableName(newTableName);
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        Map<String, String> parameters = table.getParameters().entrySet().stream()
                .filter(entry -> !entry.getKey().equals(TABLE_COMMENT))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        comment.ifPresent(value -> parameters.put(TABLE_COMMENT, value));

        table.setParameters(parameters);
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        // TODO Add role support https://github.com/trinodb/trino/issues/5706
        if (principal.getType() != USER) {
            throw new TrinoException(NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }

        Table table = fromMetastoreApiTable(delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName))));

        Table newTable = Table.builder(table)
                .setOwner(Optional.of(principal.getName()))
                .build();

        delegate.alterTable(databaseName, tableName, toMetastoreApiTable(newTable));
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        List<FieldSchema> fieldSchemas = ImmutableList.<FieldSchema>builder()
                .addAll(table.getSd().getCols())
                .addAll(table.getPartitionKeys())
                .build();

        for (FieldSchema fieldSchema : fieldSchemas) {
            if (fieldSchema.getName().equals(columnName)) {
                if (comment.isPresent()) {
                    fieldSchema.setComment(comment.get());
                }
                else {
                    fieldSchema.unsetComment();
                }
            }
        }

        alterTable(databaseName, tableName, table);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.getSd().getCols().add(
                new FieldSchema(columnName, columnType.getHiveTypeName().toString(), columnComment));
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        for (FieldSchema fieldSchema : table.getPartitionKeys()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                throw new TrinoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
            }
        }
        for (FieldSchema fieldSchema : table.getSd().getCols()) {
            if (fieldSchema.getName().equals(oldColumnName)) {
                fieldSchema.setName(newColumnName);
            }
        }
        alterTable(databaseName, tableName, table);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, databaseName, tableName, columnName);
        io.trino.hive.thrift.metastore.Table table = delegate.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        table.getSd().getCols().removeIf(fieldSchema -> fieldSchema.getName().equals(columnName));
        alterTable(databaseName, tableName, table);
    }

    private void alterTable(String databaseName, String tableName, io.trino.hive.thrift.metastore.Table table)
    {
        delegate.alterTable(databaseName, tableName, table);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return delegate.getPartition(table.getDatabaseName(), table.getTableName(), partitionValues).map(partition -> fromMetastoreApiPartition(table, partition));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(Collectors.toMap(identity(), Partition::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = delegate.getPartitionsByNames(table.getDatabaseName(), table.getTableName(), partitionNames).stream()
                .map(partition -> fromMetastoreApiPartition(table, partition))
                .collect(Collectors.toMap(Partition::getValues, identity()));
        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.buildOrThrow();
    }

    private static Partition fromMetastoreApiPartition(Table table, io.trino.hive.thrift.metastore.Partition partition)
    {
        if (isAvroTableWithSchemaSet(table)) {
            List<FieldSchema> schema = table.getDataColumns().stream()
                    .map(ThriftMetastoreUtil::toMetastoreApiFieldSchema)
                    .collect(toImmutableList());
            return ThriftMetastoreUtil.fromMetastoreApiPartition(partition, schema);
        }
        if (isCsvPartition(partition)) {
            return ThriftMetastoreUtil.fromMetastoreApiPartition(partition, csvSchemaFields(partition.getSd().getCols()));
        }

        return ThriftMetastoreUtil.fromMetastoreApiPartition(partition);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        delegate.alterPartition(databaseName, tableName, partition);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        return delegate.listRoles();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        delegate.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return delegate.listRoleGrants(principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        delegate.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        delegate.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return delegate.listTablePrivileges(databaseName, tableName, tableOwner, principal);
    }

    @Override
    public Optional<String> getConfigValue(String name)
    {
        return delegate.getConfigValue(name);
    }

    @Override
    public void checkSupportsTransactions()
    {
        delegate.checkSupportsTransactions();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        return delegate.openTransaction(transactionOwner);
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        delegate.commitTransaction(transactionId);
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        delegate.abortTransaction(transactionId);
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        delegate.sendTransactionHeartbeat(transactionId);
    }

    @Override
    public void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        delegate.acquireSharedReadLock(transactionOwner, queryId, transactionId, fullTables, partitions);
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        return delegate.getValidWriteIds(tables, currentTransactionId);
    }

    @Override
    public long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        return delegate.allocateWriteId(dbName, tableName, transactionId);
    }

    @Override
    public void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            AcidOperation acidOperation,
            boolean isDynamicPartitionWrite)
    {
        delegate.acquireTableWriteLock(transactionOwner, queryId, transactionId, dbName, tableName, toDataOperationType(acidOperation), isDynamicPartitionWrite);
    }

    @Override
    public void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        delegate.updateTableWriteId(dbName, tableName, transactionId, writeId, rowCountChange);
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        delegate.addDynamicPartitions(dbName, tableName, partitionNames, transactionId, writeId, toDataOperationType(operation));
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        delegate.alterTransactionalTable(toMetastoreApiTable(table, principalPrivileges), transactionId, writeId);
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return delegate.getFunction(databaseName, metastoreFunctionName(functionName, signatureToken)).isPresent();
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return getFunctionsByPattern(databaseName, "trino__*");
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return getFunctionsByPattern(databaseName, "trino__" + functionName + "__*");
    }

    private Collection<LanguageFunction> getFunctionsByPattern(String databaseName, String functionNamePattern)
    {
        return delegate.getFunctions(databaseName, functionNamePattern).stream()
                .map(name -> delegate.getFunction(databaseName, name))
                .flatMap(Optional::stream)
                .map(ThriftMetastoreUtil::fromMetastoreApiFunction)
                .collect(toImmutableList());
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        if (functionName.contains("__")) {
            throw new TrinoException(NOT_SUPPORTED, "Function names with double underscore are not supported");
        }
        delegate.createFunction(toMetastoreApiFunction(databaseName, functionName, function));
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        delegate.alterFunction(toMetastoreApiFunction(databaseName, functionName, function));
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        delegate.dropFunction(databaseName, metastoreFunctionName(functionName, signatureToken));
    }
}
