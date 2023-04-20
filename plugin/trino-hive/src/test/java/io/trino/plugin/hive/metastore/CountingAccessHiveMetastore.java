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

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_TABLES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_VIEWS;

@ThreadSafe
public class CountingAccessHiveMetastore
        implements HiveMetastore
{
    public enum Method
    {
        CREATE_DATABASE,
        CREATE_TABLE,
        GET_ALL_DATABASES,
        GET_DATABASE,
        GET_TABLE,
        GET_ALL_TABLES,
        GET_ALL_TABLES_FROM_DATABASE,
        GET_TABLE_WITH_PARAMETER,
        GET_TABLE_STATISTICS,
        GET_ALL_VIEWS,
        GET_ALL_VIEWS_FROM_DATABASE,
        UPDATE_TABLE_STATISTICS,
        ADD_PARTITIONS,
        GET_PARTITION_NAMES_BY_FILTER,
        GET_PARTITIONS_BY_NAMES,
        GET_PARTITION,
        GET_PARTITION_STATISTICS,
        UPDATE_PARTITION_STATISTICS,
        REPLACE_TABLE,
        DROP_TABLE,
    }

    private final HiveMetastore delegate;
    private final ConcurrentHashMultiset<Method> methodInvocations = ConcurrentHashMultiset.create();

    public CountingAccessHiveMetastore(HiveMetastore delegate)
    {
        this.delegate = delegate;
    }

    public Multiset<Method> getMethodInvocations()
    {
        return ImmutableMultiset.copyOf(methodInvocations);
    }

    public void resetCounters()
    {
        methodInvocations.clear();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        methodInvocations.add(Method.GET_TABLE);
        return delegate.getTable(databaseName, tableName);
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        // No need to count that, since it's a pure local operation.
        return delegate.getSupportedColumnStatistics(type);
    }

    @Override
    public List<String> getAllDatabases()
    {
        methodInvocations.add(Method.GET_ALL_DATABASES);
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        methodInvocations.add(Method.GET_DATABASE);
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        methodInvocations.add(Method.GET_TABLE_WITH_PARAMETER);
        return delegate.getTablesWithParameter(databaseName, parameterKey, parameterValue);
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        methodInvocations.add(Method.GET_ALL_VIEWS_FROM_DATABASE);
        return delegate.getAllViews(databaseName);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        Optional<List<SchemaTableName>> allViews = delegate.getAllViews();
        if (allViews.isPresent()) {
            methodInvocations.add(GET_ALL_VIEWS);
        }
        return allViews;
    }

    @Override
    public void createDatabase(Database database)
    {
        methodInvocations.add(Method.CREATE_DATABASE);
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        methodInvocations.add(Method.CREATE_TABLE);
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        methodInvocations.add(Method.DROP_TABLE);
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        methodInvocations.add(Method.REPLACE_TABLE);
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        methodInvocations.add(Method.GET_PARTITION);
        return delegate.getPartition(table, partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        methodInvocations.add(Method.GET_PARTITION_NAMES_BY_FILTER);
        return delegate.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        methodInvocations.add(Method.GET_PARTITIONS_BY_NAMES);
        return delegate.getPartitionsByNames(table, partitionNames);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        methodInvocations.add(Method.ADD_PARTITIONS);
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        methodInvocations.add(Method.GET_TABLE_STATISTICS);
        return delegate.getTableStatistics(table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        methodInvocations.add(Method.GET_PARTITION_STATISTICS);
        return delegate.getPartitionStatistics(table, partitions);
    }

    @Override
    public void updateTableStatistics(String databaseName,
            String tableName,
            AcidTransaction transaction,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        methodInvocations.add(Method.UPDATE_TABLE_STATISTICS);
        delegate.updateTableStatistics(databaseName, tableName, transaction, update);
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        methodInvocations.add(Method.UPDATE_PARTITION_STATISTICS);
        delegate.updatePartitionStatistics(table, updates);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        methodInvocations.add(Method.GET_ALL_TABLES_FROM_DATABASE);
        return delegate.getAllTables(databaseName);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        Optional<List<SchemaTableName>> allTables = delegate.getAllTables();
        if (allTables.isPresent()) {
            methodInvocations.add(GET_ALL_TABLES);
        }
        return allTables;
    }
}
