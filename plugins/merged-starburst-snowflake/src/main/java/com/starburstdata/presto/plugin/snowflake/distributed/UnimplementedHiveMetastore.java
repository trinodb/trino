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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PartitionWithStatistics;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

class UnimplementedHiveMetastore
        implements HiveMetastore
{
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllDatabases()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Table> getTablesByName(HiveIdentity identity, String databaseName, List<String> tableNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionStatistics getTableStatistics(HiveIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, String databaseName, String tableName, Set<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updatePartitionStatistics(HiveIdentity identity, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(HiveIdentity identity, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(HiveIdentity identity, String databaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Partition> getPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(HiveIdentity identity, String databaseName, String tableName, List<String> parts)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, String databaseName, String tableName, List<String> partitionNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partition)
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
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean withAdminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOptionFor, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, String tableOwner, Optional<HivePrincipal> principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        throw new UnsupportedOperationException();
    }
}
