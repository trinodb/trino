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

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class ThriftMetastoreStats
{
    private final ThriftMetastoreApiStats getAllDatabases = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getDatabase = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getAllTables = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getTablesWithParameter = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getAllViews = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getTable = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getFields = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getTableColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats setTableColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats deleteTableColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getPartitionColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats setPartitionColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats deletePartitionColumnStatistics = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getPartitionNames = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getPartitionNamesByParts = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getPartition = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats getPartitionsByNames = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats createDatabase = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats dropDatabase = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats alterDatabase = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats createTable = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats dropTable = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats alterTable = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats addPartitions = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats dropPartition = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats alterPartition = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats listTablePrivileges = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats grantTablePrivileges = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats revokeTablePrivileges = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats listRoles = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats grantRole = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats revokeRole = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats listRoleGrants = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats listGrantedPrincipals = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats createRole = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats dropRole = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats openTransaction = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats commitTransaction = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats abortTransaction = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats acquireLock = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats checkLock = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats unlock = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats validWriteIds = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats allocateWriteId = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats updateTableWriteId = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats alterPartitions = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats addDynamicPartitions = new ThriftMetastoreApiStats();
    private final ThriftMetastoreApiStats alterTransactionalTable = new ThriftMetastoreApiStats();

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetAllDatabases()
    {
        return getAllDatabases;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetDatabase()
    {
        return getDatabase;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetAllTables()
    {
        return getAllTables;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetTablesWithParameter()
    {
        return getTablesWithParameter;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetAllViews()
    {
        return getAllViews;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetTable()
    {
        return getTable;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetFields()
    {
        return getFields;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetTableColumnStatistics()
    {
        return getTableColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getSetTableColumnStatistics()
    {
        return setTableColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDeleteTableColumnStatistics()
    {
        return deleteTableColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetPartitionColumnStatistics()
    {
        return getPartitionColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getSetPartitionColumnStatistics()
    {
        return setPartitionColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDeletePartitionColumnStatistics()
    {
        return deletePartitionColumnStatistics;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetPartitionNames()
    {
        return getPartitionNames;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetPartitionNamesByParts()
    {
        return getPartitionNamesByParts;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetPartition()
    {
        return getPartition;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGetPartitionsByNames()
    {
        return getPartitionsByNames;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getCreateDatabase()
    {
        return createDatabase;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDropDatabase()
    {
        return dropDatabase;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAlterDatabase()
    {
        return alterDatabase;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getCreateTable()
    {
        return createTable;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDropTable()
    {
        return dropTable;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAlterTable()
    {
        return alterTable;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAddPartitions()
    {
        return addPartitions;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDropPartition()
    {
        return dropPartition;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAlterPartition()
    {
        return alterPartition;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGrantTablePrivileges()
    {
        return grantTablePrivileges;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getRevokeTablePrivileges()
    {
        return revokeTablePrivileges;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getListTablePrivileges()
    {
        return listTablePrivileges;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getListRoles()
    {
        return listRoles;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getGrantRole()
    {
        return grantRole;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getRevokeRole()
    {
        return revokeRole;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getListGrantedPrincipals()
    {
        return listGrantedPrincipals;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getListRoleGrants()
    {
        return listRoleGrants;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getCreateRole()
    {
        return createRole;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getDropRole()
    {
        return dropRole;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getOpenTransaction()
    {
        return openTransaction;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getCommitTransaction()
    {
        return commitTransaction;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAbortTransaction()
    {
        return abortTransaction;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAcquireLock()
    {
        return acquireLock;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getCheckLock()
    {
        return checkLock;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getUnlock()
    {
        return unlock;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getValidWriteIds()
    {
        return validWriteIds;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAllocateWriteId()
    {
        return allocateWriteId;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getUpdateTableWriteId()
    {
        return updateTableWriteId;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAlterPartitions()
    {
        return alterPartitions;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAddDynamicPartitions()
    {
        return addDynamicPartitions;
    }

    @Managed
    @Nested
    public ThriftMetastoreApiStats getAlterTransactionalTable()
    {
        return alterTransactionalTable;
    }
}
