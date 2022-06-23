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
package io.trino.plugin.hive.metastore.dlf;

import com.aliyun.datalake.metastore.common.ProxyMode;
import com.aliyun.datalake.metastore.common.Version;
import com.aliyun.datalake.metastore.common.functional.FunctionalUtils;
import com.aliyun.datalake.metastore.common.functional.ThrowingConsumer;
import com.aliyun.datalake.metastore.common.functional.ThrowingFunction;
import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastoreFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class ProxyTrinoClient
        implements HiveMetastore
{
    private static final Logger logger = Logger.get(ProxyTrinoClient.class);
    private final ProxyMode proxyMode;

    private AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig;

    // Dlf Client
    private HiveMetastore dlfMetaStoreClient;

    // Hive Client
    private HiveMetastore bridgingHiveMetastore;

    // ReadWrite Client
    private HiveMetastore readWriteClient;

    // Extra Write Client
    private Optional<HiveMetastore> extraClient;

    // Allow failure
    private boolean allowFailure;

    private final String readWriteClientType;

    @Inject
    public ProxyTrinoClient(BridgingHiveMetastoreFactory bridgingHiveMetastoreFactory,
                            AlibabaDlfMetaStoreClient alibabaDlfMetaStoreClient,
                            AlibabaDlfMetaStoreConfig dlfConfig) throws TrinoException
    {
        long startTime = System.currentTimeMillis();

        logger.info("ProxyMetaStoreClient start, datalake-metastore-client-version:%s", Version.DATALAKE_METASTORE_CLIENT_VERSION);
        this.alibabaDlfMetaStoreConfig = dlfConfig;
        this.proxyMode = dlfConfig.getProxyMode();

        // init logging if needed
        ProxyLogUtils.initLogUtils(proxyMode, alibabaDlfMetaStoreConfig.getLogStore().orElse(""),
                alibabaDlfMetaStoreConfig.getEnableRecordActionLog(), alibabaDlfMetaStoreConfig.getEnableRecordLog());

        this.dlfMetaStoreClient = alibabaDlfMetaStoreClient;
        if (bridgingHiveMetastoreFactory != null) {
            this.bridgingHiveMetastore = bridgingHiveMetastoreFactory.createMetastore(Optional.empty());
        }
        initClientByProxyMode();
        readWriteClientType = this.readWriteClient instanceof AlibabaDlfMetaStoreClient ? "alibaba-dlf" : "hive";

        logger.info("ProxyMetaStoreClient end, cost:%dms", System.currentTimeMillis() - startTime);
    }

    public HiveMetastore getDlfMetaStoreClient()
    {
        return dlfMetaStoreClient;
    }

    public HiveMetastore getBridgingHiveMetastore()
    {
        return bridgingHiveMetastore;
    }

    public void initClientByProxyMode()
    {
        switch (proxyMode) {
            case METASTORE_ONLY:
                this.readWriteClient = bridgingHiveMetastore;
                this.extraClient = Optional.empty();
                break;
            case METASTORE_DLF_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = bridgingHiveMetastore;
                this.extraClient = Optional.of(dlfMetaStoreClient);
                break;
            case METASTORE_DLF_SUCCESS:
                this.readWriteClient = bridgingHiveMetastore;
                this.extraClient = Optional.of(dlfMetaStoreClient);
                System.out.println(readWriteClient);
                System.out.println(extraClient);
                break;
            case DLF_METASTORE_SUCCESS:
                this.readWriteClient = dlfMetaStoreClient;
                this.extraClient = Optional.of(bridgingHiveMetastore);
                break;
            case DLF_METASTORE_FAILURE:
                this.allowFailure = true;
                this.readWriteClient = dlfMetaStoreClient;
                this.extraClient = Optional.of(bridgingHiveMetastore);
                break;
            case DLF_ONLY:
                this.readWriteClient = dlfMetaStoreClient;
                this.extraClient = Optional.empty();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + proxyMode);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return call(this.readWriteClient, client -> client.getDatabase(databaseName), "getDatabase", databaseName);
    }

    @Override
    public List<String> getAllDatabases()
    {
        return call(this.readWriteClient, client -> client.getAllDatabases(), "getAllDatabases");
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return call(this.readWriteClient, client -> client.getTable(databaseName, tableName), "getTable",
                databaseName, tableName);
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return call(this.readWriteClient, client -> client.getSupportedColumnStatistics(type), "getSupportedColumnStatistics", type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return call(this.readWriteClient, client -> client.getTableStatistics(table), "getTableStatistics", table);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return call(this.readWriteClient, client -> client.getPartitionStatistics(table, partitions), "getPartitionStatistics", table, partitions);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName,
                                      AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        this.run(client -> client.updateTableStatistics(databaseName, tableName, transaction, update),
                "updateTableStatistics", databaseName, tableName);
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        this.run(client -> client.updatePartitionStatistics(table, updates), "updatePartitionStatistics", table);
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return call(this.readWriteClient, client -> client.getAllTables(databaseName), "getAllTables", databaseName);
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return call(this.readWriteClient, client -> client.getTablesWithParameter(databaseName, parameterKey, parameterValue), "getTablesWithParameter", databaseName, parameterKey, parameterValue);
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return call(this.readWriteClient, client -> client.getAllViews(databaseName), "getAllViews", databaseName);
    }

    @Override
    public void createDatabase(Database database)
    {
        this.run(client -> client.createDatabase(database), "createDatabase", database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        this.run(client -> client.dropDatabase(databaseName, deleteData), "dropDatabase", databaseName);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        this.run(client -> client.renameDatabase(databaseName, newDatabaseName), "renameDatabase", databaseName,
                newDatabaseName);
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        this.run(client -> client.setDatabaseOwner(databaseName, principal), "setDatabaseOwner", databaseName, principal);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        this.run(client -> client.createTable(table, principalPrivileges), "createTable", table, principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        this.run(client -> client.dropTable(databaseName, tableName, deleteData), "dropTable", databaseName,
                tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        this.run(client -> client.replaceTable(databaseName, tableName, newTable, principalPrivileges), "replaceTable",
                databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        this.run(client -> client.renameTable(databaseName, tableName, newDatabaseName, newTableName), "renameTable",
                databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        this.run(client -> client.commentTable(databaseName, tableName, comment), "commentTable", databaseName,
                tableName, comment);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        this.run(client -> client.setTableOwner(databaseName, tableName, principal), "setTableOwner",
                databaseName, tableName, principal);
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        this.run(client -> client.commentColumn(databaseName, tableName, columnName, comment), "commentColumn", databaseName, tableName, columnName, comment);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        this.run(client -> client.addColumn(databaseName, tableName, columnName, columnType, columnComment), "addColumn",
                databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        this.run(client -> client.renameColumn(databaseName, tableName, oldColumnName, newColumnName), "renameColumn",
                databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        this.run(client -> client.dropColumn(databaseName, tableName, columnName), "dropColumn", databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return call(this.readWriteClient, client -> client.getPartition(table, partitionValues), "getPartition", table, partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return this.readWriteClient.getPartitionNamesByFilter(databaseName, tableName, columnNames, partitionKeysFilter);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return call(this.readWriteClient, client -> client.getPartitionsByNames(table, partitionNames), "getPartitionsByNames", table, partitionNames);
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        this.run(client -> client.addPartitions(databaseName, tableName, partitions), "addPartitions",
                databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        this.run(client -> client.dropPartition(databaseName, tableName, parts, deleteData), "dropPartition",
                databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        this.run(client -> client.alterPartition(databaseName, tableName, partition), "alterPartition",
                databaseName, tableName, partition);
    }

    @Override
    public void createRole(String role, String grantor)
    {
        this.run(client -> client.createRole(role, grantor), "createRole", role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        this.run(client -> client.dropRole(role), "dropRole", role);
    }

    @Override
    public Set<String> listRoles()
    {
        return call(this.readWriteClient, client -> client.listRoles(), "listRoles");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        this.run(client -> client.grantRoles(roles, grantees, adminOption, grantor), "grantRoles", roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        this.run(client -> client.revokeRoles(roles, grantees, adminOption, grantor), "revokeRoles", roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return call(this.readWriteClient, client -> client.listGrantedPrincipals(role), "listGrantedPrincipals", role);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return call(this.readWriteClient, client -> client.listRoleGrants(principal), "listRoleGrants", principal);
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        this.run(client -> client.grantTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption), "grantTablePrivileges",
                databaseName, tableName, tableOwner, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        run(client -> client.revokeTablePrivileges(databaseName, tableName, tableOwner, grantee, grantor, privileges, grantOption), "revokeTablePrivileges",
                databaseName, tableName, tableOwner, grantee, privileges);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return call(this.readWriteClient, client -> client.listTablePrivileges(databaseName, tableName, tableOwner, principal), "listTablePrivileges", databaseName, tableName, tableOwner, principal);
    }

    public void run(ThrowingConsumer<HiveMetastore, TrinoException> consumer, String actionName,
                    Object... parameters) throws TrinoException
    {
        FunctionalUtils.run(this.readWriteClient, extraClient, allowFailure, consumer, this.readWriteClientType,
                actionName, parameters);
    }

    public <R> R call(HiveMetastore client, ThrowingFunction<HiveMetastore, R, TrinoException> consumer,
                      String actionName, Object... parameters) throws TrinoException
    {
        return FunctionalUtils.call(client, Optional.empty(), allowFailure, consumer, this.readWriteClientType,
                actionName, parameters);
    }
}
