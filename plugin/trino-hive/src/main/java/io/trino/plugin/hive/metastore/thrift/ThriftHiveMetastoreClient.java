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
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.hive.thrift.metastore.AbortTxnRequest;
import io.trino.hive.thrift.metastore.AddDynamicPartitions;
import io.trino.hive.thrift.metastore.AllocateTableWriteIdsRequest;
import io.trino.hive.thrift.metastore.AllocateTableWriteIdsResponse;
import io.trino.hive.thrift.metastore.AlterPartitionsRequest;
import io.trino.hive.thrift.metastore.AlterTableRequest;
import io.trino.hive.thrift.metastore.CheckLockRequest;
import io.trino.hive.thrift.metastore.ClientCapabilities;
import io.trino.hive.thrift.metastore.ClientCapability;
import io.trino.hive.thrift.metastore.ColumnStatistics;
import io.trino.hive.thrift.metastore.ColumnStatisticsDesc;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.CommitTxnRequest;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.hive.thrift.metastore.Database;
import io.trino.hive.thrift.metastore.EnvironmentContext;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.Function;
import io.trino.hive.thrift.metastore.GetRoleGrantsForPrincipalRequest;
import io.trino.hive.thrift.metastore.GetRoleGrantsForPrincipalResponse;
import io.trino.hive.thrift.metastore.GetTableRequest;
import io.trino.hive.thrift.metastore.GetValidWriteIdsRequest;
import io.trino.hive.thrift.metastore.GrantRevokePrivilegeRequest;
import io.trino.hive.thrift.metastore.GrantRevokeRoleRequest;
import io.trino.hive.thrift.metastore.GrantRevokeRoleResponse;
import io.trino.hive.thrift.metastore.HeartbeatTxnRangeRequest;
import io.trino.hive.thrift.metastore.HiveObjectPrivilege;
import io.trino.hive.thrift.metastore.HiveObjectRef;
import io.trino.hive.thrift.metastore.LockRequest;
import io.trino.hive.thrift.metastore.LockResponse;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.OpenTxnRequest;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.PartitionsStatsRequest;
import io.trino.hive.thrift.metastore.PrincipalType;
import io.trino.hive.thrift.metastore.PrivilegeBag;
import io.trino.hive.thrift.metastore.Role;
import io.trino.hive.thrift.metastore.RolePrincipalGrant;
import io.trino.hive.thrift.metastore.Table;
import io.trino.hive.thrift.metastore.TableMeta;
import io.trino.hive.thrift.metastore.TableStatsRequest;
import io.trino.hive.thrift.metastore.TableValidWriteIds;
import io.trino.hive.thrift.metastore.ThriftHiveMetastore;
import io.trino.hive.thrift.metastore.TxnToWriteId;
import io.trino.hive.thrift.metastore.UnlockRequest;
import io.trino.plugin.base.util.LoggingInvocationHandler;
import io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport;
import io.trino.spi.connector.RelationType;
import jakarta.annotation.Nullable;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.hive.thrift.metastore.GrantRevokeType.GRANT;
import static io.trino.hive.thrift.metastore.GrantRevokeType.REVOKE;
import static io.trino.metastore.TableInfo.PRESTO_VIEW_COMMENT;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.NOT_SUPPORTED;
import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.SUPPORTED;
import static io.trino.plugin.hive.metastore.thrift.MetastoreSupportsDateStatistics.DateStatisticsSupport.UNKNOWN;
import static io.trino.plugin.hive.metastore.thrift.TxnUtils.createValidReadTxnList;
import static io.trino.plugin.hive.metastore.thrift.TxnUtils.createValidTxnWriteIdList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.thrift.TApplicationException.UNKNOWN_METHOD;

public class ThriftHiveMetastoreClient
        implements ThriftMetastoreClient
{
    private static final Logger log = Logger.get(ThriftHiveMetastoreClient.class);
    private static final char CATALOG_DB_THRIFT_NAME_MARKER = '@';
    private static final String CATALOG_DB_SEPARATOR = "#";
    private static final String DB_EMPTY_MARKER = "!";

    private final TransportSupplier transportSupplier;
    private TTransport transport;
    protected ThriftHiveMetastore.Iface client;
    private final String hostname;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics;
    private final boolean metastoreSupportsTableMeta;
    private final AtomicInteger chosenGetTableAlternative;
    private final AtomicInteger chosenAlterTransactionalTableAlternative;
    private final AtomicInteger chosenAlterPartitionsAlternative;
    private final Optional<String> catalogName;

    public ThriftHiveMetastoreClient(
            TransportSupplier transportSupplier,
            String hostname,
            Optional<String> catalogName,
            MetastoreSupportsDateStatistics metastoreSupportsDateStatistics,
            boolean metastoreSupportsTableMeta,
            AtomicInteger chosenGetTableAlternative,
            AtomicInteger chosenAlterTransactionalTableAlternative,
            AtomicInteger chosenAlterPartitionsAlternative)
            throws TTransportException
    {
        this.transportSupplier = requireNonNull(transportSupplier, "transportSupplier is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
        this.metastoreSupportsDateStatistics = requireNonNull(metastoreSupportsDateStatistics, "metastoreSupportsDateStatistics is null");
        this.metastoreSupportsTableMeta = metastoreSupportsTableMeta;
        this.chosenGetTableAlternative = requireNonNull(chosenGetTableAlternative, "chosenGetTableAlternative is null");
        this.chosenAlterTransactionalTableAlternative = requireNonNull(chosenAlterTransactionalTableAlternative, "chosenAlterTransactionalTableAlternative is null");
        this.chosenAlterPartitionsAlternative = requireNonNull(chosenAlterPartitionsAlternative, "chosenAlterPartitionsAlternative is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");

        connect();
    }

    private void connect()
            throws TTransportException
    {
        transport = transportSupplier.createTransport();
        ThriftHiveMetastore.Iface client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
        if (log.isDebugEnabled()) {
            client = newProxy(ThriftHiveMetastore.Iface.class, new LoggingInvocationHandler(client, log::debug));
        }
        this.client = client;
    }

    @Override
    public void close()
    {
        disconnect();
    }

    private void disconnect()
    {
        transport.close();
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        if (catalogName.isPresent()) {
            return client.getDatabases(prependCatalogToDbName(catalogName, null));
        }
        return client.getAllDatabases();
    }

    @Override
    public Database getDatabase(String dbName)
            throws TException
    {
        return client.getDatabase(prependCatalogToDbName(catalogName, dbName));
    }

    @Override
    public List<TableMeta> getTableMeta(String databaseName)
            throws TException
    {
        // TODO: remove this once Unity adds support for getTableMeta
        if (!metastoreSupportsTableMeta) {
            String catalogDatabaseName = prependCatalogToDbName(catalogName, databaseName);
            Map<String, TableMeta> tables = new HashMap<>();
            client.getTables(catalogDatabaseName, ".*").forEach(name -> tables.put(name, new TableMeta(databaseName, name, RelationType.TABLE.toString())));
            client.getTablesByType(catalogDatabaseName, ".*", VIRTUAL_VIEW.name()).forEach(name -> {
                TableMeta tableMeta = new TableMeta(databaseName, name, VIRTUAL_VIEW.name());
                // This makes all views look like a Trino view, so that they are not filtered out during SHOW VIEWS
                tableMeta.setComments(PRESTO_VIEW_COMMENT);
                tables.put(name, tableMeta);
            });
            return ImmutableList.copyOf(tables.values());
        }

        if (databaseName.indexOf('*') >= 0 || databaseName.indexOf('|') >= 0) {
            // in this case we replace any pipes with a glob and then filter the output
            return client.getTableMeta(prependCatalogToDbName(catalogName, databaseName.replace('|', '*')), "*", ImmutableList.of()).stream()
                    .filter(tableMeta -> tableMeta.getDbName().equals(databaseName))
                    .collect(toImmutableList());
        }
        return client.getTableMeta(prependCatalogToDbName(catalogName, databaseName), "*", ImmutableList.of());
    }

    @Override
    public void createDatabase(Database database)
            throws TException
    {
        client.createDatabase(catalogName.isEmpty()
                ? database
                : database.deepCopy().setCatalogName(catalogName.orElseThrow()));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        client.dropDatabase(prependCatalogToDbName(catalogName, databaseName), deleteData, cascade);
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        client.alterDatabase(prependCatalogToDbName(catalogName, databaseName), database);
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        client.createTable(catalogName.isEmpty()
                ? table
                : table.deepCopy().setCatName(catalogName.orElseThrow()));
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        client.dropTable(prependCatalogToDbName(catalogName, databaseName), name, deleteData);
    }

    @Override
    public void alterTableWithEnvironmentContext(String databaseName, String tableName, Table newTable, EnvironmentContext context)
            throws TException
    {
        client.alterTableWithEnvironmentContext(prependCatalogToDbName(catalogName, databaseName), tableName, newTable, context);
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return alternativeCall(
                ThriftHiveMetastoreClient::defaultIsValidExceptionalResponse,
                chosenGetTableAlternative,
                () -> {
                    GetTableRequest request = new GetTableRequest(databaseName, tableName);
                    catalogName.ifPresent(request::setCatName);
                    request.setCapabilities(new ClientCapabilities(ImmutableList.of(ClientCapability.INSERT_ONLY_TABLES)));
                    return client.getTableReq(request).getTable();
                },
                () -> client.getTable(prependCatalogToDbName(catalogName, databaseName), tableName));
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return client.getFields(prependCatalogToDbName(catalogName, databaseName), tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        TableStatsRequest tableStatsRequest = new TableStatsRequest(databaseName, tableName, columnNames);
        catalogName.ifPresent(tableStatsRequest::setCatName);
        return client.getTableStatisticsReq(tableStatsRequest).getTableStats();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        setColumnStatistics(
                format("table %s.%s", databaseName, tableName),
                statistics,
                stats -> {
                    ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(true, databaseName, tableName);
                    catalogName.ifPresent(statisticsDescription::setCatName);
                    ColumnStatistics request = new ColumnStatistics(statisticsDescription, stats);
                    client.updateTableColumnStatistics(request);
                });
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        client.deleteTableColumnStatistics(prependCatalogToDbName(catalogName, databaseName), tableName, columnName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        PartitionsStatsRequest partitionsStatsRequest = new PartitionsStatsRequest(databaseName, tableName, columnNames, partitionNames);
        catalogName.ifPresent(partitionsStatsRequest::setCatName);
        return client.getPartitionsStatisticsReq(partitionsStatsRequest).getPartStats();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        setColumnStatistics(
                format("partition of table %s.%s", databaseName, tableName),
                statistics,
                stats -> {
                    ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(false, databaseName, tableName);
                    catalogName.ifPresent(statisticsDescription::setCatName);
                    statisticsDescription.setPartName(partitionName);
                    ColumnStatistics request = new ColumnStatistics(statisticsDescription, stats);
                    client.updatePartitionColumnStatistics(request);
                });
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        client.deletePartitionColumnStatistics(prependCatalogToDbName(catalogName, databaseName), tableName, partitionName, columnName);
    }

    private void setColumnStatistics(String objectName, List<ColumnStatisticsObj> statistics, UnaryCall<List<ColumnStatisticsObj>> saveColumnStatistics)
            throws TException
    {
        boolean containsDateStatistics = statistics.stream().anyMatch(stats -> stats.getStatsData().isSetDateStats());

        DateStatisticsSupport dateStatisticsSupported = this.metastoreSupportsDateStatistics.isSupported();
        if (containsDateStatistics && dateStatisticsSupported == NOT_SUPPORTED) {
            log.debug("Skipping date statistics for %s because metastore does not support them", objectName);
            statistics = statistics.stream()
                    .filter(stats -> !stats.getStatsData().isSetDateStats())
                    .collect(toImmutableList());
            containsDateStatistics = false;
        }

        if (!containsDateStatistics || dateStatisticsSupported == SUPPORTED) {
            saveColumnStatistics.call(statistics);
            return;
        }

        List<ColumnStatisticsObj> statisticsExceptDate = statistics.stream()
                .filter(stats -> !stats.getStatsData().isSetDateStats())
                .collect(toImmutableList());

        List<ColumnStatisticsObj> dateStatistics = statistics.stream()
                .filter(stats -> stats.getStatsData().isSetDateStats())
                .collect(toImmutableList());

        verify(!dateStatistics.isEmpty() && dateStatisticsSupported == UNKNOWN);

        if (!statisticsExceptDate.isEmpty()) {
            saveColumnStatistics.call(statisticsExceptDate);
        }

        try {
            saveColumnStatistics.call(dateStatistics);
        }
        catch (TException e) {
            // When `dateStatistics.size() > 1` we expect something like "TApplicationException: Required field 'colName' is unset! Struct:ColumnStatisticsObj(colName:null, colType:null, statsData:null)"
            // When `dateStatistics.size() == 1` we expect something like "TTransportException: java.net.SocketTimeoutException: Read timed out"
            log.warn(e, "Failed to save date statistics for %s. Metastore might not support date statistics", objectName);
            if (!statisticsExceptDate.isEmpty()) {
                this.metastoreSupportsDateStatistics.failed();
            }
            return;
        }
        this.metastoreSupportsDateStatistics.succeeded();
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return client.getPartitionNames(prependCatalogToDbName(catalogName, databaseName), tableName, (short) -1);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.getPartitionNamesPs(prependCatalogToDbName(catalogName, databaseName), tableName, partitionValues, (short) -1);
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        if (catalogName.isEmpty()) {
            return client.addPartitions(newPartitions);
        }

        return client.addPartitions(newPartitions.stream()
                .map(partition -> partition.deepCopy().setCatName(catalogName.orElseThrow()))
                .collect(toImmutableList()));
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return client.dropPartition(prependCatalogToDbName(catalogName, databaseName), tableName, partitionValues, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        client.alterPartition(prependCatalogToDbName(catalogName, databaseName), tableName, partition);
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.getPartition(prependCatalogToDbName(catalogName, databaseName), tableName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return client.getPartitionsByNames(prependCatalogToDbName(catalogName, databaseName), tableName, partitionNames);
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return client.listRoles(principalName, principalType);
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException
    {
        HiveObjectRef hiveObjectRefParam = catalogName.isEmpty()
                ? hiveObjectRef
                : hiveObjectRef.deepCopy().setCatName(catalogName.orElseThrow());

        return client.listPrivileges(principalName, principalType, hiveObjectRefParam);
    }

    @Override
    public List<String> getRoleNames()
            throws TException
    {
        return client.getRoleNames();
    }

    @Override
    public void createRole(String roleName, String grantor)
            throws TException
    {
        Role role = new Role(roleName, 0, grantor);
        client.createRole(role);
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        client.dropRole(role);
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return client.grantRevokePrivileges(new GrantRevokePrivilegeRequest(GRANT, privilegeBag)).isSuccess();
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag, boolean revokeGrantOption)
            throws TException
    {
        GrantRevokePrivilegeRequest grantRevokePrivilegeRequest = new GrantRevokePrivilegeRequest(REVOKE, privilegeBag);
        grantRevokePrivilegeRequest.setRevokeGrantOption(revokeGrantOption);
        return client.grantRevokePrivileges(grantRevokePrivilegeRequest).isSuccess();
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                if (grant.isGrantOption() == grantOption) {
                    return;
                }
                if (!grant.isGrantOption() && grantOption) {
                    revokeRole(role, granteeName, granteeType, false);
                    break;
                }
            }
        }
        createGrant(role, granteeName, granteeType, grantorName, grantorType, grantOption);
    }

    private void createGrant(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(GRANT);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantor(grantorName);
        request.setGrantorType(grantorType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grantRevokeRole(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        RolePrincipalGrant currentGrant = null;
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                currentGrant = grant;
                break;
            }
        }

        if (currentGrant == null) {
            return;
        }

        if (!currentGrant.isGrantOption() && grantOption) {
            return;
        }

        removeGrant(role, granteeName, granteeType, grantOption);
    }

    private void removeGrant(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(REVOKE);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grantRevokeRole(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String principalName, PrincipalType principalType)
            throws TException
    {
        GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest(principalName, principalType);
        GetRoleGrantsForPrincipalResponse resp = client.getRoleGrantsForPrincipal(request);
        return ImmutableList.copyOf(resp.getPrincipalGrants());
    }

    @Override
    public void setUGI(String userName)
            throws TException
    {
        client.setUgi(userName, new ArrayList<>());
    }

    @Override
    public long openTransaction(String user)
            throws TException
    {
        OpenTxnRequest request = new OpenTxnRequest(1, user, hostname);
        return client.openTxns(request).getTxnIds().get(0);
    }

    @Override
    public void commitTransaction(long transactionId)
            throws TException
    {
        client.commitTxn(new CommitTxnRequest(transactionId));
    }

    @Override
    public void abortTransaction(long transactionId)
            throws TException
    {
        client.abortTxn(new AbortTxnRequest(transactionId));
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
            throws TException
    {
        HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(transactionId, transactionId);
        client.heartbeatTxnRange(request);
    }

    @Override
    public LockResponse acquireLock(LockRequest lockRequest)
            throws TException
    {
        return client.lock(lockRequest);
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws TException
    {
        return client.checkLock(new CheckLockRequest(lockId));
    }

    @Override
    public void unlock(long lockId)
            throws TException
    {
        client.unlock(new UnlockRequest(lockId));
    }

    @Override
    public String getValidWriteIds(List<String> tableList, long currentTransactionId)
            throws TException
    {
        // Pass currentTxn as 0L to get the recent snapshot of valid transactions in Hive
        // Do not pass currentTransactionId instead as it will break Hive's listing of delta directories if major compaction
        // deletes delta directories for valid transactions that existed at the time transaction is opened
        String validTransactions = createValidReadTxnList(client.getOpenTxns(), 0L);
        GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(tableList, validTransactions);
        List<TableValidWriteIds> validWriteIds = client.getValidWriteIds(request).getTblValidWriteIds();
        return createValidTxnWriteIdList(currentTransactionId, validWriteIds);
    }

    @Override
    public String getConfigValue(String name, String defaultValue)
            throws TException
    {
        return client.getConfigValue(name, defaultValue);
    }

    @Override
    public String getDelegationToken(String userName)
            throws TException
    {
        return client.getDelegationToken(userName, userName);
    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIds(String dbName, String tableName, List<Long> transactionIds)
            throws TException
    {
        AllocateTableWriteIdsRequest request = new AllocateTableWriteIdsRequest(dbName, tableName);
        request.setTxnIds(transactionIds);
        AllocateTableWriteIdsResponse response = client.allocateTableWriteIds(request);
        return response.getTxnToWriteIds();
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
            throws TException
    {
        alternativeCall(
                exception -> !isUnknownMethodExceptionalResponse(exception),
                chosenAlterPartitionsAlternative,
                () -> {
                    AlterPartitionsRequest request = new AlterPartitionsRequest(dbName, tableName, partitions);
                    catalogName.ifPresent(request::setCatName);
                    request.setWriteId(writeId);
                    client.alterPartitionsReq(request);
                    return null;
                },
                () -> {
                    client.alterPartitionsWithEnvironmentContext(prependCatalogToDbName(catalogName, dbName), tableName, partitions, new EnvironmentContext());
                    return null;
                });
    }

    @Override
    public void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, DataOperationType operation)
            throws TException
    {
        AddDynamicPartitions request = new AddDynamicPartitions(transactionId, writeId, dbName, tableName, partitionNames);
        request.setOperationType(operation);
        client.addDynamicPartitions(request);
    }

    @Override
    public void alterTransactionalTable(Table table, long transactionId, long writeId, EnvironmentContext environmentContext)
            throws TException
    {
        long originalWriteId = table.getWriteId();
        alternativeCall(
                exception -> !isUnknownMethodExceptionalResponse(exception),
                chosenAlterTransactionalTableAlternative,
                () -> {
                    table.setWriteId(writeId);
                    checkArgument(writeId >= table.getWriteId(), "The writeId supplied %s should be greater than or equal to the table writeId %s", writeId, table.getWriteId());
                    AlterTableRequest request = new AlterTableRequest(table.getDbName(), table.getTableName(), table);
                    catalogName.ifPresent(request::setCatName);
                    request.setValidWriteIdList(getValidWriteIds(ImmutableList.of(format("%s.%s", table.getDbName(), table.getTableName())), transactionId));
                    request.setWriteId(writeId);
                    request.setEnvironmentContext(environmentContext);
                    client.alterTableReq(request);
                    return null;
                },
                () -> {
                    table.setWriteId(originalWriteId);
                    client.alterTableWithEnvironmentContext(table.getDbName(), table.getTableName(), table, environmentContext);
                    return null;
                });
    }

    @Override
    public Function getFunction(String databaseName, String functionName)
            throws TException
    {
        return client.getFunction(prependCatalogToDbName(catalogName, databaseName), functionName);
    }

    @Override
    public Collection<String> getFunctions(String databaseName, String functionNamePattern)
            throws TException
    {
        return client.getFunctions(prependCatalogToDbName(catalogName, databaseName), functionNamePattern);
    }

    @Override
    public void createFunction(Function function)
            throws TException
    {
        client.createFunction(catalogName.isEmpty()
                ? function
                : function.deepCopy().setCatName(catalogName.orElseThrow()));
    }

    @Override
    public void alterFunction(Function function)
            throws TException
    {
        // Hive 3 does not actually replace the content of the function
        // https://github.com/apache/hive/blob/rel/release-3.1.2/standalone-metastore/src/main/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L9310
        // Fall back to use drop & create for doing the replace function operation
        dropFunction(function.getDbName(), function.getFunctionName());
        createFunction(function);
    }

    @Override
    public void dropFunction(String databaseName, String functionName)
            throws TException
    {
        client.dropFunction(prependCatalogToDbName(catalogName, databaseName), functionName);
    }

    // Method needs to be final for @SafeVarargs to work
    @SafeVarargs
    @VisibleForTesting
    final <T> T alternativeCall(
            Predicate<Exception> isValidExceptionalResponse,
            AtomicInteger chosenAlternative,
            AlternativeCall<T>... alternatives)
            throws TException
    {
        checkArgument(alternatives.length > 0, "No alternatives");
        int chosen = chosenAlternative.get();
        checkArgument(chosen == Integer.MAX_VALUE || (0 <= chosen && chosen < alternatives.length), "Bad chosen alternative value: %s", chosen);

        if (chosen != Integer.MAX_VALUE) {
            return alternatives[chosen].execute();
        }

        Exception firstException = null;
        for (int i = 0; i < alternatives.length; i++) {
            int position = i;
            try {
                T result = alternatives[i].execute();
                chosenAlternative.updateAndGet(currentChosen -> Math.min(currentChosen, position));
                return result;
            }
            catch (TException | RuntimeException exception) {
                if (isValidExceptionalResponse.test(exception)) {
                    // This is likely a valid response. We are not settling on an alternative yet.
                    // We will do it later when we get a more obviously valid response.
                    throw exception;
                }
                if (firstException == null) {
                    firstException = exception;
                }
                else if (firstException != exception) {
                    firstException.addSuppressed(exception);
                }
                // Client that threw exception is in an unknown state. We need to open it again to
                // make sure it will respond properly to the next call.
                disconnect();
                connect();
            }
        }

        verifyNotNull(firstException);
        throwIfInstanceOf(firstException, TException.class);
        throwIfUnchecked(firstException);
        throw propagate(firstException);
    }

    // TODO we should recognize exceptions which we suppress and try different alternative call
    // this requires product tests with HDP 3
    private static boolean defaultIsValidExceptionalResponse(Exception exception)
    {
        if (exception instanceof NoSuchObjectException) {
            return true;
        }

        if (exception.toString().contains("AccessControlException")) {
            // e.g. io.trino.hive.thrift.metastore.MetaException: org.apache.hadoop.security.AccessControlException: Permission denied: ...
            return true;
        }

        return false;
    }

    private static boolean isUnknownMethodExceptionalResponse(Exception exception)
    {
        if (!(exception instanceof TApplicationException applicationException)) {
            return false;
        }

        return applicationException.getType() == UNKNOWN_METHOD;
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }

    @VisibleForTesting
    @FunctionalInterface
    interface AlternativeCall<T>
    {
        T execute()
                throws TException;
    }

    @FunctionalInterface
    private interface UnaryCall<A>
    {
        void call(A arg)
                throws TException;
    }

    public interface TransportSupplier
    {
        TTransport createTransport()
                throws TTransportException;
    }

    /**
     * To construct a pattern using database and catalog name that Hive Thrift Server.
     * Based on the Hive's <a href="https://github.com/apache/hive/blob/master/standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/utils/MetaStoreUtils.java">implementation</a>.
     *
     * @param catalogName hive catalog name
     * @param databaseName database name
     * @return string pattern that Hive Thrift Server understands
     */
    private static String prependCatalogToDbName(Optional<String> catalogName, @Nullable String databaseName)
    {
        if (catalogName.isEmpty()) {
            return databaseName;
        }

        StringBuilder catalogDatabaseName = new StringBuilder()
                .append(CATALOG_DB_THRIFT_NAME_MARKER)
                .append(catalogName.orElseThrow())
                .append(CATALOG_DB_SEPARATOR);
        if (databaseName != null) {
            if (databaseName.isEmpty()) {
                catalogDatabaseName.append(DB_EMPTY_MARKER);
            }
            else {
                catalogDatabaseName.append(databaseName);
            }
        }
        return catalogDatabaseName.toString();
    }
}
