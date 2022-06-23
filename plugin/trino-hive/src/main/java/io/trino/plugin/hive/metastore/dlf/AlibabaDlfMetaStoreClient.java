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

import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.ProxyMode;
import com.aliyun.datalake.metastore.common.api.DataLakeAPIException;
import com.aliyun.datalake.metastore.common.functional.ThrowingFunction;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake20200710.models.PrincipalPrivilegeSet;
import com.aliyun.datalake20200710.models.UpdateTablePartitionColumnStatisticsRequest;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.dlf.converters.CatalogToTrinoConverter;
import io.trino.plugin.hive.metastore.dlf.converters.TrinoToCatalogConverter;
import io.trino.plugin.hive.metastore.dlf.exceptions.AlreadyExistsException;
import io.trino.plugin.hive.metastore.dlf.exceptions.InvalidOperationException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownDBException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownPartitionException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownTableException;
import io.trino.plugin.hive.metastore.dlf.utils.ExpressionUtils;
import io.trino.plugin.hive.metastore.dlf.utils.Utils;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.difference;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.partitionKeyFilterToStringList;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.createMetastoreColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static org.apache.hadoop.hive.common.FileUtils.makePartName;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

/**
 * Data Lake Formation MetaStore Client for Trino.
 */
public class AlibabaDlfMetaStoreClient
        implements HiveMetastore
{
    private static final Logger logger = Logger.get(AlibabaDlfMetaStoreClient.class);
    private static final String DEFAULT_METASTORE_USER = "trino";
    private static final String DEFAULT_CATALOG_ID = "";
    private static final int NO_BATCHING = -1;
    private final String catalogId;
    private final AlibabaDlfMetaStoreConfig dlfConfig;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final String defaultDir;
    private final IDataLakeMetaStore dataLakeMetaStore;
    private short pageSize;
    private int batchSizeForGetPartititon;
    private boolean isProxyModeDlfOnly;
    private boolean enableFsOperation;

    @Inject
    public AlibabaDlfMetaStoreClient(
            HdfsEnvironment hdfsEnvironment,
            AlibabaDlfMetaStoreConfig dlfConfig) throws TrinoException
    {
        requireNonNull(dlfConfig, "dlfConfig is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));

        this.dlfConfig = dlfConfig;
        this.catalogId = DEFAULT_CATALOG_ID;
        this.pageSize = dlfConfig.getPageSize();
        this.defaultDir = dlfConfig.getDefaultWarehouseDir();
        this.dataLakeMetaStore = new MetastoreFactory().getMetaStore(dlfConfig);
        this.batchSizeForGetPartititon = dlfConfig.getBatchSizeForGetPartititon();

        this.isProxyModeDlfOnly = dlfConfig.getProxyMode() == ProxyMode.DLF_ONLY;
        this.enableFsOperation = this.isProxyModeDlfOnly
                || (dlfConfig.getEnableFileOperation() && DataLakeUtil.isEnableFileOperationGray(dlfConfig.getEnableFileOperationGrayRate()));
    }

    private static List<String> buildPartitionNames(List<Column> partitionColumns, List<Partition> partitions)
    {
        return partitions.stream()
                .map(partition -> makePartitionName(partitionColumns, partition.getValues()))
                .collect(Collectors.toList());
    }

    public IDataLakeMetaStore getDataLakeMetaStore()
    {
        return dataLakeMetaStore;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return getDatabase(catalogId, databaseName);
    }

    public Optional<Database> getDatabase(String catalogId, String databaseName)
    {
        try {
            com.aliyun.datalake20200710.models.Database database = this.dataLakeMetaStore.getDatabase(catalogId, databaseName);
            return Optional.of(CatalogToTrinoConverter.toTrinoDatabase(database));
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                return Optional.empty();
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get database:";
            throw new UnknownDBException(msg + e, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        List<String> dbNames = new ArrayList<>();
        try {
            dbNames = this.dataLakeMetaStore.getDatabases(catalogId, ".*", this.pageSize);
            return dbNames;
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get all databases:";
            throw new UnknownDBException(msg + e, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            com.aliyun.datalake20200710.models.Table result = this.dataLakeMetaStore.getTable(catalogId, databaseName, tableName);
            return Optional.of(CatalogToTrinoConverter.toTrinoTable(result));
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                return Optional.empty();
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        // TODO: Not supported now.
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        List<String> dataColumns = table.getDataColumns().stream().map(col -> col.getName()).collect(Collectors.toList());
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(table.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = getTableColumnStatistics(table.getDatabaseName(), table.getTableName(), dataColumns, basicStatistics.getRowCount());
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, List<String> columns, OptionalLong rowCount)
    {
        return groupStatisticsByColumn(getTableColumnStatisticsObjs(catalogId, databaseName, tableName, columns), rowCount);
    }

    private Map<String, HiveColumnStatistics> groupStatisticsByColumn(List<ColumnStatisticsObj> statistics, OptionalLong rowCount)
    {
        return statistics.stream()
                .collect(toImmutableMap(ColumnStatisticsObj::getColName, statisticsObj -> ThriftMetastoreUtil.fromMetastoreApiColumnStatistics(statisticsObj, rowCount)));
    }

    public List<ColumnStatisticsObj> getTableColumnStatisticsObjs(
            String catalogId, String dbName, String tableName, List<String> colNames)
            throws UnsupportedOperationException
    {
        List<String> lowerCaseColNames = new ArrayList<>(colNames.size());
        for (String colName : colNames) {
            lowerCaseColNames.add(colName.toLowerCase(Locale.ENGLISH));
        }
        try {
            List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> results = dataLakeMetaStore.getTableColumnStatistics(catalogId.toLowerCase(Locale.ENGLISH), dbName.toLowerCase(Locale.ENGLISH), tableName.toLowerCase(Locale.ENGLISH), lowerCaseColNames);
            return CatalogToTrinoConverter.toHiveColumnStatsObjs(results, dlfConfig.getEnableBitVector());
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to get table column statistics: " + catalogId + "." + dbName + "." + tableName;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        List<String> dataColumns = table.getDataColumns().stream().map(col -> col.getName()).collect(Collectors.toList());
        List<String> partitionColumns = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        Map<String, HiveBasicStatistics> partitionBasicStatistics = partitions.stream()
                .collect(toImmutableMap(
                        partition -> makePartName(partitionColumns, partition.getValues()),
                        partition -> getHiveBasicStatistics(partition.getParameters())));

        Map<String, OptionalLong> partitionRowCounts = partitionBasicStatistics.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowCount()));

        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStatistics = getPartitionColumnStatistics(
                table.getDatabaseName(),
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

    private Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            List<String> columnNames,
            Map<String, OptionalLong> partitionRowCounts)
    {
        Map<String, List<ColumnStatisticsObj>> results = getPartitionStats(catalogId, databaseName, tableName, new ArrayList<>(partitionNames), columnNames);
        return results.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> groupStatisticsByColumn(entry.getValue(), partitionRowCounts.getOrDefault(entry.getKey(), OptionalLong.empty()))));
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatisticsObj(
            String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames)
    {
        List<String> lowerCaseColNames = columnNames.stream().map(col -> col.toLowerCase(Locale.ENGLISH)).collect(Collectors.toList());
        List<String> lowerPartitionNames = new ArrayList<>();
        for (String partName : partitionNames) {
            lowerPartitionNames.add(Utils.lowerCaseConvertPartName(partName));
        }
        try {
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> results = this.dataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId.toLowerCase(Locale.ENGLISH), dbName.toLowerCase(Locale.ENGLISH), tableName.toLowerCase(Locale.ENGLISH), lowerPartitionNames, lowerCaseColNames);
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> newResults = results.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newResults.size() == 0) {
                return new HashMap<String, List<ColumnStatisticsObj>>();
            }
            return CatalogToTrinoConverter.toHiveColumnStatsObjMaps(newResults, dlfConfig.getEnableBitVector());
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to get table column partiton statistics: " + catalogId + "." + dbName + "." + tableName;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    public Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> getPartitionColumnStatisticsObjOrigin(
            String catalogId, String dbName, String tableName, List<String> partitionNames, List<String> columnNames)
    {
        List<String> lowerCaseColNames = columnNames.stream().map(col -> col.toLowerCase(Locale.ENGLISH)).collect(Collectors.toList());
        List<String> lowerPartitionNames = new ArrayList<>();
        for (String partName : partitionNames) {
            lowerPartitionNames.add(Utils.lowerCaseConvertPartName(partName));
        }
        try {
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> results = this.dataLakeMetaStore.batchGetPartitionColumnStatistics(catalogId.toLowerCase(Locale.ENGLISH), dbName.toLowerCase(Locale.ENGLISH), tableName.toLowerCase(Locale.ENGLISH), lowerPartitionNames, lowerCaseColNames);
            Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> newResults = results.entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (newResults.size() == 0) {
                return new HashMap<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>();
            }
            return newResults;
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to get table column partiton statistics: " + catalogId + "." + dbName + "." + tableName;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionStats(
            final String catalogId, final String dbName, final String tableName, final List<String> partNames,
            List<String> colNames)
    {
        if (colNames.isEmpty() || partNames.isEmpty()) {
            return new HashMap<>();
        }
        Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics> b = new Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics>() {
            @Override
            public List<com.aliyun.datalake20200710.models.ColumnStatistics> run(final List<String> inputColNames)
            {
                Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics> b2 = new Batchable<String, com.aliyun.datalake20200710.models.ColumnStatistics>() {
                    @Override
                    public List<com.aliyun.datalake20200710.models.ColumnStatistics> run(List<String> inputPartNames)
                    {
                        List<com.aliyun.datalake20200710.models.ColumnStatistics> statistics = new ArrayList<>();
                        Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> objs = getPartitionColumnStatisticsObjOrigin(catalogId, dbName, tableName, inputPartNames, inputColNames);
                        Iterator<Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = objs.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> entry = iterator.next();
                            com.aliyun.datalake20200710.models.ColumnStatisticsDesc desc = new com.aliyun.datalake20200710.models.ColumnStatisticsDesc();
                            desc.setPartitionName(entry.getKey());
                            com.aliyun.datalake20200710.models.ColumnStatistics stat = new com.aliyun.datalake20200710.models.ColumnStatistics();
                            stat.setColumnStatisticsDesc(desc);
                            stat.setColumnStatisticsObjList(entry.getValue());
                            statistics.add(stat);
                        }
                        return statistics;
                    }
                };
                return runBatchedGetPartitions(partNames, b2);
            }
        };
        List<com.aliyun.datalake20200710.models.ColumnStatistics> columnStatisticsList = runBatchedGetPartitions(colNames, b);

        Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> objMap = new HashMap<>();
        for (com.aliyun.datalake20200710.models.ColumnStatistics columnStatistics : columnStatisticsList) {
            if (objMap.get(columnStatistics.getColumnStatisticsDesc().getPartitionName()) != null) {
                objMap.get(columnStatistics.getColumnStatisticsDesc().getPartitionName()).addAll(columnStatistics.getColumnStatisticsObjList());
            }
            else {
                List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> objs = new ArrayList<>();
                objs.addAll(columnStatistics.getColumnStatisticsObjList());
                objMap.put(columnStatistics.getColumnStatisticsDesc().getPartitionName(), objs);
            }
        }
        try {
            return CatalogToTrinoConverter.toHiveColumnStatsObjMaps(objMap, dlfConfig.getEnableBitVector());
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "catalog to HiveColumnStatsObjMaps error " + e, e);
        }
    }

    private <I, R> List<R> runBatchedGetPartitions(List<I> input, Batchable<I, R> runnable)
    {
        if (batchSizeForGetPartititon == NO_BATCHING || batchSizeForGetPartititon >= input.size()) {
            return runnable.run(input);
        }
        List<R> result = new ArrayList<R>(input.size());
        for (int fromIndex = 0, toIndex = 0; toIndex < input.size(); fromIndex = toIndex) {
            toIndex = Math.min(fromIndex + batchSizeForGetPartititon, input.size());
            List<I> batchedInput = input.subList(fromIndex, toIndex);
            List<R> batchedOutput = runnable.run(batchedInput);
            if (batchedOutput != null) {
                result.addAll(batchedOutput);
            }
        }
        return result;
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName,
                                      AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table originalTable = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        PartitionStatistics currentStatistics = getTableStatistics(originalTable);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        com.aliyun.datalake20200710.models.TableInput modifiedTable = TrinoToCatalogConverter.toCatalogTableInput(originalTable);
        HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
        modifiedTable.setParameters(ThriftMetastoreUtil.updateStatisticsParameters(modifiedTable.getParameters(), basicStatistics));
        replaceTable(databaseName, tableName, modifiedTable, null);

        OptionalLong rowCount = basicStatistics.getRowCount();
        List<ColumnStatisticsObj> metastoreColumnStatistics = updatedStatistics.getColumnStatistics().entrySet().stream()
                .map(entry -> createMetastoreColumnStatistics(entry.getKey(), originalTable.getColumn(entry.getKey()).get().getType(), entry.getValue(), rowCount))
                .collect(toImmutableList());
        if (!metastoreColumnStatistics.isEmpty()) {
            ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(true, databaseName, tableName);
            columnStatisticsDesc.setCatName(catalogId);
            columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);

            ColumnStatistics columnStatistics = new ColumnStatistics(columnStatisticsDesc, metastoreColumnStatistics);
            updateTableColumnStatistics(columnStatistics);
        }

        Set<String> removedColumnStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
        removedColumnStatistics.forEach(column -> deleteTableColumnStatistics(databaseName, tableName, column));
    }

    public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
    {
        try {
            ColumnStatisticsDesc columnStatisticsDesc = columnStatistics.getStatsDesc();
            UpdateTablePartitionColumnStatisticsRequest request = new UpdateTablePartitionColumnStatisticsRequest();
            String catalogIdLowercase = catalogId.toLowerCase(Locale.ENGLISH);
            String dbNameLowercase = columnStatisticsDesc.getDbName().toLowerCase(Locale.ENGLISH);
            String tableNameLowercase = columnStatisticsDesc.getTableName().toLowerCase(Locale.ENGLISH);
            request.setCatalogId(catalogIdLowercase);
            request.setDatabaseName(dbNameLowercase);
            request.setTableName(tableNameLowercase);
            com.aliyun.datalake20200710.models.ColumnStatistics catalogColumnStats = TrinoToCatalogConverter.toCatalogColumnStats(columnStatistics);
            List<com.aliyun.datalake20200710.models.ColumnStatistics> catalogColumnStatsList = new ArrayList<>();
            catalogColumnStatsList.add(catalogColumnStats);
            request.setColumnStatisticsList(catalogColumnStatsList);
            //update table paramters

            return dataLakeMetaStore.updateTableColumnStatistics(request);
            //update table paramters
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to update table column statistics: " + catalogId;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    public boolean deleteTableColumnStatistics(
            String dbName, String tableName, String colName)
    {
        dbName = dbName.toLowerCase(Locale.ENGLISH);
        tableName = tableName.toLowerCase(Locale.ENGLISH);
        List<String> cols = new ArrayList<>();
        try {
            if (colName != null) {
                colName = colName.toLowerCase(Locale.ENGLISH);
                cols.add(colName);
            }
            else {
                com.aliyun.datalake20200710.models.Table table = this.dataLakeMetaStore.getTable(catalogId.toLowerCase(Locale.ENGLISH), dbName, tableName);
                if (table != null && table.getSd() != null && table.getSd().getCols() != null && table.getSd().getCols().size() > 0) {
                    cols = table.getSd().getCols().stream().map(t -> t.getName()).collect(Collectors.toList());
                }
            }
            return this.dataLakeMetaStore.deleteTableColumnStatistics(catalogId.toLowerCase(Locale.ENGLISH), dbName, tableName, cols);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to delete table column statistics: " + catalogId + "." + dbName + "." + tableName + "." + colName;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    private Table getExistingTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        updates.forEach((partitionName, update) -> {
            Map<String, Optional<Partition>> partitionsMap = getPartitionsByNames(table, ImmutableList.copyOf(updates.keySet()));
            if (partitionsMap.size() != 1) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Metastore returned multiple partitions for name: " + partitionName);
            }

            List<Partition> partitions = new ArrayList<>();
            for (Map.Entry<String, Optional<Partition>> entry : partitionsMap.entrySet()) {
                if (!entry.getValue().isPresent()) {
                    throw new PartitionNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName()), Arrays.asList(partitionName));
                }
                else {
                    partitions.add(entry.getValue().get());
                }
            }

            PartitionStatistics currentStatistics = requireNonNull(
                    getPartitionStatistics(table, partitions).get(partitionName), "getPartitionStatistics() did not return statistics for partition");
            PartitionStatistics updatedStatistics = update.apply(currentStatistics);

            com.aliyun.datalake20200710.models.PartitionInput modifiedPartition = TrinoToCatalogConverter.toCatalogPartitionInput(partitions.get(0));
            HiveBasicStatistics basicStatistics = updatedStatistics.getBasicStatistics();
            modifiedPartition.setParameters(TrinoToCatalogConverter.updatetatsParamters(modifiedPartition.getParameters(), basicStatistics, true));
            alterPartitionWithoutStatistics(table.getDatabaseName(), table.getTableName(), modifiedPartition);

            Map<String, HiveType> columns = modifiedPartition.getSd().getCols().stream()
                    .collect(toImmutableMap(com.aliyun.datalake20200710.models.FieldSchema::getName, schema -> HiveType.valueOf(schema.getType())));
            setPartitionColumnStatistics(table, partitionName, columns, updatedStatistics.getColumnStatistics(), basicStatistics.getRowCount());

            Set<String> removedStatistics = difference(currentStatistics.getColumnStatistics().keySet(), updatedStatistics.getColumnStatistics().keySet());
            removedStatistics.forEach(column -> deletePartitionColumnStatistics(table.getDatabaseName(), table.getTableName(), partitionName, column));
        });
    }

    private void setPartitionColumnStatistics(
            Table table,
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
            ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(false, table.getDatabaseName(), table.getTableName());
            columnStatisticsDesc.setCatName(catalogId);
            columnStatisticsDesc.setLastAnalyzed(System.currentTimeMillis() / 1000);
            columnStatisticsDesc.setPartName(partitionName);
            ColumnStatistics columnStatisticsReal = new ColumnStatistics(columnStatisticsDesc, metastoreColumnStatistics);
            updatePartitionColumnStatistics(table, columnStatisticsReal);
        }
    }

    public boolean updatePartitionColumnStatistics(Table tbl, ColumnStatistics columnStatistics)
    {
        try {
            String partName = Utils.lowerCaseConvertPartName(columnStatistics.getStatsDesc().getPartName());

//            List<String> partVals = Utils.getPartValsFromName(tbl, partName);

            UpdateTablePartitionColumnStatisticsRequest request = new UpdateTablePartitionColumnStatisticsRequest();
            request.setCatalogId(catalogId.toLowerCase(Locale.ENGLISH));
            request.setDatabaseName(columnStatistics.getStatsDesc().getDbName().toLowerCase(Locale.ENGLISH));
            request.setTableName(columnStatistics.getStatsDesc().getTableName().toLowerCase(Locale.ENGLISH));
            columnStatistics.getStatsDesc().setPartName(partName);

            //Map<String, MTableColumnStatistics> oldStats = getPartitionColStats(table, colNames);

            com.aliyun.datalake20200710.models.ColumnStatistics catalogColumnStats = TrinoToCatalogConverter.toCatalogColumnStats(columnStatistics);
            List<com.aliyun.datalake20200710.models.ColumnStatistics> catalogColumnStatsList = new ArrayList<>();
            catalogColumnStatsList.add(catalogColumnStats);
            request.setColumnStatisticsList(catalogColumnStatsList);

            return this.dataLakeMetaStore.updatePartitionColumnStatistics(request);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to update table column partition statistics: " + catalogId;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName)
    {
        try {
            List<String> partNames = new ArrayList<>();
            partNames.add(Utils.lowerCaseConvertPartName(partName));
            List<String> colNames = new ArrayList<>();
            if (colName != null) {
                colNames.add(colName.toLowerCase(Locale.ENGLISH));
            }
            else {
                com.aliyun.datalake20200710.models.Table table = this.dataLakeMetaStore.getTable(catalogId.toLowerCase(Locale.ENGLISH), dbName, tableName);
                if (table != null && table.getSd() != null && table.getSd().getCols() != null && table.getSd().getCols().size() > 0) {
                    colNames = table.getSd().getCols().stream().map(t -> t.getName()).collect(Collectors.toList());
                }
            }
            return dataLakeMetaStore.deletePartitionColumnStatistics(catalogId.toLowerCase(Locale.ENGLISH), dbName.toLowerCase(Locale.ENGLISH), tableName.toLowerCase(Locale.ENGLISH), partNames, colNames);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to delete table column partition statistics: " + catalogId + "." + dbName + "." + tableName;
            throw new TrinoException(HIVE_METASTORE_ERROR, msg + e, e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        List<String> tables = new ArrayList<>();
        try {
            tables = this.dataLakeMetaStore.getTables(catalogId, databaseName, "*", this.pageSize, null);
            return tables;
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                // database does not exist
                return ImmutableList.of();
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get all tables:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        // TODO: Not supported now.
        throw new UnsupportedOperationException("getTablesWithParameter for DLF Catalog is not implemented");
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        List<String> views = new ArrayList<>();
//        String nextToken = EMPTY_TOKEN;
        try {
            List<com.aliyun.datalake20200710.models.Table> result = this.dataLakeMetaStore.getTableObjects(catalogId, databaseName, "*", this.pageSize, VIRTUAL_VIEW.name());
            result.stream().forEach(t -> views.add(t.tableName));
            return views;
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                // database does not exist
                return ImmutableList.of();
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "Unable to get all views:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        if (!database.getLocation().isPresent() && !defaultDir.isEmpty()) {
            String databaseLocation = new Path(defaultDir, database.getDatabaseName() + ".db").toString();
            database = Database.builder(database)
                    .setLocation(Optional.of(databaseLocation))
                    .build();
        }

        try {
            boolean madeDir = false;
            boolean isSuccess = false;
            Path dbPath = null;
            if (database.getLocation().isPresent()) {
                dbPath = new Path(database.getLocation().get());
                FileSystem fs = getFileSystem((path) -> hdfsEnvironment.getFileSystem(hdfsContext, path), dbPath);
                dbPath = new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), dbPath.toUri().getPath());
                madeDir = Utils.makeDir(hdfsContext, hdfsEnvironment, dbPath, this.enableFsOperation);
            }
            try {
                Database db = database;
                String description = db.getComment().isPresent() ? db.getComment().get() : null;
                String location = dbPath != null ? dbPath.toString() : null;
                // TODO:
                PrincipalPrivilegeSet privilegeSet = new PrincipalPrivilegeSet();
                try {
                    this.dataLakeMetaStore.createDatabase(catalogId, db.getDatabaseName(), description,
                            location, db.getParameters(), db.getOwnerName().get(), String.valueOf(db.getOwnerType().get()), privilegeSet);
                }
                catch (DataLakeAPIException e) {
                    throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
                }
                isSuccess = true;
            }
            finally {
                if (!isSuccess && madeDir) {
                    Utils.deleteDir(hdfsContext, hdfsEnvironment, dbPath, true, this.enableFsOperation);
                }
            }
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (Exception e) {
            String msg = "unable to create database:";
            throw new UnknownDBException(msg + e, e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        com.aliyun.datalake20200710.models.Database database = null;
        try {
            database = this.dataLakeMetaStore.getDatabase(catalogId, databaseName);
            this.dataLakeMetaStore.dropDatabase(catalogId, databaseName, false, false, true);
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                throw new SchemaNotFoundException(databaseName);
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to drop database:";
            throw new UnknownDBException(msg + e, e);
        }
        finally {
            if (database != null && database.locationUri != null && !database.locationUri.isEmpty() &&
                    dlfConfig.getDeleteDirWhenDropSchema()) {
                Utils.deleteDir(hdfsContext, hdfsEnvironment, new Path(database.locationUri), true, this.enableFsOperation);
            }
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        Database oldDatabase = getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
        Database newDatabase = Database.builder(oldDatabase)
                .setDatabaseName(newDatabaseName)
                .build();
        try {
            this.dataLakeMetaStore.alterDatabase(catalogId, databaseName, TrinoToCatalogConverter.toCatalogDatabase(newDatabase));
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to rename database:";
            throw new UnknownDBException(msg + e, e);
        }
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        Database oldDatabase = getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
        Database newDatabase = Database.builder(oldDatabase)
                .setOwnerName(Optional.ofNullable(principal.getName()))
                .setOwnerType(Optional.ofNullable(principal.getType()))
                .build();
        try {
            this.dataLakeMetaStore.alterDatabase(catalogId, databaseName, TrinoToCatalogConverter.toCatalogDatabase(newDatabase));
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to rename database:";
            throw new UnknownDBException(msg + e, e);
        }
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            com.aliyun.datalake20200710.models.TableInput catalogTable =
                    TrinoToCatalogConverter.toCatalogTableInput(table);
            try {
                this.dataLakeMetaStore.createTable(catalogId, catalogTable);
            }
            catch (DataLakeAPIException e) {
                throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
            }
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (Exception e) {
            String msg = "unable to create table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Table table = getExistingTable(databaseName, tableName);
        try {
            this.dataLakeMetaStore.dropTable(catalogId, databaseName, tableName, deleteData);
            String tableLocation = table.getStorage().getLocation();
            if (deleteData && Utils.isManagedTable(table) && !Strings.isNullOrEmpty(tableLocation)) {
                Utils.deleteDir(hdfsContext, hdfsEnvironment, new Path(tableLocation), true, this.enableFsOperation);
            }
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to drop table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable,
                             PrincipalPrivileges principalPrivileges)
    {
        try {
            this.dataLakeMetaStore.alterTable(catalogId, databaseName, tableName, TrinoToCatalogConverter.toCatalogTableInput(newTable));
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to replace table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    public void replaceTable(String databaseName, String tableName, com.aliyun.datalake20200710.models.TableInput newTable,
                             PrincipalPrivileges principalPrivileges)
    {
        try {
            this.dataLakeMetaStore.alterTable(catalogId, databaseName, tableName, newTable);
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to replace table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    /**
     * if a table was created in a user specified location using the DDL like
     * create table tbl ... location ...., it should be treated like an external table
     * in the table rename, its data location should not be changed. We can check
     * if the table directory was created directly under its database directory to tell
     * if it is such a table
     */
    @Override
    public void renameTable(String databaseName, String tableName,
                            String newDatabaseName, String newTableName)
    {
        if (!tableName.equalsIgnoreCase(newTableName) || !databaseName.equalsIgnoreCase(newTableName)) {
            Table oldTable = getExistingTable(databaseName, tableName);
            Table newTable = Table.builder(oldTable)
                    .setDatabaseName(newDatabaseName)
                    .setTableName(newTableName)
                    .build();

            if (isView(oldTable) || isNewLocationSpecified(oldTable, newTable) || Utils.isExternalTable(oldTable)) {
                doRenameTableInMs(catalogId, oldTable, newTable);
                return;
            }

            Database database = getDatabase(catalogId, databaseName).get();

            Path srcPath = new Path(oldTable.getStorage().getLocation());
            String oldTableRelativePath = (new Path(database.getLocation().get()).toUri()).relativize(srcPath.toUri()).toString();

            boolean tableInSpecifiedLoc = !oldTableRelativePath.equalsIgnoreCase(tableName)
                    && !oldTableRelativePath.equalsIgnoreCase(tableName + Path.SEPARATOR);

            if (tableInSpecifiedLoc) {
                doRenameTableInMs(catalogId, oldTable, newTable);
                return;
            }

            Database newDatabase = database;
            if (!databaseName.equalsIgnoreCase(newTable.getDatabaseName())) {
                newDatabase = getDatabase(catalogId, newTable.getDatabaseName()).get();
            }

            FileSystem srcFs = getFileSystem((path) -> hdfsEnvironment.getFileSystem(hdfsContext, path), srcPath);
            FileSystem destFs = getFileSystem((path) -> hdfsEnvironment.getFileSystem(hdfsContext, path), getDatabasePath(newDatabase));

            if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
                throw new InvalidOperationException("table new location is" +
                        " on a different file system than the old location "
                        + srcPath + ". This operation is not supported");
            }

            Path databasePath = Utils.constructRenamedPath(getDatabasePath(newDatabase), srcPath);
            Path destPath = new Path(databasePath, newTable.getTableName().toLowerCase(Locale.ENGLISH));
            destFs = getFileSystem((path) -> hdfsEnvironment.getFileSystem(hdfsContext, path), destPath);

            if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
                throw new InvalidOperationException("table new location " + destPath
                        + " is on a different file system than the old location "
                        + srcPath + ". This operation is not supported");
            }

            boolean dataWasMoved = false;
            try {
                if (enableFsOperation && destFs.exists(destPath)) {
                    throw new InvalidOperationException("New location for this table "
                            + newTable.getDatabaseName() + "." + newTable.getTableName()
                            + " already exists : " + destPath);
                }
                // check that src exists and also checks permissions necessary, rename src to dest
                if (srcFs.exists(srcPath) && Utils.renameFs(srcFs, srcPath, destPath, enableFsOperation)) {
                    dataWasMoved = true;
                }
            }
            catch (IOException e) {
                throw new InvalidOperationException("Alter Table operation for " + databaseName + "." + tableName +
                        " failed to move data due to: '" + e.getMessage() + "' See log file for details.");
            }

            try {
                doRenameTableInMs(catalogId, oldTable, newTable);
            }
            catch (Exception e) {
                if (dataWasMoved) {
                    try {
                        if (enableFsOperation && destFs.exists(destPath)) {
                            if (!Utils.renameFs(destFs, destPath, srcPath, enableFsOperation)) {
                                logger.error("Failed to restore data from " + destPath + " to " + srcPath
                                        + " in alter table failure. Manual restore is needed.");
                            }
                        }
                    }
                    catch (IOException e1) {
                        logger.error("Failed to restore data from " + destPath + " to " + srcPath
                                + " in alter table failure. Manual restore is needed.");
                    }
                }
                String msg = String.format("Unable to rename table from %s to %s due to: ",
                        tableName, newTable.getTableName());

                throw new InvalidOperationException(msg + e.getMessage());
            }
        }
    }

    private void doRenameTableInMs(String catalogId, Table oldTable, Table newTable)
    {
        try {
            boolean isAsync = !oldTable.getDatabaseName().equalsIgnoreCase(newTable.getDatabaseName()) || (oldTable.getPartitionColumns() != null && oldTable.getPartitionColumns().size() > 0);
            this.dataLakeMetaStore.doRenameTableInMs(catalogId, oldTable.getDatabaseName(), oldTable.getTableName(), TrinoToCatalogConverter.toCatalogTableInput(newTable), isAsync);
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                throw new TableNotFoundException(new SchemaTableName(oldTable.getDatabaseName(), oldTable.getTableName()));
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to rename table:";
            throw new UnknownTableException(msg + e, e);
        }
    }

    private boolean isNewLocationSpecified(Table oldTable, Table newTable)
    {
        return StringUtils.isNotEmpty(newTable.getStorage().getLocation()) &&
                !newTable.getStorage().getLocation().equals(oldTable.getStorage().getLocation());
    }

    private boolean isView(Table table)
    {
        return TableType.VIRTUAL_VIEW.toString().equals(table.getTableType());
    }

    public Path getDatabasePath(Database db)
    {
        return "default".equalsIgnoreCase(db.getDatabaseName()) ? new Path(defaultDir) : new Path(db.getLocation().get());
    }

    public FileSystem getFileSystem(ThrowingFunction<Path, FileSystem, IOException> consumer, Path path)
    {
        try {
            return consumer.apply(path);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);

        Map<String, String> parameters = oldTable.getParameters().entrySet().stream()
                .filter((entry) -> !"comment".equals(entry.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        comment.ifPresent((value) -> parameters.put("comment", value));

        Table newTable = Table.builder(oldTable)
                .setParameters(parameters)
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        if (principal.getType() != PrincipalType.USER) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }
        else {
            Table oldTable = getExistingTable(databaseName, tableName);

            Table newTable = Table.builder(oldTable)
                    .setOwner(Optional.ofNullable(principal.getName()))
                    .build();
            replaceTable(databaseName, tableName, newTable, null);
        }
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column column : oldTable.getDataColumns()) {
            if (column.getName().equals(columnName) && comment.isPresent()) {
                newDataColumns.add(new Column(columnName, column.getType(), comment));
            }
            else {
                newDataColumns.add(column);
            }
        }

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName,
                          HiveType columnType, String columnComment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        Table newTable = Table.builder(oldTable)
                .addDataColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName,
                             String newColumnName)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        if (oldTable.getPartitionColumns().stream().anyMatch(c -> c.getName().equals(oldColumnName))) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column column : oldTable.getDataColumns()) {
            if (column.getName().equals(oldColumnName)) {
                newDataColumns.add(new Column(newColumnName, column.getType(), column.getComment()));
            }
            else {
                newDataColumns.add(column);
            }
        }

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, databaseName, tableName, columnName);
        Table oldTable = getExistingTable(databaseName, tableName);

        if (!oldTable.getColumn(columnName).isPresent()) {
            SchemaTableName name = new SchemaTableName(databaseName, tableName);
            throw new ColumnNotFoundException(name, columnName);
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        oldTable.getDataColumns().stream()
                .filter(fieldSchema -> !fieldSchema.getName().equals(columnName))
                .forEach(newDataColumns::add);

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        try {
            com.aliyun.datalake20200710.models.Partition partition = this.dataLakeMetaStore.getPartition(catalogId, table.getDatabaseName(), table.getTableName(), partitionValues);
            return Optional.of(CatalogToTrinoConverter.toTrinoPartition(partition));
        }
        catch (DataLakeAPIException e) {
            if ("NoSuchObject".equals(e.getResult().code)) {
                return Optional.empty();
            }
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get partition:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        Optional<List<String>> parts = partitionKeyFilterToStringList(columnNames, partitionKeysFilter, dlfConfig.getAssumeCanonicalPartitionKeys().orElse(true));
        checkArgument(!columnNames.isEmpty() || partitionKeysFilter.isAll(), "must pass in all columnNames or the filter must be all");
        if (parts.isEmpty()) {
            return Optional.of(ImmutableList.of());
        }

        Table table = getExistingTable(databaseName, tableName);
        String expression = ExpressionUtils.buildDlfExpression(table.getPartitionColumns(), parts.get());
        List<Partition> partitions = getPartitions(table, expression);
        return Optional.of(buildPartitionNames(table.getPartitionColumns(), partitions));
    }

    private List<Partition> getPartitions(Table table, String expression)
    {
        try {
            List<com.aliyun.datalake20200710.models.Partition> partitions = this.dataLakeMetaStore.listPartitionsByFilter(catalogId, table.getDatabaseName(), table.getTableName(), expression, this.pageSize);
            return partitions.stream().map(p -> CatalogToTrinoConverter.toTrinoPartition(p))
                    .collect(Collectors.toList());
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get partitions:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table,
                                                                 List<String> partitionNames)
    {
        List<List<String>> partitionValues = partitionNames.stream()
                .map(HiveUtil::toPartitionValues)
                .collect(Collectors.toList());
        try {
            List<Partition> partitions = this.dataLakeMetaStore.getPartitionsByValues(catalogId, table.getDatabaseName(), table.getTableName(), partitionValues)
                    .stream()
                    .map(p -> CatalogToTrinoConverter.toTrinoPartition(p))
                    .collect(Collectors.toList());

            Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                    .collect(Collectors.toMap(identity(), HiveUtil::toPartitionValues));
            Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                    .collect(Collectors.toMap(Partition::getValues, identity()));

            ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
            for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
                Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
                resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
            }
            return resultBuilder.buildOrThrow();
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to get partitions by names:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName,
                              List<PartitionWithStatistics> partitions)
    {
        addPartitionsWithoutStatistics(databaseName, tableName, partitions);
        for (PartitionWithStatistics partitionWithStatistics : partitions) {
            storePartitionColumnStatistics(databaseName, tableName, partitionWithStatistics.getPartitionName(), partitionWithStatistics);
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
        Table table = getTable(databaseName, tableName).orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        setPartitionColumnStatistics(table, partitionName, columnTypes, columnStatistics, statistics.getBasicStatistics().getRowCount());
    }

    public void addPartitionsWithoutStatistics(String databaseName, String tableName,
                                               List<PartitionWithStatistics> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }

        try {
            List<com.aliyun.datalake20200710.models.PartitionInput> catalogPartitions = partitions.stream()
                    .map(p -> TrinoToCatalogConverter.toCatalogPartitionInput(p.getPartition(), p.getStatistics().getBasicStatistics(), true))
                    .collect(Collectors.toList());
            this.dataLakeMetaStore.addPartitions(catalogId, databaseName, tableName, catalogPartitions, true, false);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to add partition:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts,
                              boolean deleteData)
    {
        Table table = getExistingTable(databaseName, tableName);
        Partition partition = getPartition(table, parts)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts));
        try {
            this.dataLakeMetaStore.doDropPartition(catalogId, databaseName, tableName, parts, true);
            String partLocation = partition.getStorage().getLocation();
            if (deleteData && Utils.isManagedTable(table) && !Strings.isNullOrEmpty(partLocation)) {
                Utils.deleteDir(hdfsContext, hdfsEnvironment, new Path(partLocation), true, this.enableFsOperation);
            }
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to drop partition:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName,
                               PartitionWithStatistics partition)
    {
        alterPartitionWithoutStatistics(databaseName, tableName, partition);
        storePartitionColumnStatistics(databaseName, tableName, partition.getPartitionName(), partition);
        dropExtraColumnStatisticsAfterAlterPartition(databaseName, tableName, partition);
    }

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
        List<ColumnStatisticsObj> statisticsToBeRemoved = getPartitionColumnStatisticsObj(
                catalogId,
                databaseName,
                tableName,
                ImmutableList.of(partitionName),
                ImmutableList.copyOf(columnsWithMissingStatistics))
                .getOrDefault(partitionName, ImmutableList.of());

        for (ColumnStatisticsObj statistics : statisticsToBeRemoved) {
            deletePartitionColumnStatistics(databaseName, tableName, partitionName, statistics.getColName());
        }
    }

    public void alterPartitionWithoutStatistics(String databaseName, String tableName,
                                                PartitionWithStatistics partition)
    {
        try {
            List<com.aliyun.datalake20200710.models.PartitionInput> partitions = new ArrayList<>();
            partitions.add(TrinoToCatalogConverter.toCatalogPartitionInput(partition.getPartition(), partition.getStatistics().getBasicStatistics(), true));

            this.dataLakeMetaStore.alterPartitions(catalogId, databaseName, tableName, partitions);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to alter partition:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    public void alterPartitionWithoutStatistics(String databaseName, String tableName,
                                                com.aliyun.datalake20200710.models.PartitionInput partition)
    {
        try {
            List<com.aliyun.datalake20200710.models.PartitionInput> partitions = new ArrayList<>();
            partitions.add(partition);
            this.dataLakeMetaStore.alterPartitions(catalogId, databaseName, tableName, partitions);
        }
        catch (DataLakeAPIException e) {
            throw CatalogToTrinoConverter.toTrinoException(e.getResult(), e.getAction(), e);
        }
        catch (Exception e) {
            String msg = "unable to alter partition:";
            throw new UnknownPartitionException(msg + e, e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "createRole is not supported by DLF");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropRole is not supported by DLF");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new TrinoException(NOT_SUPPORTED, "listRoles is not supported by DLF");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantRoles is not supported by DLF");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption,
                            HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeRoles is not supported by DLF");
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "listGrantedPrincipals is not supported by DLF");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        // not supported now
        throw new TrinoException(NOT_SUPPORTED, "listRoleGrants is not supported by DLF");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        // not supported now
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by DLF");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        // not supported now
        throw new TrinoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by DLF");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner,
                                                      Optional<HivePrincipal> principal)
    {
        // not supported now
        return ImmutableSet.of();
    }

    private abstract static class Batchable<I, R>
    {
        public abstract List<R> run(List<I> input);
    }

    private static class PartitionToTrinoVisitor
            implements IDataLakeMetaStore.PartitionVisitor<List<Partition>,
            com.aliyun.datalake20200710.models.Partition>
    {
        List<Column> columns;
        private List<Partition> result;

        PartitionToTrinoVisitor(List<Column> columns)
        {
            this.columns = columns;
            result = new ArrayList<>();
        }

        @Override
        public void accept(List<com.aliyun.datalake20200710.models.Partition> partitions)
        {
            partitions.stream().forEach(p -> result.add(CatalogToTrinoConverter.toTrinoPartition(p)));
        }

        @Override
        public List<Partition> getResult()
        {
            return result;
        }
    }
}
