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
package io.trino.plugin.hive.metastore.dlf.converters;

import com.aliyun.datalake20200710.models.FieldSchema;
import com.aliyun.datalake20200710.models.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;

public class TrinoToCatalogConverter
{
    private TrinoToCatalogConverter() {}

    public static com.aliyun.datalake20200710.models.Database toCatalogDatabase(Database trinoDatabase)
    {
        com.aliyun.datalake20200710.models.Database database = new com.aliyun.datalake20200710.models.Database();
        database.name = trinoDatabase.getDatabaseName();
        if (trinoDatabase.getComment().isPresent()) {
            database.description = trinoDatabase.getComment().get();
        }
        database.ownerName = trinoDatabase.getOwnerName().get();
        database.ownerType = String.valueOf(trinoDatabase.getOwnerType().get());
        if (trinoDatabase.getLocation().isPresent()) {
            database.locationUri = trinoDatabase.getLocation().get();
        }
        database.parameters = trinoDatabase.getParameters();
        return database;
    }

    public static com.aliyun.datalake20200710.models.Table toCatalogTable(Table trinoTable)
    {
        com.aliyun.datalake20200710.models.Table table = new com.aliyun.datalake20200710.models.Table();

        table.databaseName = trinoTable.getDatabaseName();
        table.tableName = trinoTable.getTableName();
        table.tableType = trinoTable.getTableType();
        table.partitionKeys = trinoTable.getPartitionColumns().stream()
                .map(TrinoToCatalogConverter::columnToFieldSchema)
                .collect(Collectors.toList());
        table.owner = String.valueOf(trinoTable.getOwner());
        // ownerType is not supported in trino table.
        // table.ownerType = ;
        table.parameters = trinoTable.getParameters();
        // privileges is not supported in trino table.
        // table.privileges =;

        table.sd = toCatalogStorageDescriptor(trinoTable.getStorage(),
                trinoTable.getDataColumns());

        if (trinoTable.getViewOriginalText().isPresent()) {
            table.viewOriginalText = trinoTable.getViewOriginalText().get();
        }
        if (trinoTable.getViewExpandedText().isPresent()) {
            table.viewExpandedText = trinoTable.getViewExpandedText().get();
        }

        return table;
    }

    public static com.aliyun.datalake20200710.models.TableInput toCatalogTableInput(Table trinoTable)
    {
        com.aliyun.datalake20200710.models.TableInput table = new com.aliyun.datalake20200710.models.TableInput();

        table.databaseName = trinoTable.getDatabaseName();
        table.tableName = trinoTable.getTableName();
        table.tableType = trinoTable.getTableType();
        table.partitionKeys = trinoTable.getPartitionColumns().stream()
                .map(TrinoToCatalogConverter::columnToFieldSchema)
                .collect(Collectors.toList());
        table.owner = String.valueOf(trinoTable.getOwner());
        // ownerType is not supported in trino table.
        // table.ownerType = ;
        table.parameters = trinoTable.getParameters();
        // privileges is not supported in trino table.
        // table.privileges =;

        table.sd = toCatalogStorageDescriptor(trinoTable.getStorage(),
                trinoTable.getDataColumns());

        if (trinoTable.getViewOriginalText().isPresent()) {
            table.viewOriginalText = trinoTable.getViewOriginalText().get();
        }
        if (trinoTable.getViewExpandedText().isPresent()) {
            table.viewExpandedText = trinoTable.getViewExpandedText().get();
        }

        return table;
    }

    private static FieldSchema columnToFieldSchema(Column col)
    {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.name = col.getName();
        fieldSchema.type = col.getType().getTypeInfo().getTypeName();
        fieldSchema.comment = col.getComment().isPresent() ? col.getComment().get() : null;
        return fieldSchema;
    }

    public static com.aliyun.datalake20200710.models.StorageDescriptor toCatalogStorageDescriptor(
            Storage trinoSd, List<Column> dataColumns)
    {
        com.aliyun.datalake20200710.models.StorageDescriptor sd = new com.aliyun.datalake20200710.models.StorageDescriptor();
        List<Column> allColumns = new ArrayList<>(dataColumns);

        // serDe
        com.aliyun.datalake20200710.models.SerDeInfo serDeInfo = new com.aliyun.datalake20200710.models.SerDeInfo();
        StorageFormat storageFormat = trinoSd.getStorageFormat();
        serDeInfo.name = storageFormat.getSerDeNullable();
        serDeInfo.serializationLib = storageFormat.getSerDeNullable();
        serDeInfo.parameters = trinoSd.getSerdeParameters();
        sd.serDeInfo = serDeInfo;
        sd.location = trinoSd.getLocation();

        // inputformat and outputformat
        sd.inputFormat = storageFormat.getInputFormatNullable();
        sd.outputFormat = storageFormat.getOutputFormatNullable();

        // bucket
        if (trinoSd.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = trinoSd.getBucketProperty().get();
            sd.numBuckets = bucketProperty.getBucketCount();
            sd.bucketCols = bucketProperty.getBucketedBy();
            sd.sortCols = bucketProperty.getSortedBy().stream().map(
                    c -> {
                        Order order = new Order();
                        order.col = c.getColumnName();
                        order.order = c.getOrder().getHiveOrder();
                        return order;
                    }
            ).collect(Collectors.toList());
        }

        // skewedInfo: skewedInfo is not supported in trino table.
        // com.aliyun.datalake20200710.models.SkewedInfo skewedInfo = new com.aliyun.datalake20200710.models.SkewedInfo();
        // sd.skewedInfo = skewedInfo;

        // others
        sd.parameters = trinoSd.getSerdeParameters();
        sd.cols = allColumns.stream()
                .map(TrinoToCatalogConverter::columnToFieldSchema)
                .collect(Collectors.toList());

        return sd;
    }

    public static com.aliyun.datalake20200710.models.Partition toCatalogPartition(Partition trinoPartition)
    {
        com.aliyun.datalake20200710.models.Partition partition = new com.aliyun.datalake20200710.models.Partition();
        partition.databaseName = trinoPartition.getDatabaseName();
        partition.tableName = trinoPartition.getTableName();
        partition.values = trinoPartition.getValues();
        partition.createTime = toIntExact(System.currentTimeMillis() / 1000L);
        partition.parameters = trinoPartition.getParameters();
        partition.sd = toCatalogStorageDescriptor(trinoPartition.getStorage(), trinoPartition.getColumns());

        return partition;
    }

    public static com.aliyun.datalake20200710.models.PartitionInput toCatalogPartitionInput(Partition trinoPartition)
    {
        com.aliyun.datalake20200710.models.PartitionInput partition = new com.aliyun.datalake20200710.models.PartitionInput();
        partition.databaseName = trinoPartition.getDatabaseName();
        partition.tableName = trinoPartition.getTableName();
        partition.values = trinoPartition.getValues();
        partition.parameters = trinoPartition.getParameters();
        partition.sd = toCatalogStorageDescriptor(trinoPartition.getStorage(), trinoPartition.getColumns());
        return partition;
    }

    public static com.aliyun.datalake20200710.models.PartitionInput toCatalogPartitionInput(Partition trinoPartition, HiveBasicStatistics hiveBasicStatistics, Boolean isSetBasicStats)
    {
        com.aliyun.datalake20200710.models.PartitionInput partition = new com.aliyun.datalake20200710.models.PartitionInput();
        partition.databaseName = trinoPartition.getDatabaseName();
        partition.tableName = trinoPartition.getTableName();
        partition.values = trinoPartition.getValues();
        partition.parameters = updatetatsParamters(trinoPartition.getParameters(), hiveBasicStatistics, isSetBasicStats);
        partition.sd = toCatalogStorageDescriptor(trinoPartition.getStorage(), trinoPartition.getColumns());
        return partition;
    }

    public static Map<String, String> updatetatsParamters(Map<String, String> parameters, HiveBasicStatistics hiveBasicStatistics, Boolean isSetBasicStats)
    {
        Map<String, String> initResult = ThriftMetastoreUtil.updateStatisticsParameters(parameters, hiveBasicStatistics);
        Map<String, String> result = new HashMap<>();
        initResult.forEach((key, value) -> {
            result.put(key, value);
        });
        if (isSetBasicStats && (result.get("COLUMN_STATS_ACCURATE") == null || !result.get("COLUMN_STATS_ACCURATE").contains("BASIC_STATS"))) {
            StatsSetupConst.setBasicStatsState(result, StatsSetupConst.TRUE);
        }
        return result;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatistics toCatalogColumnStats(ColumnStatistics columnStatistics) throws IOException
    {
        com.aliyun.datalake20200710.models.ColumnStatistics catalogStats = new com.aliyun.datalake20200710.models.ColumnStatistics();
        catalogStats.setColumnStatisticsDesc(toCatalogColumnStatsDesc(columnStatistics.getStatsDesc()));
        catalogStats.setColumnStatisticsObjList(toCatalogColumnStatsObjs(columnStatistics.getStatsObj()));
        return catalogStats;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsObj toCatalogColumnStatsObj(ColumnStatisticsObj columnStatisticsObj) throws IOException
    {
        com.aliyun.datalake20200710.models.ColumnStatisticsObj catalogStatsObj = new com.aliyun.datalake20200710.models.ColumnStatisticsObj();
        if (columnStatisticsObj.getColName() != null) {
            catalogStatsObj.setColumnName(columnStatisticsObj.getColName().toLowerCase(Locale.ENGLISH));
        }
        if (columnStatisticsObj.getColType() != null) {
            catalogStatsObj.setColumnType(columnStatisticsObj.getColType().toLowerCase(Locale.ENGLISH));
        }
        catalogStatsObj.setColumnStatisticsData(toCatalogColumnStatsData(columnStatisticsObj.getStatsData()));
        return catalogStatsObj;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData toCatalogColumnStatsData(ColumnStatisticsData columnStatisticsData) throws IOException
    {
        com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData catalogColumnStatisticsData = new com.aliyun.datalake20200710.models.ColumnStatisticsObj.ColumnStatisticsObjColumnStatisticsData();
        catalogColumnStatisticsData.setStatisticsData(toCatalogColumnStatsDataCore(columnStatisticsData));
        catalogColumnStatisticsData.setStatisticsType(String.valueOf(columnStatisticsData.getSetField().getThriftFieldId()));
        return catalogColumnStatisticsData;
    }

    public static String toCatalogColumnStatsDataCore(ColumnStatisticsData columnStatisticsData) throws IOException
    {
        String statsDataCore = "";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        switch (columnStatisticsData.getSetField()) {
            case BOOLEAN_STATS:
                BooleanColumnStatsData boolStats = columnStatisticsData.getBooleanStats();
                root.put("numNulls", boolStats.isSetNumNulls() ? boolStats.getNumNulls() : null);
                root.put("numTrues", boolStats.isSetNumTrues() ? boolStats.getNumTrues() : null);
                root.put("numFalses", boolStats.isSetNumFalses() ? boolStats.getNumFalses() : null);
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case LONG_STATS:
                LongColumnStatsData longStats = columnStatisticsData.getLongStats();
                root.put("numNulls", longStats.isSetNumNulls() ? longStats.getNumNulls() : null);
                root.put("numDVs", longStats.isSetNumDVs() ? longStats.getNumDVs() : null);
                root.put("lowValue", longStats.isSetLowValue() ? longStats.getLowValue() : null);
                root.put("highValue", longStats.isSetHighValue() ? longStats.getHighValue() : null);
                root.put("bitVectors", getLongBitVector(longStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DOUBLE_STATS:
                DoubleColumnStatsData doubleStats = columnStatisticsData.getDoubleStats();
                root.put("numNulls", doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null);
                root.put("numDVs", doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null);
                root.put("lowValue", doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null);
                root.put("highValue", doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null);
                root.put("bitVectors", getDoubleBitVector(doubleStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case STRING_STATS:
                StringColumnStatsData stringStats = columnStatisticsData.getStringStats();
                root.put("numNulls", stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null);
                root.put("numDVs", stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null);
                root.put("maxColLen", stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null);
                root.put("avgColLen", stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null);
                root.put("bitVectors", getStringBitVector(stringStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case BINARY_STATS:
                BinaryColumnStatsData binaryStats = columnStatisticsData.getBinaryStats();
                root.put("numNulls", binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null);
                root.put("maxColLen", binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null);
                root.put("avgColLen", binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null);
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DECIMAL_STATS:
                DecimalColumnStatsData decimalStats = columnStatisticsData.getDecimalStats();
                root.put("numNulls", decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null);
                root.put("numDVs", decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null);
                root.put("lowValue", decimalStats.isSetLowValue() ? createJdoDecimalString(decimalStats.getLowValue()) : null);
                root.put("highValue", decimalStats.isSetHighValue() ? createJdoDecimalString(decimalStats.getHighValue()) : null);
                root.put("bitVectors", getDecimalBitVector(decimalStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            case DATE_STATS:
                DateColumnStatsData dateStats = columnStatisticsData.getDateStats();
                root.put("numNulls", dateStats.isSetNumNulls() ? dateStats.getNumNulls() : null);
                root.put("numDVs", dateStats.isSetNumDVs() ? dateStats.getNumDVs() : null);
                root.put("lowValue", dateStats.isSetLowValue() ? dateStats.getLowValue().getDaysSinceEpoch() : null);
                root.put("highValue", dateStats.isSetHighValue() ? dateStats.getHighValue().getDaysSinceEpoch() : null);
                root.put("bitVectors", getDateBitVector(dateStats));
                statsDataCore = mapper.writeValueAsString(root);
                break;
            default:
                break;
        }
        return statsDataCore;
    }

    public static List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> toCatalogColumnStatsObjs(List<ColumnStatisticsObj> columnStatisticsObjs) throws IOException
    {
        List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> catalogObjs = new ArrayList<>();
        for (ColumnStatisticsObj obj : columnStatisticsObjs) {
            catalogObjs.add(toCatalogColumnStatsObj(obj));
        }
        return catalogObjs;
    }

    public static com.aliyun.datalake20200710.models.ColumnStatisticsDesc toCatalogColumnStatsDesc(ColumnStatisticsDesc columnStatisticsDesc)
    {
        com.aliyun.datalake20200710.models.ColumnStatisticsDesc catalogStatsDesc = new com.aliyun.datalake20200710.models.ColumnStatisticsDesc();
        if (columnStatisticsDesc.getLastAnalyzed() == 0) {
            long time = System.currentTimeMillis() / 1000;
            catalogStatsDesc.setLastAnalyzedTime(time);
        }
        else {
            catalogStatsDesc.setLastAnalyzedTime(columnStatisticsDesc.getLastAnalyzed());
        }
        if (columnStatisticsDesc.getPartName() != null) {
            catalogStatsDesc.setPartitionName(columnStatisticsDesc.getPartName());
        }
        return catalogStatsDesc;
    }

    public static String createJdoDecimalString(Decimal d)
    {
        return new BigDecimal(new BigInteger(d.getUnscaled()), d.getScale()).toString();
    }

    public static byte[] getLongBitVector(LongColumnStatsData longColumnStatsData)
    {
        return longColumnStatsData.isSetBitVectors() ? longColumnStatsData.getBitVectors() : null;
    }

    public static byte[] getDoubleBitVector(DoubleColumnStatsData doubleColumnStatsData)
    {
        return doubleColumnStatsData.isSetBitVectors() ? doubleColumnStatsData.getBitVectors() : null;
    }

    public static byte[] getDecimalBitVector(DecimalColumnStatsData decimalColumnStatsData)
    {
        return decimalColumnStatsData.isSetBitVectors() ? decimalColumnStatsData.getBitVectors() : null;
    }

    public static byte[] getDateBitVector(DateColumnStatsData dateColumnStatsData)
    {
        return dateColumnStatsData.isSetBitVectors() ? dateColumnStatsData.getBitVectors() : null;
    }

    public static byte[] getStringBitVector(StringColumnStatsData stringColumnStatsData)
    {
        return stringColumnStatsData.isSetBitVectors() ? stringColumnStatsData.getBitVectors() : null;
    }
}
