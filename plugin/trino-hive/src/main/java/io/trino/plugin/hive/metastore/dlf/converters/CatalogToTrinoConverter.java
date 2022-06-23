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

import com.aliyun.datalake.metastore.common.Action;
import com.aliyun.datalake.metastore.common.entity.ResultModel;
import com.aliyun.datalake.metastore.common.util.DataLakeUtil;
import com.aliyun.datalake20200710.models.FieldSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.SortingColumn.Order;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.dlf.exceptions.AlreadyExistsException;
import io.trino.plugin.hive.metastore.dlf.exceptions.InvalidOperationException;
import io.trino.plugin.hive.metastore.dlf.exceptions.NoSuchObjectException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownDBException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownPartitionException;
import io.trino.plugin.hive.metastore.dlf.exceptions.UnknownTableException;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.security.PrincipalType;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public class CatalogToTrinoConverter
{
    private CatalogToTrinoConverter() {}

    private static final ImmutableMap<String, MetaStoreException> EXCEPTION_MAP
            = ImmutableMap.<String, MetaStoreException>builder()
            .put("UnknownTable", UnknownTableException::new)
            .put("UnknownDB", UnknownDBException::new)
            .put("AlreadyExists", AlreadyExistsException::new)
            .put("UnknownPartition", UnknownPartitionException::new)
            .put("NoSuchObject", NoSuchObjectException::new)
            .put("InvalidOperation", InvalidOperationException::new)
            .buildOrThrow();

    public static TrinoException toTrinoException(ResultModel<?> result, Action action, Exception e)
    {
        if (result == null) {
            return new TrinoException(HIVE_METASTORE_ERROR, "Empty result returned from catalog.", e);
        }
        String code = result.code;
        String msg = result.message;
        String requestId = result.requestId;
        String message = String.join(" ", "Action:", action.name(), "Message:", msg, "RequestId:", requestId);
        if (EXCEPTION_MAP.containsKey(code)) {
            return EXCEPTION_MAP.get(code).get(message, e);
        }
        return new TrinoException(HIVE_METASTORE_ERROR, message, e);
    }

    public static Database toTrinoDatabase(com.aliyun.datalake20200710.models.Database catalogDatabase)
    {
        return Database.builder()
                .setDatabaseName(catalogDatabase.name)
                .setLocation(Optional.ofNullable(catalogDatabase.locationUri))
                .setOwnerName(Optional.of(DataLakeUtil.wrapperNullString(catalogDatabase.ownerName)))
                .setOwnerType(Optional.of(PrincipalType.valueOf(catalogDatabase.ownerType)))
                .setParameters(catalogDatabase.parameters)
                .build();
    }

    public static Table toTrinoTable(com.aliyun.datalake20200710.models.Table catalogTable)
    {
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(catalogTable.databaseName)
                .setTableName(catalogTable.tableName)
                // The owner api of DLF cannot be changed, which would influence other components using DLF, so do a tricky judgement here.
                .setOwner(Optional.of(DataLakeUtil.wrapperNullString(catalogTable.owner.startsWith("Optional") ?
                        catalogTable.owner.substring(9, catalogTable.owner.length() - 1) : catalogTable.owner)))
                .setTableType(catalogTable.tableType)
                .setDataColumns(catalogTable.sd.cols.stream().map(CatalogToTrinoConverter::convertColumn)
                        .collect(Collectors.toList()))
                .setParameters(catalogTable.parameters)
                .setViewOriginalText(Optional.ofNullable(catalogTable.viewOriginalText))
                .setViewExpandedText(Optional.ofNullable(catalogTable.viewExpandedText));
        if (catalogTable.partitionKeys != null && !catalogTable.partitionKeys.isEmpty()) {
            tableBuilder.setPartitionColumns(catalogTable.partitionKeys.stream()
                    .map(CatalogToTrinoConverter::convertColumn).collect(Collectors.toList()));
        }
        else {
            tableBuilder.setPartitionColumns(new ArrayList<>());
        }
        setStorageBuilder(catalogTable.sd, tableBuilder.getStorageBuilder(), catalogTable.parameters);
        return tableBuilder.build();
    }

    private static void setStorageBuilder(com.aliyun.datalake20200710.models.StorageDescriptor sd,
                                          Storage.Builder storageBuilder, Map<String, String> tableParameters)
    {
        SdConsumer consumer = new SdConsumer(sd, tableParameters);
        consumer.accept(storageBuilder);
    }

    private static Column convertColumn(FieldSchema col)
    {
        return new Column(col.name,
                HiveType.valueOf(col.type.toLowerCase(Locale.ENGLISH)),
                Optional.ofNullable(col.comment));
    }

    public static Partition toTrinoPartition(
            com.aliyun.datalake20200710.models.Partition catalogPartition)
    {
        return Partition.builder()
                .withStorage(new SdConsumer(catalogPartition.getSd(), catalogPartition.parameters))
                .setDatabaseName(catalogPartition.databaseName)
                .setTableName(catalogPartition.tableName)
                .setParameters(catalogPartition.parameters)
                .setColumns(catalogPartition.getSd() == null ? new ArrayList<Column>() : catalogPartition.getSd().getCols().stream().map(CatalogToTrinoConverter::convertColumn).collect(Collectors.toList()))
                .setValues(catalogPartition.values)
                .build();
    }

    private static boolean isNullOrEmpty(List<?> list)
    {
        return list == null || list.isEmpty();
    }

    public static ColumnStatisticsObj toHiveColumnStatsObj(com.aliyun.datalake20200710.models.ColumnStatisticsObj columnStatisticsObj, boolean enableBitVector) throws IOException
    {
        ColumnStatisticsObj hiveColumnStats = new ColumnStatisticsObj();
        hiveColumnStats.setColName(columnStatisticsObj.getColumnName());
        hiveColumnStats.setColType(columnStatisticsObj.getColumnType());
        String type = columnStatisticsObj.getColumnStatisticsData().getStatisticsType();
        ColumnStatisticsData hiveColumnStatsData = new ColumnStatisticsData();
        ColumnStatisticsData._Fields field = hiveColumnStatsData.fieldForId(Integer.parseInt(type));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(columnStatisticsObj.getColumnStatisticsData().getStatisticsData());
        switch (field) {
            case BOOLEAN_STATS:
                hiveColumnStatsData.setBooleanStats(getBooleanColumnStatsData(root.get("numFalses").asLong(), root.get("numTrues").asLong(), root.get("numNulls").asLong()));
                break;
            case LONG_STATS:
                hiveColumnStatsData.setLongStats(getLongColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("lowValue").asLong(), root.get("highValue").asLong(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case DOUBLE_STATS:
                hiveColumnStatsData.setDoubleStats(getDoubleColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("lowValue").asDouble(), root.get("highValue").asDouble(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case STRING_STATS:
                hiveColumnStatsData.setStringStats(getStringColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), root.get("avgColLen").asDouble(), root.get("maxColLen").asLong(), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case BINARY_STATS:
                hiveColumnStatsData.setBinaryStats(getBinaryColumnStatsData(root.get("numNulls").asLong(), root.get("avgColLen").asDouble(), root.get("maxColLen").asLong()));
                break;
            case DECIMAL_STATS:
                hiveColumnStatsData.setDecimalStats(getDecimalColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), createThriftDecimal(root.get("lowValue").textValue()), createThriftDecimal(root.get("highValue").textValue()), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            case DATE_STATS:
                hiveColumnStatsData.setDateStats(getDateColumnStatsData(root.get("numNulls").asLong(), root.get("numDVs").asLong(), new Date(root.get("lowValue").asLong()), new Date(root.get("highValue").asLong()), root.get("bitVectors").binaryValue(), enableBitVector));
                break;
            default:
                break;
        }
        hiveColumnStats.setStatsData(hiveColumnStatsData);
        return hiveColumnStats;
    }

    public static List<ColumnStatisticsObj> toHiveColumnStatsObjs(List<com.aliyun.datalake20200710.models.ColumnStatisticsObj> columnStatisticsObj, boolean enableBitVector) throws IOException
    {
        List<ColumnStatisticsObj> hiveColumnStatsObjs = new ArrayList<>();
        for (com.aliyun.datalake20200710.models.ColumnStatisticsObj obj : columnStatisticsObj) {
            hiveColumnStatsObjs.add(toHiveColumnStatsObj(obj, enableBitVector));
        }
        return hiveColumnStatsObjs;
    }

    public static Map<String, List<ColumnStatisticsObj>> toHiveColumnStatsObjMaps(Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> columnStatisticsObjMap, boolean enableBitVector) throws IOException
    {
        Map<String, List<ColumnStatisticsObj>> hiveStatsMap = new HashMap<>();
        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = columnStatisticsObjMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> iter = iterator.next();
            hiveStatsMap.put(iter.getKey(), toHiveColumnStatsObjs(iter.getValue(), enableBitVector));
        }
        return hiveStatsMap;
    }

    public static Map<String, ColumnStatistics> toHiveColumnStatsMaps(String catalogId, String dbName, String tblName, Map<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> columnStatisticsObjMap, boolean isTableLevel, boolean enableBitVector) throws IOException
    {
        Map<String, ColumnStatistics> hiveStatsMap = new HashMap<>();
        Iterator<Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>>> iterator = columnStatisticsObjMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<com.aliyun.datalake20200710.models.ColumnStatisticsObj>> iter = iterator.next();
            List<ColumnStatisticsObj> objs = toHiveColumnStatsObjs(iter.getValue(), enableBitVector);
            ColumnStatistics stats = new ColumnStatistics();
            ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
            desc.setTableName(tblName);
            desc.setDbName(dbName);
            desc.setPartName(iter.getKey());
            desc.setIsTblLevel(isTableLevel);
            desc.setCatName(catalogId);
            stats.setStatsDesc(desc);
            stats.setStatsObj(objs);
            hiveStatsMap.put(iter.getKey(), stats);
        }
        return hiveStatsMap;
    }

    public static ColumnStatisticsDesc toHiveColumnStatsDesc(String catalogId, String dbName, String tblName, boolean isTableLevel, com.aliyun.datalake20200710.models.ColumnStatisticsDesc columnStatistics)
    {
        ColumnStatisticsDesc hiveColumnStatsDesc = new ColumnStatisticsDesc();
        hiveColumnStatsDesc.setPartName(columnStatistics.getPartitionName());
        hiveColumnStatsDesc.setLastAnalyzed(columnStatistics.getLastAnalyzedTime());
        hiveColumnStatsDesc.setTableName(dbName);
        hiveColumnStatsDesc.setDbName(tblName);
        hiveColumnStatsDesc.setCatName(catalogId);
        hiveColumnStatsDesc.setIsTblLevel(isTableLevel);
        return hiveColumnStatsDesc;
    }

    public static LongColumnStatsData getLongColumnStatsData(Long numNulls,
                                                             Long numDVs,
                                                             Long lowValue,
                                                             Long highValue,
                                                             byte[] bitVectors,
                                                             boolean enableBitVector)
    {
        LongColumnStatsDataInspector longStats = new LongColumnStatsDataInspector();
        longStats.setNumNulls(numNulls);
        if (highValue != null) {
            longStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            longStats.setLowValue(lowValue);
        }
        longStats.setNumDVs(numDVs);
        if (enableBitVector) {
            longStats.setBitVectors(bitVectors);
        }
        return longStats;
    }

    public static StringColumnStatsData getStringColumnStatsData(Long numNulls,
                                                                 Long numDVs,
                                                                 double avglen,
                                                                 Long maxlen,
                                                                 byte[] bitVectors,
                                                                 boolean enableBitVector)
    {
        StringColumnStatsDataInspector stringStats = new StringColumnStatsDataInspector();
        stringStats.setNumNulls(numNulls);
        stringStats.setAvgColLen(avglen);
        stringStats.setMaxColLen(maxlen);
        stringStats.setNumDVs(numDVs);
        if (enableBitVector) {
            stringStats.setBitVectors(bitVectors);
        }
        return stringStats;
    }

    public static DoubleColumnStatsData getDoubleColumnStatsData(Long numNulls,
                                                                 Long numDVs,
                                                                 Double lowValue,
                                                                 Double highValue,
                                                                 byte[] bitVectors,
                                                                 boolean enableBitVector)
    {
        DoubleColumnStatsDataInspector doubleStats = new DoubleColumnStatsDataInspector();
        doubleStats.setNumNulls(numNulls);
        if (highValue != null) {
            doubleStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            doubleStats.setLowValue(lowValue);
        }
        doubleStats.setNumDVs(numDVs);
        if (enableBitVector) {
            doubleStats.setBitVectors(bitVectors);
        }
        return doubleStats;
    }

    public static DecimalColumnStatsData getDecimalColumnStatsData(Long numNulls,
                                                                   Long numDVs,
                                                                   Decimal lowValue,
                                                                   Decimal highValue,
                                                                   byte[] bitVectors,
                                                                   boolean enableBitVector)
    {
        DecimalColumnStatsDataInspector decimalStats = new DecimalColumnStatsDataInspector();
        decimalStats.setNumNulls(numNulls);
        if (highValue != null) {
            decimalStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            decimalStats.setLowValue(lowValue);
        }
        decimalStats.setNumDVs(numDVs);
        if (enableBitVector) {
            decimalStats.setBitVectors(bitVectors);
        }
        return decimalStats;
    }

    public static DateColumnStatsData getDateColumnStatsData(Long numNulls,
                                                             Long numDVs,
                                                             Date lowValue,
                                                             Date highValue,
                                                             byte[] bitVectors,
                                                             boolean enableBitVector)
    {
        DateColumnStatsDataInspector dateStats = new DateColumnStatsDataInspector();
        dateStats.setNumNulls(numNulls);
        if (highValue != null) {
            dateStats.setHighValue(highValue);
        }
        if (lowValue != null) {
            dateStats.setLowValue(lowValue);
        }
        dateStats.setNumDVs(numDVs);
        if (enableBitVector) {
            dateStats.setBitVectors(bitVectors);
        }
        return dateStats;
    }

    public static BooleanColumnStatsData getBooleanColumnStatsData(Long numFalses, Long numTrues, Long numNulls)
    {
        BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
        boolStats.setNumFalses(numFalses);
        boolStats.setNumTrues(numTrues);
        boolStats.setNumNulls(numNulls);
        return boolStats;
    }

    public static BinaryColumnStatsData getBinaryColumnStatsData(Long numNulls, double avglen, Long maxlen)
    {
        BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
        binaryStats.setNumNulls(numNulls);
        binaryStats.setAvgColLen(avglen);
        binaryStats.setMaxColLen(maxlen);
        return binaryStats;
    }

    public static Decimal createThriftDecimal(String s)
    {
        BigDecimal d = new BigDecimal(s);
        return new Decimal((short) d.scale(), ByteBuffer.wrap(d.unscaledValue().toByteArray()));
    }

    interface MetaStoreException
    {
        TrinoException get(String msg, Throwable e);
    }

    private static class SdConsumer
            implements Consumer<Storage.Builder>
    {
        private com.aliyun.datalake20200710.models.StorageDescriptor sd;
        private Map<String, String> parameters;

        SdConsumer(com.aliyun.datalake20200710.models.StorageDescriptor sd, Map<String, String> parameters)
        {
            this.sd = sd;
            this.parameters = parameters;
        }

        @Override
        public void accept(Storage.Builder storageBuilder)
        {
            Optional<HiveBucketProperty> bucketProperty = Optional.empty();
            if (sd.numBuckets > 0) {
                List<SortingColumn> sortedBy = ImmutableList.of();
                if (!isNullOrEmpty(sd.sortCols)) {
                    sortedBy = sd.sortCols.stream()
                            .map(c -> new SortingColumn(c.col, Order.fromMetastoreApiOrder(c.order, "unknown")))
                            .collect(Collectors.toList());
                }
                BucketingVersion bucketingVersion = HiveBucketing.getBucketingVersion(parameters);
                bucketProperty =
                        Optional.of(new HiveBucketProperty(sd.bucketCols, bucketingVersion, sd.numBuckets, sortedBy));
            }

            storageBuilder.setStorageFormat(StorageFormat.createNullable(
                    sd.serDeInfo.serializationLib, sd.inputFormat, sd.outputFormat))
                    .setLocation(Strings.nullToEmpty(sd.location))
                    .setBucketProperty(bucketProperty)
                    .setSkewed(sd.skewedInfo != null && !isNullOrEmpty(sd.skewedInfo.skewedColNames))
                    .setSerdeParameters(sd.serDeInfo.parameters);
        }
    }
}
