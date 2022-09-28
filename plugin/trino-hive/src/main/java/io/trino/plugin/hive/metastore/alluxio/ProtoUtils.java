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
package io.trino.plugin.hive.metastore.alluxio;

import alluxio.grpc.table.BinaryColumnStatsData;
import alluxio.grpc.table.BooleanColumnStatsData;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.Date;
import alluxio.grpc.table.DateColumnStatsData;
import alluxio.grpc.table.Decimal;
import alluxio.grpc.table.DecimalColumnStatsData;
import alluxio.grpc.table.DoubleColumnStatsData;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.LongColumnStatsData;
import alluxio.grpc.table.PrincipalType;
import alluxio.grpc.table.StringColumnStatsData;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.shaded.client.com.google.protobuf.InvalidProtocolBufferException;
import com.google.common.collect.Lists;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.TrinoException;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreDistinctValuesCount;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getTotalSizeInBytes;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public final class ProtoUtils
{
    private ProtoUtils() {}

    public static Database fromProto(alluxio.grpc.table.Database db)
    {
        Optional<String> owner = Optional.ofNullable(db.getOwnerName());
        Optional<io.trino.spi.security.PrincipalType> ownerType = owner.map(name -> db.getOwnerType() == PrincipalType.USER ? io.trino.spi.security.PrincipalType.USER : io.trino.spi.security.PrincipalType.ROLE);
        return Database.builder()
                .setDatabaseName(db.getDbName())
                .setLocation(db.hasLocation() ? Optional.of(db.getLocation()) : Optional.empty())
                .setOwnerName(owner)
                .setOwnerType(ownerType)
                .setComment(db.hasComment() ? Optional.of(db.getComment()) : Optional.empty())
                .setParameters(db.getParameterMap())
                .build();
    }

    public static Table fromProto(alluxio.grpc.table.TableInfo table)
    {
        if (!table.hasLayout()) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table metadata. missing layout.: " + table.getTableName());
        }
        Layout layout = table.getLayout();
        if (!alluxio.table.ProtoUtils.isHiveLayout(layout)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table layout: " + layout + " for table: " + table.getTableName());
        }
        try {
            PartitionInfo partitionInfo = alluxio.table.ProtoUtils.toHiveLayout(layout);

            // compute the data columns
            Set<String> partitionColumns = table.getPartitionColsList().stream()
                    .map(FieldSchema::getName)
                    .collect(toImmutableSet());
            List<FieldSchema> dataColumns = table.getSchema().getColsList().stream()
                    .filter((f) -> !partitionColumns.contains(f.getName()))
                    .collect(toImmutableList());

            Map<String, String> tableParameters = table.getParametersMap();
            Table.Builder builder = Table.builder()
                    .setDatabaseName(table.getDbName())
                    .setTableName(table.getTableName())
                    .setOwner(Optional.ofNullable(table.getOwner()))
                    .setTableType(table.getType().toString())
                    .setDataColumns(dataColumns.stream()
                            .map(ProtoUtils::fromProto)
                            .collect(toImmutableList()))
                    .setPartitionColumns(table.getPartitionColsList().stream()
                            .map(ProtoUtils::fromProto)
                            .collect(toImmutableList()))
                    .setParameters(tableParameters)
                    .setViewOriginalText(Optional.empty())
                    .setViewExpandedText(Optional.empty());
            alluxio.grpc.table.layout.hive.Storage storage = partitionInfo.getStorage();
            builder.getStorageBuilder()
                    .setSkewed(storage.getSkewed())
                    .setStorageFormat(fromProto(storage.getStorageFormat()))
                    .setLocation(storage.getLocation())
                    .setBucketProperty(storage.hasBucketProperty() ? fromProto(tableParameters, storage.getBucketProperty()) : Optional.empty())
                    .setSerdeParameters(storage.getStorageFormat().getSerdelibParametersMap());
            return builder.build();
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo from TableInfo", e);
        }
    }

    static SortingColumn fromProto(alluxio.grpc.table.layout.hive.SortingColumn column)
    {
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.ASCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.ASCENDING);
        }
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.DESCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.DESCENDING);
        }
        throw new IllegalArgumentException("Invalid sort order: " + column.getOrder());
    }

    static Optional<HiveBucketProperty> fromProto(Map<String, String> tableParameters, alluxio.grpc.table.layout.hive.HiveBucketProperty property)
    {
        // must return empty if buckets <= 0
        if (!property.hasBucketCount() || property.getBucketCount() <= 0) {
            return Optional.empty();
        }
        List<SortingColumn> sortedBy = property.getSortedByList().stream()
                .map(ProtoUtils::fromProto)
                .collect(toImmutableList());
        HiveBucketing.BucketingVersion bucketingVersion = HiveBucketing.getBucketingVersion(tableParameters);
        return Optional.of(new HiveBucketProperty(property.getBucketedByList(), bucketingVersion, (int) property.getBucketCount(), sortedBy));
    }

    static StorageFormat fromProto(alluxio.grpc.table.layout.hive.StorageFormat format)
    {
        return StorageFormat.create(format.getSerde(), format.getInputFormat(), format.getOutputFormat());
    }

    private static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled().toByteArray()), decimal.getScale()));
    }

    private static Optional<LocalDate> fromMetastoreDate(@Nullable Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(date.getDaysSinceEpoch()));
    }

    public static HiveColumnStatistics fromProto(ColumnStatisticsData columnStatistics, OptionalLong rowCount)
    {
        if (columnStatistics.hasLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getLongStats();
            OptionalLong min = longStatsData.hasLowValue() ? OptionalLong.of(longStatsData.getLowValue()) : OptionalLong.empty();
            OptionalLong max = longStatsData.hasHighValue() ? OptionalLong.of(longStatsData.getHighValue()) : OptionalLong.empty();
            OptionalLong nullsCount = longStatsData.hasNumNulls() ? fromMetastoreNullsCount(longStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = longStatsData.hasNumDistincts() ? OptionalLong.of(longStatsData.getNumDistincts()) : OptionalLong.empty();
            return createIntegerColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getDoubleStats();
            OptionalDouble min = doubleStatsData.hasLowValue() ? OptionalDouble.of(doubleStatsData.getLowValue()) : OptionalDouble.empty();
            OptionalDouble max = doubleStatsData.hasHighValue() ? OptionalDouble.of(doubleStatsData.getHighValue()) : OptionalDouble.empty();
            OptionalLong nullsCount = doubleStatsData.hasNumNulls() ? fromMetastoreNullsCount(doubleStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = doubleStatsData.hasNumDistincts() ? OptionalLong.of(doubleStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDoubleColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getDecimalStats();
            Optional<BigDecimal> min = decimalStatsData.hasLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty();
            Optional<BigDecimal> max = decimalStatsData.hasHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = decimalStatsData.hasNumNulls() ? fromMetastoreNullsCount(decimalStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = decimalStatsData.hasNumDistincts() ? OptionalLong.of(decimalStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDecimalColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getDateStats();
            Optional<LocalDate> min = dateStatsData.hasLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty();
            Optional<LocalDate> max = dateStatsData.hasHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = dateStatsData.hasNumNulls() ? fromMetastoreNullsCount(dateStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = dateStatsData.hasNumDistincts() ? OptionalLong.of(dateStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDateColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getBooleanStats();
            OptionalLong trueCount = OptionalLong.empty();
            OptionalLong falseCount = OptionalLong.empty();
            // Impala 'COMPUTE STATS' writes 1 as the numTrue and -1 as the numFalse
            if (booleanStatsData.hasNumTrues() && booleanStatsData.hasNumFalses() && (booleanStatsData.getNumFalses() != -1)) {
                trueCount = OptionalLong.of(booleanStatsData.getNumTrues());
                falseCount = OptionalLong.of(booleanStatsData.getNumFalses());
            }
            return createBooleanColumnStatistics(
                    trueCount,
                    falseCount,
                    booleanStatsData.hasNumNulls() ? fromMetastoreNullsCount(booleanStatsData.getNumNulls()) : OptionalLong.empty());
        }
        if (columnStatistics.hasStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStringStats();
            OptionalLong maxColumnLength = stringStatsData.hasMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = stringStatsData.hasAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = stringStatsData.hasNumNulls() ? fromMetastoreNullsCount(stringStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = stringStatsData.hasNumDistincts() ? OptionalLong.of(stringStatsData.getNumDistincts()) : OptionalLong.empty();
            return createStringColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getBinaryStats();
            OptionalLong maxColumnLength = binaryStatsData.hasMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = binaryStatsData.hasAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = binaryStatsData.hasNumNulls() ? fromMetastoreNullsCount(binaryStatsData.getNumNulls()) : OptionalLong.empty();
            return createBinaryColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount);
        }
        throw new TrinoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + columnStatistics);
    }

    static Column fromProto(alluxio.grpc.table.FieldSchema column)
    {
        Optional<String> comment = column.hasComment() ? Optional.of(column.getComment()) : Optional.empty();
        return new Column(column.getName(), HiveType.valueOf(column.getType()), comment);
    }

    public static Partition fromProto(alluxio.grpc.table.layout.hive.PartitionInfo info)
    {
        Map<String, String> parametersMap = info.getParametersMap();
        Partition.Builder builder = Partition.builder()
                .setColumns(info.getDataColsList().stream()
                        .map(ProtoUtils::fromProto)
                        .collect(toImmutableList()))
                .setDatabaseName(info.getDbName())
                .setParameters(parametersMap)
                .setValues(Lists.newArrayList(info.getValuesList()))
                .setTableName(info.getTableName());

        builder.getStorageBuilder()
                .setSkewed(info.getStorage().getSkewed())
                .setStorageFormat(fromProto(info.getStorage().getStorageFormat()))
                .setLocation(info.getStorage().getLocation())
                .setBucketProperty(info.getStorage().hasBucketProperty()
                        ? fromProto(parametersMap, info.getStorage().getBucketProperty()) : Optional.empty())
                .setSerdeParameters(info.getStorage().getStorageFormat().getSerdelibParametersMap());

        return builder.build();
    }

    public static alluxio.grpc.table.layout.hive.PartitionInfo toPartitionInfo(alluxio.grpc.table.Partition part)
    {
        try {
            return alluxio.table.ProtoUtils.extractHiveLayout(part);
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo", e);
        }
    }

    public static List<alluxio.grpc.table.layout.hive.PartitionInfo> toPartitionInfoList(List<alluxio.grpc.table.Partition> parts)
    {
        return parts.stream()
                .map(ProtoUtils::toPartitionInfo)
                .collect(toImmutableList());
    }
}
