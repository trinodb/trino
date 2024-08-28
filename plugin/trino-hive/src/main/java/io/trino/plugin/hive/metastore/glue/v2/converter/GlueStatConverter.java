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
package io.trino.plugin.hive.metastore.glue.v2.converter;

import io.trino.hive.thrift.metastore.Decimal;
import io.trino.metastore.Column;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalNumber;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static io.trino.metastore.type.Category.PRIMITIVE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;

public class GlueStatConverter
{
    private GlueStatConverter() {}

    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    public static List<ColumnStatistics> toGlueColumnStatistics(Partition partition, Map<String, HiveColumnStatistics> trinoColumnStats)
    {
        return partition.getColumns().stream()
                .filter(column -> trinoColumnStats.containsKey(column.getName()))
                .map(c -> toColumnStatistics(c, trinoColumnStats.get(c.getName())))
                .collect(toImmutableList());
    }

    public static List<ColumnStatistics> toGlueColumnStatistics(Table table, Map<String, HiveColumnStatistics> trinoColumnStats)
    {
        return trinoColumnStats.entrySet().stream()
                .map(e -> toColumnStatistics(table.getColumn(e.getKey()).get(), e.getValue()))
                .collect(toImmutableList());
    }

    private static ColumnStatistics toColumnStatistics(Column column, HiveColumnStatistics statistics)
    {
        ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
        HiveType columnType = column.getType();
        columnStatistics.columnName(column.getName());
        columnStatistics.columnType(columnType.toString());
        ColumnStatisticsData catalogColumnStatisticsData = toGlueColumnStatisticsData(statistics, columnType);
        columnStatistics.statisticsData(catalogColumnStatisticsData);
        columnStatistics.analyzedTime(Instant.now());
        return columnStatistics.build();
    }

    public static HiveColumnStatistics fromGlueColumnStatistics(ColumnStatisticsData catalogColumnStatisticsData)
    {
        return switch (catalogColumnStatisticsData.type()) {
            case BINARY -> {
                BinaryColumnStatisticsData data = catalogColumnStatisticsData.binaryColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.maximumLength());
                OptionalDouble avg = OptionalDouble.of(data.averageLength());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                yield createBinaryColumnStatistics(max, avg, nulls);
            }
            case BOOLEAN -> {
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.booleanColumnStatisticsData();
                yield createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.numberOfTrues()),
                        OptionalLong.of(catalogBooleanData.numberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.numberOfNulls()));
            }
            case DATE -> {
                DateColumnStatisticsData data = catalogColumnStatisticsData.dateColumnStatisticsData();
                Optional<LocalDate> min = dateToLocalDate(data.minimumValue());
                Optional<LocalDate> max = dateToLocalDate(data.maximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValuesWithNullCount = OptionalLong.of(data.numberOfDistinctValues());
                yield createDateColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
            }
            case DECIMAL -> {
                DecimalColumnStatisticsData data = catalogColumnStatisticsData.decimalColumnStatisticsData();
                Optional<BigDecimal> min = glueDecimalToBigDecimal(data.minimumValue());
                Optional<BigDecimal> max = glueDecimalToBigDecimal(data.maximumValue());
                OptionalLong distinctValuesWithNullCount = OptionalLong.of(data.numberOfDistinctValues());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                yield createDecimalColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
            }
            case DOUBLE -> {
                DoubleColumnStatisticsData data = catalogColumnStatisticsData.doubleColumnStatisticsData();
                OptionalDouble min = OptionalDouble.of(data.minimumValue());
                OptionalDouble max = OptionalDouble.of(data.maximumValue());
                OptionalLong nulls = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValuesWithNullCount = OptionalLong.of(data.numberOfDistinctValues());
                yield createDoubleColumnStatistics(min, max, nulls, distinctValuesWithNullCount);
            }
            case LONG -> {
                LongColumnStatisticsData data = catalogColumnStatisticsData.longColumnStatisticsData();
                OptionalLong min = OptionalLong.of(data.minimumValue());
                OptionalLong max = OptionalLong.of(data.maximumValue());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValuesWithNullCount = OptionalLong.of(data.numberOfDistinctValues());
                yield createIntegerColumnStatistics(min, max, nullsCount, distinctValuesWithNullCount);
            }
            case STRING -> {
                StringColumnStatisticsData data = catalogColumnStatisticsData.stringColumnStatisticsData();
                OptionalLong max = OptionalLong.of(data.maximumLength());
                OptionalDouble avg = OptionalDouble.of(data.averageLength());
                OptionalLong nullsCount = fromMetastoreNullsCount(data.numberOfNulls());
                OptionalLong distinctValuesWithNullCount = OptionalLong.of(data.numberOfDistinctValues());
                yield createStringColumnStatistics(max, avg, nullsCount, distinctValuesWithNullCount);
            }
            case UNKNOWN_TO_SDK_VERSION -> throw new TrinoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
        };
    }

    private static ColumnStatisticsData toGlueColumnStatisticsData(HiveColumnStatistics statistics, HiveType columnType)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "Unsupported statistics type: %s", columnType);

        ColumnStatisticsData.Builder catalogColumnStatisticsData = ColumnStatisticsData.builder();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN: {
                BooleanColumnStatisticsData.Builder data = BooleanColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
                    booleanStatistics.getFalseCount().ifPresent(data::numberOfFalses);
                    booleanStatistics.getTrueCount().ifPresent(data::numberOfTrues);
                });
                catalogColumnStatisticsData.type(ColumnStatisticsType.BOOLEAN);
                catalogColumnStatisticsData.booleanColumnStatisticsData(data.build());
                break;
            }
            case BINARY: {
                BinaryColumnStatisticsData.Builder data = BinaryColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                data.maximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(statistics.getAverageColumnLength().orElse(0));
                catalogColumnStatisticsData.type(ColumnStatisticsType.BINARY);
                catalogColumnStatisticsData.binaryColumnStatisticsData(data.build());
                break;
            }
            case DATE: {
                DateColumnStatisticsData.Builder data = DateColumnStatisticsData.builder();
                statistics.getDateStatistics().ifPresent(dateStatistics -> {
                    dateStatistics.getMin().ifPresent(value -> data.minimumValue(localDateToInstant(value)));
                    dateStatistics.getMax().ifPresent(value -> data.maximumValue(localDateToInstant(value)));
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getDistinctValuesWithNullCount().ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DATE);
                catalogColumnStatisticsData.dateColumnStatisticsData(data.build());
                break;
            }
            case DECIMAL: {
                DecimalColumnStatisticsData.Builder data = DecimalColumnStatisticsData.builder();
                statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
                    decimalStatistics.getMin().ifPresent(value -> data.minimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStatistics.getMax().ifPresent(value -> data.maximumValue(bigDecimalToGlueDecimal(value)));
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getDistinctValuesWithNullCount().ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DECIMAL);
                catalogColumnStatisticsData.decimalColumnStatisticsData(data.build());
                break;
            }
            case FLOAT:
            case DOUBLE: {
                DoubleColumnStatisticsData.Builder data = DoubleColumnStatisticsData.builder();
                statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
                    doubleStatistics.getMin().ifPresent(data::minimumValue);
                    doubleStatistics.getMax().ifPresent(data::maximumValue);
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getDistinctValuesWithNullCount().ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.DOUBLE);
                catalogColumnStatisticsData.doubleColumnStatisticsData(data.build());
                break;
            }
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP: {
                LongColumnStatisticsData.Builder data = LongColumnStatisticsData.builder();
                statistics.getIntegerStatistics().ifPresent(stats -> {
                    stats.getMin().ifPresent(data::minimumValue);
                    stats.getMax().ifPresent(data::maximumValue);
                });
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getDistinctValuesWithNullCount().ifPresent(data::numberOfDistinctValues);
                catalogColumnStatisticsData.type(ColumnStatisticsType.LONG);
                catalogColumnStatisticsData.longColumnStatisticsData(data.build());
                break;
            }
            case VARCHAR:
            case CHAR:
            case STRING: {
                StringColumnStatisticsData.Builder data = StringColumnStatisticsData.builder();
                statistics.getNullsCount().ifPresent(data::numberOfNulls);
                statistics.getDistinctValuesWithNullCount().ifPresent(data::numberOfDistinctValues);
                data.maximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                data.averageLength(statistics.getAverageColumnLength().orElse(0));
                catalogColumnStatisticsData.type(ColumnStatisticsType.STRING);
                catalogColumnStatisticsData.stringColumnStatisticsData(data.build());
                break;
            }
            default:
                throw new TrinoException(HIVE_INVALID_METADATA, "Invalid column statistics type: " + ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
        }
        return catalogColumnStatisticsData.build();
    }

    private static DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal)
    {
        Decimal hiveDecimal = new Decimal((short) decimal.scale(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        DecimalNumber.Builder catalogDecimal = DecimalNumber.builder();
        catalogDecimal.unscaledValue(SdkBytes.fromByteArray(hiveDecimal.getUnscaled()));
        catalogDecimal.scale((int) hiveDecimal.getScale());
        return catalogDecimal.build();
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(DecimalNumber catalogDecimal)
    {
        if (catalogDecimal == null) {
            return Optional.empty();
        }
        Decimal decimal = new Decimal();
        decimal.setUnscaled(catalogDecimal.unscaledValue().asByteArray());
        decimal.setScale(catalogDecimal.scale().shortValue());
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    private static Optional<LocalDate> dateToLocalDate(Instant date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofInstant(date, ZoneId.systemDefault())); // Is this ok?
    }

    private static Instant localDateToInstant(LocalDate date)
    {
        return Instant.ofEpochMilli(date.toEpochDay() * MILLIS_PER_DAY);
    }
}
