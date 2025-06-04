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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;

public final class DeltaLakeParquetStatisticsUtils
{
    private DeltaLakeParquetStatisticsUtils() {}

    private static final Logger LOG = Logger.get(DeltaLakeParquetStatisticsUtils.class);

    public static boolean hasInvalidStatistics(Collection<ColumnChunkMetadata> metadataList)
    {
        return metadataList.stream()
                .anyMatch(metadata ->
                        // If any row group does not have stats collected, stats for the file will not be valid
                        !metadata.getStatistics().isNumNullsSet() || metadata.getStatistics().isEmpty() ||
                                // Columns with NaN values are marked by `hasNonNullValue` = false by the Parquet reader. See issue: https://issues.apache.org/jira/browse/PARQUET-1246
                                (!metadata.getStatistics().hasNonNullValue() && metadata.getStatistics().getNumNulls() != metadata.getValueCount()));
    }

    @Nullable
    public static Object jsonValueToTrinoValue(Type type, @Nullable Object jsonValue)
    {
        if (jsonValue == null) {
            return null;
        }

        if (type == SMALLINT || type == TINYINT || type == INTEGER) {
            return (long) (int) jsonValue;
        }
        if (type == BIGINT) {
            if (jsonValue instanceof Long) {
                //noinspection RedundantCast
                return (long) jsonValue;
            }
            if (jsonValue instanceof Integer) {
                return (long) (int) jsonValue;
            }
            throw new IllegalArgumentException("Unexpected value for bigint type: " + jsonValue);
        }
        if (type == REAL) {
            return (long) floatToRawIntBits((float) (double) jsonValue);
        }
        if (type == DOUBLE) {
            //noinspection RedundantCast
            return (double) jsonValue;
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = new BigDecimal((String) jsonValue);

            if (decimalType.isShort()) {
                return Decimals.encodeShortScaledValue(decimal, decimalType.getScale());
            }
            return Decimals.encodeScaledValue(decimal, decimalType.getScale());
        }
        if (type instanceof VarcharType) {
            return utf8Slice((String) jsonValue);
        }
        if (type == DateType.DATE) {
            return LocalDate.parse((String) jsonValue).toEpochDay();
        }
        if (type == TIMESTAMP_MILLIS) {
            return Instant.parse((String) jsonValue).toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
        }
        if (type == TIMESTAMP_MICROS) {
            Instant instant = Instant.parse((String) jsonValue);
            return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
        }
        if (type instanceof RowType rowType) {
            Map<?, ?> values = (Map<?, ?>) jsonValue;
            List<Type> fieldTypes = rowType.getTypeParameters();
            return buildRowValue(rowType, fields -> {
                for (int i = 0; i < values.size(); ++i) {
                    Type fieldType = fieldTypes.get(i);
                    String fieldName = rowType.getFields().get(i).getName().orElseThrow(() -> new IllegalArgumentException("Field name must exist"));
                    Object fieldValue = jsonValueToTrinoValue(fieldType, values.remove(fieldName));
                    writeNativeValue(fieldType, fields.get(i), fieldValue);
                }
                checkState(values.isEmpty(), "All fields must be converted into Trino value: %s", values);
            });
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static Map<String, Object> toJsonValues(Map<String, Type> columnTypeMapping, Map<String, Object> values)
    {
        Map<String, Object> jsonValues = new HashMap<>();
        for (Map.Entry<String, Object> value : values.entrySet()) {
            Type type = columnTypeMapping.get(value.getKey());
            if (type instanceof ArrayType || type instanceof MapType) {
                continue;
            }
            jsonValues.put(value.getKey(), toJsonValue(columnTypeMapping.get(value.getKey()), value.getValue()));
        }
        return jsonValues;
    }

    @Nullable
    public static Object toJsonValue(Type type, @Nullable Object value)
    {
        if (value == null) {
            return null;
        }

        if (type == SMALLINT || type == TINYINT || type == INTEGER || type == BIGINT) {
            return value;
        }
        if (type == REAL) {
            return intBitsToFloat(toIntExact((long) value));
        }
        if (type == DOUBLE) {
            return value;
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return Decimals.toString((long) value, decimalType.getScale());
            }
            return Decimals.toString((Int128) value, decimalType.getScale());
        }

        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        if (type == DateType.DATE) {
            return LocalDate.ofEpochDay((long) value).format(ISO_LOCAL_DATE);
        }
        if (type == TIMESTAMP_MICROS) {
            long epochMicros = (long) value;
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoAdjustment = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
            return ISO_INSTANT.format(ZonedDateTime.ofInstant(instant.truncatedTo(MILLIS), UTC));
        }
        if (type == TIMESTAMP_TZ_MILLIS) {
            Instant ts = Instant.ofEpochMilli(unpackMillisUtc((long) value));
            return ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC));
        }
        if (type instanceof RowType rowType) {
            SqlRow row = (SqlRow) value;
            int rawIndex = row.getRawIndex();
            ImmutableMap.Builder<String, Object> fieldValues = ImmutableMap.builder();
            for (int i = 0; i < row.getFieldCount(); i++) {
                RowType.Field field = rowType.getFields().get(i);
                Object fieldValue = readNativeValue(field.getType(), row.getRawFieldBlock(i), rawIndex);
                Object jsonValue = toJsonValue(field.getType(), fieldValue);
                if (jsonValue != null) {
                    fieldValues.put(field.getName().orElseThrow(), jsonValue);
                }
            }
            return fieldValues.buildOrThrow();
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static Map<String, Object> jsonEncodeMin(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn)
    {
        return jsonEncode(stats, typeForColumn, DeltaLakeParquetStatisticsUtils::getMin);
    }

    public static Map<String, Object> jsonEncodeMax(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn)
    {
        return jsonEncode(stats, typeForColumn, DeltaLakeParquetStatisticsUtils::getMax);
    }

    private static Map<String, Object> jsonEncode(Map<String, Optional<Statistics<?>>> stats, Map<String, Type> typeForColumn, BiFunction<Type, Statistics<?>, Optional<Object>> accessor)
    {
        Map<String, Optional<Object>> allStats = stats.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().isPresent() && !entry.getValue().get().isEmpty())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> accessor.apply(typeForColumn.get(entry.getKey()), entry.getValue().get())));

        return allStats.entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().isPresent())
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    public static Map<String, Object> toNullCounts(Map<String, Type> columnTypeMapping, Map<String, Object> values)
    {
        ImmutableMap.Builder<String, Object> nullCounts = ImmutableMap.builderWithExpectedSize(values.size());
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Type type = columnTypeMapping.get(entry.getKey());
            requireNonNull(type, "type is null");
            Object value = entry.getValue();
            if (type instanceof RowType rowType) {
                value = toNullCount(rowType, (SqlRow) value);
            }
            nullCounts.put(entry.getKey(), value);
        }
        return nullCounts.buildOrThrow();
    }

    private static ImmutableMap<String, Object> toNullCount(RowType rowType, SqlRow row)
    {
        List<RowType.Field> fields = rowType.getFields();
        ImmutableMap.Builder<String, Object> nullCounts = ImmutableMap.builderWithExpectedSize(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            Block fieldBlock = row.getRawFieldBlock(i);
            int fieldBlockIndex = row.getRawIndex();
            String fieldName = field.getName().orElseThrow();
            if (field.getType() instanceof RowType fieldRowType) {
                nullCounts.put(fieldName, toNullCount(fieldRowType, fieldRowType.getObject(fieldBlock, fieldBlockIndex)));
            }
            else {
                nullCounts.put(fieldName, BIGINT.getLong(fieldBlock, fieldBlockIndex));
            }
        }
        return nullCounts.buildOrThrow();
    }

    private static Optional<Object> getMin(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMin() == null || !statistics.hasNonNullValue()) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMin());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampType) {
            // Spark truncates the timestamp to milliseconds. The connector follows the same behavior.
            if (statistics instanceof LongStatistics longStatistics) {
                long epochMicros = longStatistics.genericGetMin();
                long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
                int nanoAdjustment = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
                Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(instant, UTC).truncatedTo(MILLIS)));
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(binaryStatistics.genericGetMin());
                Instant instant = Instant.ofEpochSecond(decodedTimestamp.epochSeconds(), decodedTimestamp.nanosOfSecond());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(instant, UTC).truncatedTo(MILLIS)));
            }
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics longStatistics) {
                Instant ts = Instant.ofEpochMilli(longStatistics.genericGetMin());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(binaryStatistics.genericGetMin());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.epochSeconds(), decodedTimestamp.nanosOfSecond());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC).truncatedTo(MILLIS)));
            }
        }

        if (type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) {
            checkArgument(statistics instanceof IntStatistics || statistics instanceof LongStatistics,
                    "Column with %s type contained invalid statistics: %s", type, statistics);
            return Optional.of(statistics.genericGetMin());
        }

        if (type.equals(REAL)) {
            checkArgument(statistics instanceof FloatStatistics, "Column with REAL type contained invalid statistics: %s", statistics);
            Float min = ((FloatStatistics) statistics).genericGetMin();
            return Optional.of(min.compareTo(-0.0f) == 0 ? 0.0f : min);
        }

        if (type.equals(DOUBLE)) {
            checkArgument(statistics instanceof DoubleStatistics, "Column with DOUBLE type contained invalid statistics: %s", statistics);
            Double min = ((DoubleStatistics) statistics).genericGetMin();
            return Optional.of(min.compareTo(-0.0d) == 0 ? 0.0d : min);
        }

        if (type instanceof DecimalType) {
            LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
            checkArgument(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, "DECIMAL column had invalid Parquet Logical Type: %s", logicalType);
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

            BigDecimal min;
            if (statistics instanceof IntStatistics intStatistics) {
                min = BigDecimal.valueOf(intStatistics.getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            if (statistics instanceof LongStatistics longStatistics) {
                min = BigDecimal.valueOf(longStatistics.getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                BigInteger base = new BigInteger(binaryStatistics.genericGetMin().getBytes());
                min = new BigDecimal(base, scale);
                return Optional.of(min.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMin().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN) || type.equals(VARBINARY)) {
            // Boolean and varbinary columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.debug("Accumulating Parquet statistics with Trino type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }

    private static Optional<Object> getMax(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMax() == null || !statistics.hasNonNullValue()) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMax());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampType timestampType) {
            // Spark truncates the timestamp to milliseconds. The connector follows the same behavior.
            checkArgument(timestampType.getPrecision() >= TIMESTAMP_MILLIS.getPrecision(), "type precision must be at least milliseconds");

            if (statistics instanceof LongStatistics longStatistics) {
                long epochMicros = longStatistics.genericGetMax();
                long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
                int nanoAdjustment = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
                Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                Instant truncatedToMillis = instant.truncatedTo(MILLIS);
                if (truncatedToMillis.isBefore(instant)) {
                    truncatedToMillis = truncatedToMillis.plusMillis(1);
                }
                return Optional.of(ISO_INSTANT.format(truncatedToMillis));
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(binaryStatistics.genericGetMax());
                Instant instant = Instant.ofEpochSecond(decodedTimestamp.epochSeconds(), decodedTimestamp.nanosOfSecond());
                Instant truncatedToMillis = instant.truncatedTo(MILLIS);
                if (truncatedToMillis.isBefore(instant)) {
                    truncatedToMillis = truncatedToMillis.plusMillis(1);
                }
                return Optional.of(ISO_INSTANT.format(truncatedToMillis));
            }
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics longStatistics) {
                Instant ts = Instant.ofEpochMilli(longStatistics.genericGetMax());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(binaryStatistics.genericGetMax());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.epochSeconds(), decodedTimestamp.nanosOfSecond());
                ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(ts, UTC);
                ZonedDateTime truncatedToMillis = zonedDateTime.truncatedTo(MILLIS);
                if (truncatedToMillis.isBefore(zonedDateTime)) {
                    truncatedToMillis = truncatedToMillis.plus(1, MILLIS);
                }
                return Optional.of(ISO_INSTANT.format(truncatedToMillis));
            }
        }

        if (type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER)) {
            checkArgument(statistics instanceof IntStatistics || statistics instanceof LongStatistics,
                    "Column with %s type contained invalid statistics: %s", type, statistics);
            return Optional.of(statistics.genericGetMax());
        }

        if (type.equals(REAL)) {
            checkArgument(statistics instanceof FloatStatistics, "Column with REAL type contained invalid statistics: %s", statistics);
            return Optional.of(((FloatStatistics) statistics).genericGetMax());
        }

        if (type.equals(DOUBLE)) {
            checkArgument(statistics instanceof DoubleStatistics, "Column with DOUBLE type contained invalid statistics: %s", statistics);
            return Optional.of(((DoubleStatistics) statistics).genericGetMax());
        }

        if (type instanceof DecimalType) {
            LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
            checkArgument(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, "DECIMAL column had invalid Parquet Logical Type: %s", logicalType);
            int scale = ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale();

            BigDecimal max;
            if (statistics instanceof IntStatistics intStatistics) {
                max = BigDecimal.valueOf(intStatistics.getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            if (statistics instanceof LongStatistics longStatistics) {
                max = BigDecimal.valueOf(longStatistics.getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            if (statistics instanceof BinaryStatistics binaryStatistics) {
                BigInteger base = new BigInteger(binaryStatistics.genericGetMax().getBytes());
                max = new BigDecimal(base, scale);
                return Optional.of(max.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMax().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN) || type.equals(VARBINARY)) {
            // Boolean and varbinary columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.debug("Accumulating Parquet statistics with Trino type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }
}
