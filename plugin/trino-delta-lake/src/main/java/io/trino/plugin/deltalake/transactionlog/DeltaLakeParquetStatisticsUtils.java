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

import io.airlift.log.Logger;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;

public class DeltaLakeParquetStatisticsUtils
{
    private static final Logger LOG = Logger.get(DeltaLakeParquetStatisticsUtils.class);

    private DeltaLakeParquetStatisticsUtils() {}

    public static boolean hasInvalidStatistics(Collection<ColumnChunkMetaData> metadataList)
    {
        return metadataList.stream()
                .anyMatch(metadata ->
                        // If any row group does not have stats collected, stats for the file will not be valid
                        !metadata.getStatistics().isNumNullsSet() || metadata.getStatistics().isEmpty() ||
                        // Columns with NaN values are marked by `hasNonNullValue` = false by the Parquet reader. See issue: https://issues.apache.org/jira/browse/PARQUET-1246
                        (!metadata.getStatistics().hasNonNullValue() && metadata.getStatistics().getNumNulls() != metadata.getValueCount()));
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

    private static Optional<Object> getMin(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMin() == null) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMin());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics) {
                Instant ts = Instant.ofEpochMilli(((LongStatistics) statistics).genericGetMin());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            else if (statistics instanceof BinaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(((BinaryStatistics) statistics).genericGetMin());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.getEpochSeconds(), decodedTimestamp.getNanosOfSecond());
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
            if (statistics instanceof IntStatistics) {
                min = BigDecimal.valueOf(((IntStatistics) statistics).getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            else if (statistics instanceof LongStatistics) {
                min = BigDecimal.valueOf(((LongStatistics) statistics).getMin()).movePointLeft(scale);
                return Optional.of(min.toPlainString());
            }
            else if (statistics instanceof BinaryStatistics) {
                BigInteger base = new BigInteger(((BinaryStatistics) statistics).genericGetMin().getBytes());
                min = new BigDecimal(base, scale);
                return Optional.of(min.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMin().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN)) {
            // Boolean columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.warn("Accumulating Parquet statistics with Presto type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }

    private static Optional<Object> getMax(Type type, Statistics<?> statistics)
    {
        if (statistics.genericGetMax() == null) {
            return Optional.empty();
        }

        if (type.equals(DateType.DATE)) {
            checkArgument(statistics instanceof IntStatistics, "Column with DATE type contained invalid statistics: %s", statistics);
            IntStatistics intStatistics = (IntStatistics) statistics;
            LocalDate date = LocalDate.ofEpochDay(intStatistics.genericGetMax());
            return Optional.of(date.format(ISO_LOCAL_DATE));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (statistics instanceof LongStatistics) {
                Instant ts = Instant.ofEpochMilli(((LongStatistics) statistics).genericGetMax());
                return Optional.of(ISO_INSTANT.format(ZonedDateTime.ofInstant(ts, UTC)));
            }
            else if (statistics instanceof BinaryStatistics) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(((BinaryStatistics) statistics).genericGetMax());
                Instant ts = Instant.ofEpochSecond(decodedTimestamp.getEpochSeconds(), decodedTimestamp.getNanosOfSecond());
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
            if (statistics instanceof IntStatistics) {
                max = BigDecimal.valueOf(((IntStatistics) statistics).getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            else if (statistics instanceof LongStatistics) {
                max = BigDecimal.valueOf(((LongStatistics) statistics).getMax()).movePointLeft(scale);
                return Optional.of(max.toPlainString());
            }
            else if (statistics instanceof BinaryStatistics) {
                BigInteger base = new BigInteger(((BinaryStatistics) statistics).genericGetMax().getBytes());
                max = new BigDecimal(base, scale);
                return Optional.of(max.toPlainString());
            }
        }

        if (type instanceof VarcharType) {
            return Optional.of(new String(((BinaryStatistics) statistics).genericGetMax().getBytes(), UTF_8));
        }

        if (type.equals(BOOLEAN)) {
            // Boolean columns do not collect min/max stats
            return Optional.empty();
        }

        LOG.warn("Accumulating Parquet statistics with Presto type: %s and Parquet statistics of type: %s is not supported", type, statistics);
        return Optional.empty();
    }
}
