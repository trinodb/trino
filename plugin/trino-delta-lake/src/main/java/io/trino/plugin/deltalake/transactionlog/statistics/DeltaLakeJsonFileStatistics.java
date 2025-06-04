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
package io.trino.plugin.deltalake.transactionlog.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.CanonicalColumnName;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.math.LongMath.divide;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.toCanonicalNameKeyedMap;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.JSON_STATISTICS_TIMESTAMP_FORMATTER;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.START_OF_MODERN_ERA_EPOCH_DAY;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.START_OF_MODERN_ERA_EPOCH_MICROS;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.deserializeColumnValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;

public class DeltaLakeJsonFileStatistics
        implements DeltaLakeFileStatistics
{
    private static final Logger log = Logger.get(DeltaLakeJsonFileStatistics.class);
    private static final long INSTANCE_SIZE = instanceSize(DeltaLakeJsonFileStatistics.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final Optional<Long> numRecords;
    private final Optional<Map<CanonicalColumnName, Object>> minValues;
    private final Optional<Map<CanonicalColumnName, Object>> maxValues;
    private final Optional<Map<CanonicalColumnName, Object>> nullCount;

    public static DeltaLakeJsonFileStatistics create(String jsonStatistics)
            throws JsonProcessingException
    {
        return parseJson(OBJECT_MAPPER, jsonStatistics, DeltaLakeJsonFileStatistics.class);
    }

    @JsonCreator
    public DeltaLakeJsonFileStatistics(
            @JsonProperty("numRecords") Optional<Long> numRecords,
            @JsonProperty("minValues") Optional<Map<String, Object>> minValues,
            @JsonProperty("maxValues") Optional<Map<String, Object>> maxValues,
            @JsonProperty("nullCount") Optional<Map<String, Object>> nullCount)
    {
        this.numRecords = numRecords;
        // Re-use CanonicalColumnName for min/max/null maps to benefit from cached hashCode
        Map<String, CanonicalColumnName> canonicalColumnNames = DeltaLakeFileStatistics.getCanonicalColumnNames(minValues, maxValues, nullCount);
        this.minValues = minValues.map(minValuesMap -> toCanonicalNameKeyedMap(minValuesMap, canonicalColumnNames));
        this.maxValues = maxValues.map(maxValuesMap -> toCanonicalNameKeyedMap(maxValuesMap, canonicalColumnNames));
        this.nullCount = nullCount.map(nullCountMap -> toCanonicalNameKeyedMap(nullCountMap, canonicalColumnNames));
    }

    @JsonProperty
    @Override
    public Optional<Long> getNumRecords()
    {
        return numRecords;
    }

    @JsonProperty
    @Override
    public Optional<Map<String, Object>> getMinValues()
    {
        return minValues.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @JsonProperty
    @Override
    public Optional<Map<String, Object>> getMaxValues()
    {
        return maxValues.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @JsonProperty
    @Override
    public Optional<Map<String, Object>> getNullCount()
    {
        return nullCount.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @Override
    public Optional<Object> getMaxColumnValue(DeltaLakeColumnHandle columnHandle)
    {
        if (!columnHandle.isBaseColumn()) {
            return Optional.empty();
        }
        Optional<Object> value = getStat(columnHandle.basePhysicalColumnName(), maxValues);
        return value.flatMap(o -> deserializeStatisticsValue(columnHandle, String.valueOf(o)));
    }

    @Override
    public Optional<Object> getMinColumnValue(DeltaLakeColumnHandle columnHandle)
    {
        if (!columnHandle.isBaseColumn()) {
            return Optional.empty();
        }
        Optional<Object> value = getStat(columnHandle.basePhysicalColumnName(), minValues);
        return value.flatMap(o -> deserializeStatisticsValue(columnHandle, String.valueOf(o)));
    }

    private Optional<Object> deserializeStatisticsValue(DeltaLakeColumnHandle columnHandle, String statValue)
    {
        if (!columnHandle.isBaseColumn()) {
            return Optional.empty();
        }

        Object columnValue;
        try {
            columnValue = deserializeColumnValue(columnHandle, statValue, DeltaLakeJsonFileStatistics::readStatisticsTimestamp, DeltaLakeJsonFileStatistics::readStatisticsTimestampWithZone);
        }
        catch (TrinoException e) {
            // Catching exception and returning empty stats so that one un-serializable column doesn't prevent the use of statistics for all columns
            // TODO ensure that we are able to deserialize all valid values
            log.debug("Cannot deserialize column value, skipping statistics: %s", e.getMessage());
            return Optional.empty();
        }

        Type columnType = columnHandle.baseType();
        if (columnType.equals(DATE)) {
            long epochDate = (long) columnValue;
            if (epochDate < START_OF_MODERN_ERA_EPOCH_DAY) {
                return Optional.empty();
            }
        }
        if (columnType instanceof TimestampType) {
            long epochMicros = (long) columnValue;
            if (epochMicros < START_OF_MODERN_ERA_EPOCH_MICROS) {
                return Optional.empty();
            }
        }
        if (columnType instanceof TimestampWithTimeZoneType) {
            long packedTimestamp = (long) columnValue;
            long epochMillis = unpackMillisUtc(packedTimestamp);
            if (floorDiv(epochMillis, MILLISECONDS_PER_DAY) < START_OF_MODERN_ERA_EPOCH_DAY) {
                return Optional.empty();
            }
        }

        return Optional.of(columnValue);
    }

    private static Long readStatisticsTimestamp(String timestamp)
    {
        LocalDateTime localDateTime = LocalDateTime.parse(timestamp, JSON_STATISTICS_TIMESTAMP_FORMATTER);
        return localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + divide(localDateTime.getNano(), NANOSECONDS_PER_MICROSECOND, UNNECESSARY);
    }

    private static Long readStatisticsTimestampWithZone(String timestamp)
    {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, JSON_STATISTICS_TIMESTAMP_FORMATTER);
        return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), UTC_KEY);
    }

    @Override
    public Optional<Long> getNullCount(String columnName)
    {
        return getStat(columnName, nullCount).map(o -> Long.valueOf(o.toString()));
    }

    private Optional<Object> getStat(String columnName, Optional<Map<CanonicalColumnName, Object>> stats)
    {
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        CanonicalColumnName canonicalColumnName = new CanonicalColumnName(columnName);
        Object contents = stats.get().get(canonicalColumnName);
        if (contents == null) {
            return Optional.empty();
        }
        if (contents instanceof List || contents instanceof Map) {
            log.debug("Skipping statistics value for column with complex value type: %s", columnName);
            return Optional.empty();
        }
        return Optional.of(contents);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long totalSize = INSTANCE_SIZE;
        if (minValues.isPresent()) {
            totalSize += estimatedSizeOf(minValues.get(), CanonicalColumnName::getRetainedSize, value -> SizeOf.sizeOfObjectArray(1));
        }
        if (maxValues.isPresent()) {
            totalSize += estimatedSizeOf(maxValues.get(), CanonicalColumnName::getRetainedSize, value -> SizeOf.sizeOfObjectArray(1));
        }
        if (nullCount.isPresent()) {
            totalSize += estimatedSizeOf(nullCount.get(), CanonicalColumnName::getRetainedSize, value -> SizeOf.sizeOfLongArray(1));
        }
        return totalSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaLakeJsonFileStatistics that = (DeltaLakeJsonFileStatistics) o;
        return Objects.equals(numRecords, that.numRecords) &&
                Objects.equals(minValues, that.minValues) &&
                Objects.equals(maxValues, that.maxValues) &&
                Objects.equals(nullCount, that.nullCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(numRecords, minValues, maxValues, nullCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numRecords", numRecords)
                .add("minValues", minValues)
                .add("maxValues", maxValues)
                .add("nullCount", nullCount)
                .toString();
    }
}
