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

import io.airlift.log.Logger;
import io.airlift.slice.SizeOf;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.CanonicalColumnName;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.toCanonicalNameKeyedMap;

public class DeltaLakeParquetFileStatistics
        implements DeltaLakeFileStatistics
{
    private static final Logger log = Logger.get(DeltaLakeParquetFileStatistics.class);
    private static final long INSTANCE_SIZE = instanceSize(DeltaLakeParquetFileStatistics.class);

    private final Optional<Long> numRecords;
    private final Optional<Map<CanonicalColumnName, Object>> minValues;
    private final Optional<Map<CanonicalColumnName, Object>> maxValues;
    private final Optional<Map<CanonicalColumnName, Object>> nullCount;

    public DeltaLakeParquetFileStatistics(
            Optional<Long> numRecords,
            Optional<Map<String, Object>> minValues,
            Optional<Map<String, Object>> maxValues,
            Optional<Map<String, Object>> nullCount)
    {
        this.numRecords = numRecords;
        // Re-use CanonicalColumnName for min/max/null maps to benefit from cached hashCode
        Map<String, CanonicalColumnName> canonicalColumnNames = DeltaLakeFileStatistics.getCanonicalColumnNames(minValues, maxValues, nullCount);
        this.minValues = minValues.map(minValuesMap -> toCanonicalNameKeyedMap(minValuesMap, canonicalColumnNames));
        this.maxValues = maxValues.map(maxValuesMap -> toCanonicalNameKeyedMap(maxValuesMap, canonicalColumnNames));
        this.nullCount = nullCount.map(nullCountMap -> toCanonicalNameKeyedMap(nullCountMap, canonicalColumnNames));
    }

    @Override
    public Optional<Long> getNumRecords()
    {
        return numRecords;
    }

    @Override
    public Optional<Map<String, Object>> getMinValues()
    {
        return minValues.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @Override
    public Optional<Map<String, Object>> getMaxValues()
    {
        return maxValues.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @Override
    public Optional<Map<String, Object>> getNullCount()
    {
        return nullCount.map(TransactionLogAccess::toOriginalNameKeyedMap);
    }

    @Override
    public Optional<Object> getMaxColumnValue(DeltaLakeColumnHandle columnHandle)
    {
        return getStat(columnHandle.getPhysicalName(), maxValues);
    }

    @Override
    public Optional<Object> getMinColumnValue(DeltaLakeColumnHandle columnHandle)
    {
        return getStat(columnHandle.getPhysicalName(), minValues);
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
        if (contents instanceof List || contents instanceof Map || contents instanceof Block) {
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
            totalSize += estimatedSizeOf(minValues.get(), CanonicalColumnName::getRetainedSize, DeltaLakeParquetFileStatistics::sizeOfMinMaxStatsEntry);
        }
        if (maxValues.isPresent()) {
            totalSize += estimatedSizeOf(maxValues.get(), CanonicalColumnName::getRetainedSize, DeltaLakeParquetFileStatistics::sizeOfMinMaxStatsEntry);
        }
        if (nullCount.isPresent()) {
            totalSize += estimatedSizeOf(nullCount.get(), CanonicalColumnName::getRetainedSize, DeltaLakeParquetFileStatistics::sizeOfNullCountStatsEntry);
        }
        return totalSize;
    }

    private static long sizeOfMinMaxStatsEntry(Object value)
    {
        if (value instanceof Block) {
            return ((Block) value).getRetainedSizeInBytes();
        }

        return SizeOf.sizeOfObjectArray(1);
    }

    private static long sizeOfNullCountStatsEntry(Object value)
    {
        if (value instanceof Block) {
            return ((Block) value).getRetainedSizeInBytes();
        }

        return SizeOf.sizeOfLongArray(1);
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
        DeltaLakeParquetFileStatistics that = (DeltaLakeParquetFileStatistics) o;
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
