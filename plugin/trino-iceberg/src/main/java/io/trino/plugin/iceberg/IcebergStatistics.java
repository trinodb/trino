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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public record IcebergStatistics(
        long recordCount,
        long fileCount,
        long size,
        Map<Integer, Object> minValues,
        Map<Integer, Object> maxValues,
        Map<Integer, Long> nullCounts,
        Map<Integer, Long> nanCounts,
        Map<Integer, Long> columnSizes)
{
    public IcebergStatistics
    {
        minValues = ImmutableMap.copyOf(requireNonNull(minValues, "minValues is null"));
        maxValues = ImmutableMap.copyOf(requireNonNull(maxValues, "maxValues is null"));
        nullCounts = ImmutableMap.copyOf(requireNonNull(nullCounts, "nullCounts is null"));
        nanCounts = ImmutableMap.copyOf(requireNonNull(nanCounts, "nanCounts is null"));
        columnSizes = ImmutableMap.copyOf(requireNonNull(columnSizes, "columnSizes is null"));
    }

    public static class Builder
    {
        private final TypeManager typeManager;
        private final List<Types.NestedField> columns;

        private long recordCount;
        private long fileCount;
        private long size;
        private final Map<Integer, ColumnStatistics> columnStatistics = new HashMap<>();
        private final Map<Integer, Long> nullCounts = new HashMap<>();
        private final Map<Integer, Long> nanCounts = new HashMap<>();
        private final Map<Integer, Long> columnSizes = new HashMap<>();

        public Builder(
                List<Types.NestedField> columns,
                TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.columns = ImmutableList.copyOf(columns);
        }

        public void acceptDataFile(DataFile dataFile, PartitionSpec partitionSpec)
        {
            fileCount++;
            recordCount += dataFile.recordCount();
            size += dataFile.fileSizeInBytes();

            mergeLongStatistics(columnSizes, dataFile.columnSizes());
            mergeLongStatistics(nanCounts, dataFile.nanValueCounts());
            Map<Integer, Long> nullValueCounts = dataFile.nullValueCounts();
            mergeLongStatistics(nullCounts, nullValueCounts);

            Map<Integer, Optional<String>> identityPartitionValues = getPartitionKeys(dataFile.partition(), partitionSpec);
            for (Types.NestedField column : columns) {
                int id = column.fieldId();
                io.trino.spi.type.Type trinoType = toTrinoType(column.type(), typeManager);
                Optional<String> partitionValue = identityPartitionValues.get(id);
                if (partitionValue != null) {
                    if (partitionValue.isPresent()) {
                        Object trinoValue = deserializePartitionValue(trinoType, partitionValue.get(), column.name());
                        // Update min/max stats but there are no null values to count
                        updateMinMaxStats(
                                id,
                                trinoType,
                                trinoValue,
                                trinoValue,
                                OptionalLong.of(0),
                                dataFile.recordCount());
                    }
                    else {
                        // Update null counts, but do not clear min/max
                        if (nullValueCounts == null || !nullValueCounts.containsKey(id)) {
                            nullCounts.merge(id, dataFile.recordCount(), Long::sum);
                        }
                    }
                }
                else {
                    Object lowerBound = convertIcebergValueToTrino(column.type(),
                            Conversions.fromByteBuffer(column.type(), Optional.ofNullable(dataFile.lowerBounds()).map(a -> a.get(id)).orElse(null)));
                    Object upperBound = convertIcebergValueToTrino(column.type(),
                            Conversions.fromByteBuffer(column.type(), Optional.ofNullable(dataFile.upperBounds()).map(a -> a.get(id)).orElse(null)));
                    OptionalLong nullCount = (nullValueCounts == null || !nullValueCounts.containsKey(id)) ? OptionalLong.empty() : OptionalLong.of(nullValueCounts.get(id));
                    updateMinMaxStats(
                            id,
                            trinoType,
                            lowerBound,
                            upperBound,
                            nullCount,
                            dataFile.recordCount());
                }
            }
        }

        public IcebergStatistics build()
        {
            ImmutableMap.Builder<Integer, Object> minValues = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, Object> maxValues = ImmutableMap.builder();

            columnStatistics.forEach((fieldId, statistics) -> {
                statistics.getMin().ifPresent(min -> minValues.put(fieldId, min));
                statistics.getMax().ifPresent(max -> maxValues.put(fieldId, max));
            });

            return new IcebergStatistics(
                    recordCount,
                    fileCount,
                    size,
                    minValues.buildOrThrow(),
                    maxValues.buildOrThrow(),
                    ImmutableMap.copyOf(nullCounts),
                    ImmutableMap.copyOf(nanCounts),
                    ImmutableMap.copyOf(columnSizes));
        }

        private void updateMinMaxStats(
                int id,
                io.trino.spi.type.Type type,
                @Nullable Object lowerBound,
                @Nullable Object upperBound,
                OptionalLong nullCount,
                long recordCount)
        {
            // If this column is only nulls for this file, don't update or invalidate min/max statistics
            if (type.isOrderable() && (nullCount.isEmpty() || nullCount.getAsLong() != recordCount)) {
                // Capture the initial bounds during construction so there are always valid min/max values to compare to. This does make the first call to
                // `ColumnStatistics#updateMinMax` a no-op.
                columnStatistics.computeIfAbsent(id, _ -> {
                    MethodHandle comparisonHandle = typeManager.getTypeOperators()
                            .getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                    return new ColumnStatistics(comparisonHandle, lowerBound, upperBound);
                }).updateMinMax(lowerBound, upperBound);
            }
        }

        private static void mergeLongStatistics(Map<Integer, Long> accumulated, Map<Integer, Long> fileStatistics)
        {
            if (fileStatistics == null) {
                return;
            }
            for (Entry<Integer, Long> entry : fileStatistics.entrySet()) {
                Long value = entry.getValue();
                if (value != null) {
                    Integer fieldId = entry.getKey();
                    accumulated.merge(fieldId, value, Long::sum);
                }
            }
        }
    }

    private static class ColumnStatistics
    {
        private final MethodHandle comparisonHandle;

        private Optional<Object> min;
        private Optional<Object> max;

        public ColumnStatistics(MethodHandle comparisonHandle, Object initialMin, Object initialMax)
        {
            this.comparisonHandle = requireNonNull(comparisonHandle, "comparisonHandle is null");
            this.min = Optional.ofNullable(initialMin);
            this.max = Optional.ofNullable(initialMax);
        }

        /**
         * Gets the minimum value accumulated during stats collection.
         *
         * @return Empty if the statistics contained values which were not comparable, otherwise returns the min value.
         */
        public Optional<Object> getMin()
        {
            return min;
        }

        /**
         * Gets the maximum value accumulated during stats collection.
         *
         * @return Empty if the statistics contained values which were not comparable, otherwise returns the max value.
         */
        public Optional<Object> getMax()
        {
            return max;
        }

        /**
         * Update the stats, as long as they haven't already been invalidated
         *
         * @param lowerBound Trino encoded lower bound value from a file
         * @param upperBound Trino encoded upper bound value from a file
         */
        public void updateMinMax(Object lowerBound, Object upperBound)
        {
            if (min.isPresent()) {
                if (lowerBound == null) {
                    min = Optional.empty();
                }
                else if (compareTrinoValue(lowerBound, min.get()) < 0) {
                    min = Optional.of(lowerBound);
                }
            }

            if (max.isPresent()) {
                if (upperBound == null) {
                    max = Optional.empty();
                }
                else if (compareTrinoValue(upperBound, max.get()) > 0) {
                    max = Optional.of(upperBound);
                }
            }
        }

        private long compareTrinoValue(Object value, Object otherValue)
        {
            try {
                return (Long) comparisonHandle.invoke(value, otherValue);
            }
            catch (Throwable throwable) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unable to compare Iceberg min/max values", throwable);
            }
        }
    }
}
