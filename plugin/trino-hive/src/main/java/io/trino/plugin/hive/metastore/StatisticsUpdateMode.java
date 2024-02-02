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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.PartitionStatistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.intersection;

public enum StatisticsUpdateMode
{
    /**
     * Remove all existing table and column statistics, and replace them with the new statistics.
     */
    OVERWRITE_ALL {
        @Override
        public PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
        {
            return newPartitionStats;
        }
    },
    /**
     * Replace table statistics and present columns statics, but retain the statistics for columns that are not present in the new statistics.
     */
    OVERWRITE_SOME_COLUMNS {
        @Override
        public PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
        {
            Map<String, HiveColumnStatistics> allColumnStatists = new HashMap<>(oldPartitionStats.getColumnStatistics());
            allColumnStatists.putAll(newPartitionStats.getColumnStatistics());

            return new PartitionStatistics(newPartitionStats.getBasicStatistics(), allColumnStatists);
        }
    },
    /**
     * Merges the new incremental data from an INSERT operation into the existing statistics.
     */
    MERGE_INCREMENTAL {
        @Override
        public PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
        {
            return addIncrementalStatistics(oldPartitionStats, newPartitionStats);
        }
    },
    /**
     * Undo the effect of a previous MERGE_INCREMENTAL operation. This will clear all column statistics because min/max calculations are not reversible.
     */
    UNDO_MERGE_INCREMENTAL {
        @Override
        public PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
        {
            HiveBasicStatistics newTableStatistics = reduce(oldPartitionStats.getBasicStatistics(), newPartitionStats.getBasicStatistics(), Operator.SUBTRACT);
            return new PartitionStatistics(newTableStatistics, ImmutableMap.of());
        }
    },
    /**
     * Removes all statics from the table and columns.
     */
    CLEAR_ALL {
        @Override
        public PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats)
        {
            return PartitionStatistics.empty();
        }
    };

    public abstract PartitionStatistics updatePartitionStatistics(PartitionStatistics oldPartitionStats, PartitionStatistics newPartitionStats);

    private static PartitionStatistics addIncrementalStatistics(PartitionStatistics existingStatistics, PartitionStatistics incrementalStatistics)
    {
        if (existingStatistics.getBasicStatistics().getRowCount().isPresent() && existingStatistics.getBasicStatistics().getRowCount().getAsLong() == 0) {
            return incrementalStatistics;
        }

        if (incrementalStatistics.getBasicStatistics().getRowCount().isPresent() && incrementalStatistics.getBasicStatistics().getRowCount().getAsLong() == 0) {
            return existingStatistics;
        }

        var mergedTableStatistics = reduce(existingStatistics.getBasicStatistics(), incrementalStatistics.getBasicStatistics(), Operator.ADD);

        // only keep columns that have statistics in old and new
        var existingColumnStatistics = existingStatistics.getColumnStatistics();
        var incrementalColumnStatistics = incrementalStatistics.getColumnStatistics();
        var mergedColumnStatistics = intersection(existingColumnStatistics.keySet(), incrementalColumnStatistics.keySet()).stream()
                .collect(toImmutableMap(
                        column -> column,
                        column -> merge(existingColumnStatistics.get(column), incrementalColumnStatistics.get(column))));

        return new PartitionStatistics(mergedTableStatistics, mergedColumnStatistics);
    }

    private static HiveBasicStatistics reduce(HiveBasicStatistics first, HiveBasicStatistics second, Operator operator)
    {
        return new HiveBasicStatistics(
                reduce(first.getFileCount(), second.getFileCount(), operator, false),
                reduce(first.getRowCount(), second.getRowCount(), operator, false),
                reduce(first.getInMemoryDataSizeInBytes(), second.getInMemoryDataSizeInBytes(), operator, false),
                reduce(first.getOnDiskDataSizeInBytes(), second.getOnDiskDataSizeInBytes(), operator, false));
    }

    private static HiveColumnStatistics merge(HiveColumnStatistics first, HiveColumnStatistics second)
    {
        return new HiveColumnStatistics(
                mergeIntegerStatistics(first.getIntegerStatistics(), second.getIntegerStatistics()),
                mergeDoubleStatistics(first.getDoubleStatistics(), second.getDoubleStatistics()),
                mergeDecimalStatistics(first.getDecimalStatistics(), second.getDecimalStatistics()),
                mergeDateStatistics(first.getDateStatistics(), second.getDateStatistics()),
                mergeBooleanStatistics(first.getBooleanStatistics(), second.getBooleanStatistics()),
                reduce(first.getMaxValueSizeInBytes(), second.getMaxValueSizeInBytes(), Operator.MAX, true),
                reduce(first.getTotalSizeInBytes(), second.getTotalSizeInBytes(), Operator.ADD, true),
                reduce(first.getNullsCount(), second.getNullsCount(), Operator.ADD, false),
                reduce(first.getDistinctValuesCount(), second.getDistinctValuesCount(), Operator.MAX, false));
    }

    private static Optional<IntegerStatistics> mergeIntegerStatistics(Optional<IntegerStatistics> first, Optional<IntegerStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new IntegerStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), Operator.MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), Operator.MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<DoubleStatistics> mergeDoubleStatistics(Optional<DoubleStatistics> first, Optional<DoubleStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DoubleStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), Operator.MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), Operator.MAX, true)));
        }
        return Optional.empty();
    }

    private static Optional<DecimalStatistics> mergeDecimalStatistics(Optional<DecimalStatistics> first, Optional<DecimalStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DecimalStatistics(
                    mergeComparable(first.get().getMin(), second.get().getMin(), Operator.MIN),
                    mergeComparable(first.get().getMax(), second.get().getMax(), Operator.MAX)));
        }
        return Optional.empty();
    }

    private static Optional<DateStatistics> mergeDateStatistics(Optional<DateStatistics> first, Optional<DateStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DateStatistics(
                    mergeComparable(first.get().getMin(), second.get().getMin(), Operator.MIN),
                    mergeComparable(first.get().getMax(), second.get().getMax(), Operator.MAX)));
        }
        return Optional.empty();
    }

    private static Optional<BooleanStatistics> mergeBooleanStatistics(Optional<BooleanStatistics> first, Optional<BooleanStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new BooleanStatistics(
                    reduce(first.get().getTrueCount(), second.get().getTrueCount(), Operator.ADD, false),
                    reduce(first.get().getFalseCount(), second.get().getFalseCount(), Operator.ADD, false)));
        }
        return Optional.empty();
    }

    private static OptionalLong reduce(OptionalLong first, OptionalLong second, Operator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            return switch (operator) {
                case ADD -> OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT -> OptionalLong.of(first.getAsLong() - second.getAsLong());
                case MAX -> OptionalLong.of(max(first.getAsLong(), second.getAsLong()));
                case MIN -> OptionalLong.of(min(first.getAsLong(), second.getAsLong()));
            };
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalLong.empty();
    }

    private static OptionalDouble reduce(OptionalDouble first, OptionalDouble second, Operator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            return switch (operator) {
                case ADD -> OptionalDouble.of(first.getAsDouble() + second.getAsDouble());
                case SUBTRACT -> OptionalDouble.of(first.getAsDouble() - second.getAsDouble());
                case MAX -> OptionalDouble.of(max(first.getAsDouble(), second.getAsDouble()));
                case MIN -> OptionalDouble.of(min(first.getAsDouble(), second.getAsDouble()));
            };
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalDouble.empty();
    }

    private static <T extends Comparable<? super T>> Optional<T> mergeComparable(Optional<T> first, Optional<T> second, Operator operator)
    {
        if (first.isPresent() && second.isPresent()) {
            return switch (operator) {
                case MAX -> Optional.of(max(first.get(), second.get()));
                case MIN -> Optional.of(min(first.get(), second.get()));
                default -> throw new IllegalArgumentException("Unexpected operator: " + operator);
            };
        }
        return first.isPresent() ? first : second;
    }

    private static <T extends Comparable<? super T>> T max(T first, T second)
    {
        return first.compareTo(second) >= 0 ? first : second;
    }

    private static <T extends Comparable<? super T>> T min(T first, T second)
    {
        return first.compareTo(second) <= 0 ? first : second;
    }

    private enum Operator
    {
        ADD,
        SUBTRACT,
        MIN,
        MAX,
    }
}
