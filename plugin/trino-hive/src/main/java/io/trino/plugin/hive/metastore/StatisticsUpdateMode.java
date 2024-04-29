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
            Map<String, HiveColumnStatistics> allColumnStatists = new HashMap<>(oldPartitionStats.columnStatistics());
            allColumnStatists.putAll(newPartitionStats.columnStatistics());

            return new PartitionStatistics(newPartitionStats.basicStatistics(), allColumnStatists);
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
            HiveBasicStatistics newTableStatistics = reduce(oldPartitionStats.basicStatistics(), newPartitionStats.basicStatistics(), Operator.SUBTRACT);
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
        if (existingStatistics.basicStatistics().getRowCount().isPresent() && existingStatistics.basicStatistics().getRowCount().getAsLong() == 0) {
            return incrementalStatistics;
        }

        if (incrementalStatistics.basicStatistics().getRowCount().isPresent() && incrementalStatistics.basicStatistics().getRowCount().getAsLong() == 0) {
            return existingStatistics;
        }

        var mergedTableStatistics = reduce(existingStatistics.basicStatistics(), incrementalStatistics.basicStatistics(), Operator.ADD);

        // only keep columns that have statistics in old and new
        var mergedColumnStatistics = intersection(existingStatistics.columnStatistics().keySet(), incrementalStatistics.columnStatistics().keySet()).stream()
                .collect(toImmutableMap(
                        column -> column,
                        column -> merge(column, existingStatistics, incrementalStatistics)));

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

    private static HiveColumnStatistics merge(String column, PartitionStatistics firstStats, PartitionStatistics secondStats)
    {
        HiveColumnStatistics first = firstStats.columnStatistics().get(column);
        HiveColumnStatistics second = secondStats.columnStatistics().get(column);

        return new HiveColumnStatistics(
                mergeIntegerStatistics(first.getIntegerStatistics(), second.getIntegerStatistics()),
                mergeDoubleStatistics(first.getDoubleStatistics(), second.getDoubleStatistics()),
                mergeDecimalStatistics(first.getDecimalStatistics(), second.getDecimalStatistics()),
                mergeDateStatistics(first.getDateStatistics(), second.getDateStatistics()),
                mergeBooleanStatistics(first.getBooleanStatistics(), second.getBooleanStatistics()),
                reduce(first.getMaxValueSizeInBytes(), second.getMaxValueSizeInBytes(), Operator.MAX, true),
                mergeAverageColumnLength(column, firstStats, secondStats),
                reduce(first.getNullsCount(), second.getNullsCount(), Operator.ADD, false),
                mergeDistinctValueCount(column, firstStats, secondStats));
    }

    private static OptionalLong mergeDistinctValueCount(String column, PartitionStatistics first, PartitionStatistics second)
    {
        HiveColumnStatistics firstColumn = first.columnStatistics().get(column);
        HiveColumnStatistics secondColumn = second.columnStatistics().get(column);

        OptionalLong firstDistinct = firstColumn.getDistinctValuesWithNullCount();
        OptionalLong secondDistinct = secondColumn.getDistinctValuesWithNullCount();

        // if one column is entirely non-null and the other is entirely null
        if (firstDistinct.isPresent() && noNulls(firstColumn) && isAllNull(second, secondColumn)) {
            return OptionalLong.of(firstDistinct.getAsLong() + 1);
        }
        if (secondDistinct.isPresent() && noNulls(secondColumn) && isAllNull(first, firstColumn)) {
            return OptionalLong.of(secondDistinct.getAsLong() + 1);
        }

        if (firstDistinct.isPresent() && secondDistinct.isPresent()) {
            return OptionalLong.of(max(firstDistinct.getAsLong(), secondDistinct.getAsLong()));
        }
        return OptionalLong.empty();
    }

    private static boolean noNulls(HiveColumnStatistics columnStats)
    {
        if (columnStats.getNullsCount().isEmpty()) {
            return false;
        }
        return columnStats.getNullsCount().orElse(-1) == 0;
    }

    private static boolean isAllNull(PartitionStatistics stats, HiveColumnStatistics columnStats)
    {
        if (stats.basicStatistics().getRowCount().isEmpty() || columnStats.getNullsCount().isEmpty()) {
            return false;
        }
        return stats.basicStatistics().getRowCount().getAsLong() == columnStats.getNullsCount().getAsLong();
    }

    private static OptionalDouble mergeAverageColumnLength(String column, PartitionStatistics first, PartitionStatistics second)
    {
        // row count is required to merge average column length
        if (first.basicStatistics().getRowCount().isEmpty() || second.basicStatistics().getRowCount().isEmpty()) {
            return OptionalDouble.empty();
        }
        long firstRowCount = first.basicStatistics().getRowCount().getAsLong();
        long secondRowCount = second.basicStatistics().getRowCount().getAsLong();

        HiveColumnStatistics firstColumn = first.columnStatistics().get(column);
        HiveColumnStatistics secondColumn = second.columnStatistics().get(column);

        // if one column is entirely null, return the average column length of the other column
        if (firstRowCount == firstColumn.getNullsCount().orElse(0)) {
            return secondColumn.getAverageColumnLength();
        }
        if (secondRowCount == secondColumn.getNullsCount().orElse(0)) {
            return firstColumn.getAverageColumnLength();
        }

        if (firstColumn.getAverageColumnLength().isEmpty() || secondColumn.getAverageColumnLength().isEmpty()) {
            return OptionalDouble.empty();
        }

        long firstNonNullRowCount = firstRowCount - firstColumn.getNullsCount().orElse(0);
        long secondNonNullRowCount = secondRowCount - secondColumn.getNullsCount().orElse(0);

        double firstTotalSize = firstColumn.getAverageColumnLength().getAsDouble() * firstNonNullRowCount;
        double secondTotalSize = secondColumn.getAverageColumnLength().getAsDouble() * secondNonNullRowCount;

        return OptionalDouble.of((firstTotalSize + secondTotalSize) / (firstNonNullRowCount + secondNonNullRowCount));
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
