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
package io.trino.plugin.base.statistics;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.Block;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.base.statistics.ReduceOperator.ADD;
import static io.trino.plugin.base.statistics.ReduceOperator.MAX;
import static io.trino.plugin.base.statistics.ReduceOperator.MIN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class Statistics
{
    private Statistics() {}

    public static Map<String, Map<ColumnStatisticType, Block>> createColumnToComputedStatisticsMap(Map<ColumnStatisticMetadata, Block> computedStatistics)
    {
        Map<String, Map<ColumnStatisticType, Block>> result = new HashMap<>();
        computedStatistics.forEach((metadata, block) -> {
            Map<ColumnStatisticType, Block> columnStatistics = result.computeIfAbsent(metadata.getColumnName(), key -> new HashMap<>());
            columnStatistics.put(metadata.getStatisticType(), block);
        });
        return result.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
    }

    public static Optional<IntegerStatistics> mergeIntegerStatistics(Optional<IntegerStatistics> first, Optional<IntegerStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new IntegerStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    public static Optional<DoubleStatistics> mergeDoubleStatistics(Optional<DoubleStatistics> first, Optional<DoubleStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DoubleStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    public static Optional<DecimalStatistics> mergeDecimalStatistics(Optional<DecimalStatistics> first, Optional<DecimalStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DecimalStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    public static Optional<DateStatistics> mergeDateStatistics(Optional<DateStatistics> first, Optional<DateStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new DateStatistics(
                    reduce(first.get().getMin(), second.get().getMin(), MIN, true),
                    reduce(first.get().getMax(), second.get().getMax(), MAX, true)));
        }
        return Optional.empty();
    }

    public static Optional<BooleanStatistics> mergeBooleanStatistics(Optional<BooleanStatistics> first, Optional<BooleanStatistics> second)
    {
        // normally, either both or none is present
        if (first.isPresent() && second.isPresent()) {
            return Optional.of(new BooleanStatistics(
                    reduce(first.get().getTrueCount(), second.get().getTrueCount(), ADD, false),
                    reduce(first.get().getFalseCount(), second.get().getFalseCount(), ADD, false)));
        }
        return Optional.empty();
    }

    public static OptionalLong reduce(OptionalLong first, OptionalLong second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalLong.of(first.getAsLong() + second.getAsLong());
                case SUBTRACT:
                    return OptionalLong.of(first.getAsLong() - second.getAsLong());
                case MAX:
                    return OptionalLong.of(max(first.getAsLong(), second.getAsLong()));
                case MIN:
                    return OptionalLong.of(min(first.getAsLong(), second.getAsLong()));
            }
            throw new IllegalArgumentException("Unexpected operator: " + operator);
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalLong.empty();
    }

    private static OptionalDouble reduce(OptionalDouble first, OptionalDouble second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                    return OptionalDouble.of(first.getAsDouble() + second.getAsDouble());
                case SUBTRACT:
                    return OptionalDouble.of(first.getAsDouble() - second.getAsDouble());
                case MAX:
                    return OptionalDouble.of(max(first.getAsDouble(), second.getAsDouble()));
                case MIN:
                    return OptionalDouble.of(min(first.getAsDouble(), second.getAsDouble()));
            }
            throw new IllegalArgumentException("Unexpected operator: " + operator);
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return OptionalDouble.empty();
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<? super T>> Optional<T> reduce(Optional<T> first, Optional<T> second, ReduceOperator operator, boolean returnFirstNonEmpty)
    {
        if (first.isPresent() && second.isPresent()) {
            switch (operator) {
                case ADD:
                case SUBTRACT:
                    // unsupported
                    break;
                case MAX:
                    return Optional.of(max(first.get(), second.get()));
                case MIN:
                    return Optional.of(min(first.get(), second.get()));
            }
            throw new IllegalArgumentException("Unexpected operator: " + operator);
        }
        if (returnFirstNonEmpty) {
            return first.isPresent() ? first : second;
        }
        return Optional.empty();
    }

    private static <T extends Comparable<? super T>> T max(T first, T second)
    {
        return first.compareTo(second) >= 0 ? first : second;
    }

    private static <T extends Comparable<? super T>> T min(T first, T second)
    {
        return first.compareTo(second) <= 0 ? first : second;
    }

    public static OptionalLong getIntegerValue(Type type, Block block)
    {
        verify(type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(type.getLong(block, 0));
    }

    public static OptionalDouble getDoubleValue(Type type, Block block)
    {
        verify(type == DOUBLE || type == REAL, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return OptionalDouble.empty();
        }
        double value;
        if (type == DOUBLE) {
            value = type.getDouble(block, 0);
        }
        else {
            verify(type == REAL);
            value = intBitsToFloat(toIntExact(type.getLong(block, 0)));
        }
        if (!Double.isFinite(value)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(value);
    }

    public static Optional<LocalDate> getDateValue(Type type, Block block)
    {
        verify(type == DATE, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return Optional.empty();
        }
        int days = toIntExact(type.getLong(block, 0));
        return Optional.of(LocalDate.ofEpochDay(days));
    }

    public static Optional<BigDecimal> getDecimalValue(Type type, Block block)
    {
        verify(type instanceof DecimalType, "Unsupported type: %s", type);
        if (block.isNull(0)) {
            return Optional.empty();
        }
        return Optional.of(Decimals.readBigDecimal((DecimalType) type, block, 0));
    }
}
