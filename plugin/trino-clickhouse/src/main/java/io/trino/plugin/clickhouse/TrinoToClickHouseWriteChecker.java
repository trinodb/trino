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
package io.trino.plugin.clickhouse;

import com.clickhouse.data.ClickHouseVersion;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoToClickHouseWriteChecker<T>
{
    // Different versions of ClickHouse may support different min/max values for the
    // same data type, you can refer to the table below:
    //
    // | version | column type | min value           | max value            |
    // |---------|-------------|---------------------|----------------------|
    // | any     | UInt8       | 0                   | 255                  |
    // | any     | UInt16      | 0                   | 65535                |
    // | any     | UInt32      | 0                   | 4294967295           |
    // | any     | UInt64      | 0                   | 18446744073709551615 |
    // | < 21.4  | Date        | 1970-01-01          | 2106-02-07           |
    // | < 21.4  | DateTime    | 1970-01-01 00:00:00 | 2106-02-06 06:28:15  |
    // | >= 21.4 | Date        | 1970-01-01          | 2149-06-06           |
    // | >= 21.4 | DateTime    | 1970-01-01 00:00:00 | 2106-02-07 06:28:15  |
    //
    // And when the value written to ClickHouse is out of range, ClickHouse will store
    // the incorrect result, so we need to check the range of the written value to
    // prevent ClickHouse from storing the incorrect value.

    public static final TrinoToClickHouseWriteChecker<Long> UINT8 = new TrinoToClickHouseWriteChecker<>(ImmutableList.of(new LongWriteValueChecker(alwaysTrue(), new Range<>(0L, 255L))));
    public static final TrinoToClickHouseWriteChecker<Long> UINT16 = new TrinoToClickHouseWriteChecker<>(ImmutableList.of(new LongWriteValueChecker(alwaysTrue(), new Range<>(0L, 65535L))));
    public static final TrinoToClickHouseWriteChecker<Long> UINT32 = new TrinoToClickHouseWriteChecker<>(ImmutableList.of(new LongWriteValueChecker(alwaysTrue(), new Range<>(0L, 4294967295L))));
    public static final TrinoToClickHouseWriteChecker<BigDecimal> UINT64 = new TrinoToClickHouseWriteChecker<>(
            ImmutableList.of(new BigDecimalWriteValueChecker(alwaysTrue(), new Range<>(BigDecimal.ZERO, new BigDecimal("18446744073709551615")))));
    public static final TrinoToClickHouseWriteChecker<LocalDate> DATE = new TrinoToClickHouseWriteChecker<>(
            ImmutableList.of(
                    new DateWriteValueChecker(version -> version.isOlderThan("21.4"), new Range<>(LocalDate.parse("1970-01-01"), LocalDate.parse("2106-02-07"))),
                    new DateWriteValueChecker(version -> version.isNewerOrEqualTo("21.4"), new Range<>(LocalDate.parse("1970-01-01"), LocalDate.parse("2149-06-06")))));
    public static final TrinoToClickHouseWriteChecker<LocalDateTime> DATETIME = new TrinoToClickHouseWriteChecker<>(
            ImmutableList.of(
                    new TimestampWriteValueChecker(
                            version -> version.isOlderThan("21.4"),
                            new Range<>(LocalDateTime.parse("1970-01-01T00:00:00"), LocalDateTime.parse("2106-02-06T06:28:15"))),
                    new TimestampWriteValueChecker(
                            version -> version.isNewerOrEqualTo("21.4"),
                            new Range<>(LocalDateTime.parse("1970-01-01T00:00:00"), LocalDateTime.parse("2106-02-07T06:28:15")))));

    private final List<Checker<T>> checkers;

    private TrinoToClickHouseWriteChecker(List<Checker<T>> checkers)
    {
        this.checkers = ImmutableList.copyOf(requireNonNull(checkers, "checkers is null"));
    }

    public void validate(ClickHouseVersion version, T value)
    {
        for (Checker<T> checker : checkers) {
            checker.validate(version, value);
        }
    }

    private interface Checker<T>
    {
        void validate(ClickHouseVersion version, T value);
    }

    private static class LongWriteValueChecker
            implements Checker<Long>
    {
        private final Predicate<ClickHouseVersion> predicate;
        private final Range<Long> range;

        public LongWriteValueChecker(Predicate<ClickHouseVersion> predicate, Range<Long> range)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(ClickHouseVersion version, Long value)
        {
            if (!predicate.test(version)) {
                return;
            }

            if (value >= range.getMin() && value <= range.getMax()) {
                return;
            }

            throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %d and %d in ClickHouse: %d", range.getMin(), range.getMax(), value));
        }
    }

    private static class BigDecimalWriteValueChecker
            implements Checker<BigDecimal>
    {
        private final Predicate<ClickHouseVersion> predicate;
        private final Range<BigDecimal> range;

        public BigDecimalWriteValueChecker(Predicate<ClickHouseVersion> predicate, Range<BigDecimal> range)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(ClickHouseVersion version, BigDecimal value)
        {
            if (!predicate.test(version)) {
                return;
            }

            if (value.compareTo(range.getMin()) >= 0 && value.compareTo(range.getMax()) <= 0) {
                return;
            }

            throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", range.getMin(), range.getMax(), value));
        }
    }

    private static class DateWriteValueChecker
            implements Checker<LocalDate>
    {
        private final Predicate<ClickHouseVersion> predicate;
        private final Range<LocalDate> range;

        public DateWriteValueChecker(Predicate<ClickHouseVersion> predicate, Range<LocalDate> range)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(ClickHouseVersion version, LocalDate value)
        {
            if (!predicate.test(version)) {
                return;
            }

            if (value.isBefore(range.getMin()) || value.isAfter(range.getMax())) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Date must be between %s and %s in ClickHouse: %s", range.getMin(), range.getMax(), value));
            }
        }
    }

    private static class TimestampWriteValueChecker
            implements Checker<LocalDateTime>
    {
        private final Predicate<ClickHouseVersion> predicate;
        private final Range<LocalDateTime> range;

        public TimestampWriteValueChecker(Predicate<ClickHouseVersion> predicate, Range<LocalDateTime> range)
        {
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(ClickHouseVersion version, LocalDateTime value)
        {
            if (!predicate.test(version)) {
                return;
            }

            if (value.isBefore(range.getMin()) || value.isAfter(range.getMax())) {
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                        .appendPattern("uuuu-MM-dd HH:mm:ss")
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                        .toFormatter();
                throw new TrinoException(
                        INVALID_ARGUMENTS,
                        format("Timestamp must be between %s and %s in ClickHouse: %s", formatter.format(range.getMin()), formatter.format(range.getMax()), formatter.format(value)));
            }
        }
    }

    private static class Range<T>
    {
        private final T min;
        private final T max;

        public Range(T min, T max)
        {
            this.min = requireNonNull(min, "min is null");
            this.max = requireNonNull(max, "max is null");
        }

        public T getMin()
        {
            return min;
        }

        public T getMax()
        {
            return max;
        }
    }
}
