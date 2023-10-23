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

import io.trino.spi.TrinoException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoToClickHouseWriteChecker<T>
{
    // | version | column type | min value           | max value            |
    // |---------|-------------|---------------------|----------------------|
    // | any     | UInt8       | 0                   | 255                  |
    // | any     | UInt16      | 0                   | 65535                |
    // | any     | UInt32      | 0                   | 4294967295           |
    // | any     | UInt64      | 0                   | 18446744073709551615 |
    // | any     | Date        | 1970-01-01          | 2149-06-06           |
    // | any     | DateTime    | 1970-01-01 00:00:00 | 2106-02-07 06:28:15  |
    //
    // When the value written to ClickHouse is out of range, ClickHouse will store
    // the incorrect result, so we need to check the range of the written value to
    // prevent ClickHouse from storing the incorrect value.

    public static final TrinoToClickHouseWriteChecker<Long> UINT8 = new TrinoToClickHouseWriteChecker<>(new LongWriteValueChecker(new Range<>(0L, 255L)));
    public static final TrinoToClickHouseWriteChecker<Long> UINT16 = new TrinoToClickHouseWriteChecker<>(new LongWriteValueChecker(new Range<>(0L, 65535L)));
    public static final TrinoToClickHouseWriteChecker<Long> UINT32 = new TrinoToClickHouseWriteChecker<>(new LongWriteValueChecker(new Range<>(0L, 4294967295L)));
    public static final TrinoToClickHouseWriteChecker<BigDecimal> UINT64 = new TrinoToClickHouseWriteChecker<>(
            new BigDecimalWriteValueChecker(new Range<>(BigDecimal.ZERO, new BigDecimal("18446744073709551615"))));
    public static final TrinoToClickHouseWriteChecker<LocalDate> DATE = new TrinoToClickHouseWriteChecker<>(
            new DateWriteValueChecker(new Range<>(LocalDate.parse("1970-01-01"), LocalDate.parse("2149-06-06"))));
    public static final TrinoToClickHouseWriteChecker<LocalDateTime> DATETIME = new TrinoToClickHouseWriteChecker<>(
            new TimestampWriteValueChecker(new Range<>(LocalDateTime.parse("1970-01-01T00:00:00"), LocalDateTime.parse("2106-02-07T06:28:15"))));

    private final Checker<T> checker;

    private TrinoToClickHouseWriteChecker(Checker<T> checker)
    {
        this.checker = requireNonNull(checker, "checker is null");
    }

    public void validate(T value)
    {
        checker.validate(value);
    }

    private interface Checker<T>
    {
        void validate(T value);
    }

    private static class LongWriteValueChecker
            implements Checker<Long>
    {
        private final Range<Long> range;

        public LongWriteValueChecker(Range<Long> range)
        {
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(Long value)
        {
            if (value >= range.getMin() && value <= range.getMax()) {
                return;
            }

            throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %d and %d in ClickHouse: %d", range.getMin(), range.getMax(), value));
        }
    }

    private static class BigDecimalWriteValueChecker
            implements Checker<BigDecimal>
    {
        private final Range<BigDecimal> range;

        public BigDecimalWriteValueChecker(Range<BigDecimal> range)
        {
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(BigDecimal value)
        {
            if (value.compareTo(range.getMin()) >= 0 && value.compareTo(range.getMax()) <= 0) {
                return;
            }

            throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", range.getMin(), range.getMax(), value));
        }
    }

    private static class DateWriteValueChecker
            implements Checker<LocalDate>
    {
        private final Range<LocalDate> range;

        public DateWriteValueChecker(Range<LocalDate> range)
        {
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(LocalDate value)
        {
            if (value.isBefore(range.getMin()) || value.isAfter(range.getMax())) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Date must be between %s and %s in ClickHouse: %s", range.getMin(), range.getMax(), value));
            }
        }
    }

    private static class TimestampWriteValueChecker
            implements Checker<LocalDateTime>
    {
        private final Range<LocalDateTime> range;

        public TimestampWriteValueChecker(Range<LocalDateTime> range)
        {
            this.range = requireNonNull(range, "range is null");
        }

        @Override
        public void validate(LocalDateTime value)
        {
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
