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
package io.trino.orc;

import io.airlift.slice.Slice;
import io.trino.orc.metadata.statistics.BinaryStatistics;
import io.trino.orc.metadata.statistics.BooleanStatistics;
import io.trino.orc.metadata.statistics.ColumnStatistics;
import io.trino.orc.metadata.statistics.DateStatistics;
import io.trino.orc.metadata.statistics.DecimalStatistics;
import io.trino.orc.metadata.statistics.DoubleStatistics;
import io.trino.orc.metadata.statistics.IntegerStatistics;
import io.trino.orc.metadata.statistics.StringStatistics;
import io.trino.orc.metadata.statistics.TimestampStatistics;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.orc.TupleDomainOrcPredicate.getDomain;
import static io.trino.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static io.trino.spi.predicate.Domain.all;
import static io.trino.spi.predicate.Domain.create;
import static io.trino.spi.predicate.Domain.none;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThanOrEqual;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTupleDomainOrcPredicate
{
    private static final Type SHORT_DECIMAL = createDecimalType(5, 2);
    private static final Type LONG_DECIMAL = createDecimalType(20, 10);
    private static final Type CHAR = createCharType(10);

    @Test
    public void testBoolean()
    {
        assertThat(getDomain(BOOLEAN, 0, null)).isEqualTo(none(BOOLEAN));
        assertThat(getDomain(BOOLEAN, 10, null)).isEqualTo(all(BOOLEAN));

        assertThat(getDomain(BOOLEAN, 0, booleanColumnStats(null, null))).isEqualTo(none(BOOLEAN));
        assertThat(getDomain(BOOLEAN, 0, booleanColumnStats(0L, null))).isEqualTo(none(BOOLEAN));
        assertThat(getDomain(BOOLEAN, 0, booleanColumnStats(0L, 0L))).isEqualTo(none(BOOLEAN));

        assertThat(getDomain(BOOLEAN, 10, booleanColumnStats(0L, 0L))).isEqualTo(onlyNull(BOOLEAN));
        assertThat(getDomain(BOOLEAN, 10, booleanColumnStats(10L, null))).isEqualTo(notNull(BOOLEAN));

        assertThat(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 10L))).isEqualTo(singleValue(BOOLEAN, true));
        assertThat(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 0L))).isEqualTo(singleValue(BOOLEAN, false));

        assertThat(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 5L))).isEqualTo(all(BOOLEAN));

        assertThat(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 10L))).isEqualTo(create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), true));
        assertThat(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 0L))).isEqualTo(create(ValueSet.ofRanges(Range.equal(BOOLEAN, false)), true));
    }

    private static ColumnStatistics booleanColumnStats(Long numberOfValues, Long trueValueCount)
    {
        BooleanStatistics booleanStatistics = null;
        if (trueValueCount != null) {
            booleanStatistics = new BooleanStatistics(trueValueCount);
        }
        return new ColumnStatistics(numberOfValues, 2L, booleanStatistics, null, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testBigint()
    {
        assertThat(getDomain(BIGINT, 0, null)).isEqualTo(none(BIGINT));
        assertThat(getDomain(BIGINT, 10, null)).isEqualTo(all(BIGINT));

        assertThat(getDomain(BIGINT, 0, integerColumnStats(null, null, null))).isEqualTo(none(BIGINT));
        assertThat(getDomain(BIGINT, 0, integerColumnStats(0L, null, null))).isEqualTo(none(BIGINT));
        assertThat(getDomain(BIGINT, 0, integerColumnStats(0L, 100L, 100L))).isEqualTo(none(BIGINT));

        assertThat(getDomain(BIGINT, 10, integerColumnStats(0L, null, null))).isEqualTo(onlyNull(BIGINT));
        assertThat(getDomain(BIGINT, 10, integerColumnStats(10L, null, null))).isEqualTo(notNull(BIGINT));

        assertThat(getDomain(BIGINT, 10, integerColumnStats(10L, 100L, 100L))).isEqualTo(singleValue(BIGINT, 100L));

        assertThat(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, 100L))).isEqualTo(create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), false));
        assertThat(getDomain(BIGINT, 10, integerColumnStats(10L, null, 100L))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 100L)), false));
        assertThat(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(BIGINT, 0L)), false));

        assertThat(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, 100L))).isEqualTo(create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), true));
        assertThat(getDomain(BIGINT, 10, integerColumnStats(5L, null, 100L))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(BIGINT, 100L)), true));
        assertThat(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(BIGINT, 0L)), true));
    }

    private static ColumnStatistics integerColumnStats(Long numberOfValues, Long minimum, Long maximum)
    {
        return new ColumnStatistics(numberOfValues, 9L, null, new IntegerStatistics(minimum, maximum, null), null, null, null, null, null, null, null, null);
    }

    @Test
    public void testDouble()
    {
        assertThat(getDomain(DOUBLE, 0, null)).isEqualTo(none(DOUBLE));
        assertThat(getDomain(DOUBLE, 10, null)).isEqualTo(all(DOUBLE));

        assertThat(getDomain(DOUBLE, 0, doubleColumnStats(null, null, null))).isEqualTo(none(DOUBLE));
        assertThat(getDomain(DOUBLE, 0, doubleColumnStats(0L, null, null))).isEqualTo(none(DOUBLE));
        assertThat(getDomain(DOUBLE, 0, doubleColumnStats(0L, 42.24, 42.24))).isEqualTo(none(DOUBLE));

        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(0L, null, null))).isEqualTo(onlyNull(DOUBLE));
        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, null))).isEqualTo(notNull(DOUBLE));

        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(10L, 42.24, 42.24))).isEqualTo(singleValue(DOUBLE, 42.24));

        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, 42.24))).isEqualTo(create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), false));
        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, 42.24))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(DOUBLE, 42.24)), false));
        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(DOUBLE, 3.3)), false));

        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, 42.24))).isEqualTo(create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), true));
        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(5L, null, 42.24))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(DOUBLE, 42.24)), true));
        assertThat(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(DOUBLE, 3.3)), true));
    }

    private static ColumnStatistics doubleColumnStats(Long numberOfValues, Double minimum, Double maximum)
    {
        return new ColumnStatistics(numberOfValues, 9L, null, null, new DoubleStatistics(minimum, maximum), null, null, null, null, null, null, null);
    }

    @Test
    public void testFloat()
    {
        assertThat(getDomain(REAL, 0, null)).isEqualTo(none(REAL));
        assertThat(getDomain(REAL, 10, null)).isEqualTo(all(REAL));

        assertThat(getDomain(REAL, 0, doubleColumnStats(null, null, null))).isEqualTo(none(REAL));
        assertThat(getDomain(REAL, 0, doubleColumnStats(0L, null, null))).isEqualTo(none(REAL));
        assertThat(getDomain(REAL, 0, doubleColumnStats(0L, (double) 42.24f, (double) 42.24f))).isEqualTo(none(REAL));

        assertThat(getDomain(REAL, 10, doubleColumnStats(0L, null, null))).isEqualTo(onlyNull(REAL));
        assertThat(getDomain(REAL, 10, doubleColumnStats(10L, null, null))).isEqualTo(notNull(REAL));

        assertThat(getDomain(REAL, 10, doubleColumnStats(10L, (double) 42.24f, (double) 42.24f))).isEqualTo(singleValue(REAL, (long) floatToRawIntBits(42.24f)));

        assertThat(getDomain(REAL, 10, doubleColumnStats(10L, 3.3, (double) 42.24f))).isEqualTo(create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(3.3f), true, (long) floatToRawIntBits(42.24f), true)), false));
        assertThat(getDomain(REAL, 10, doubleColumnStats(10L, null, (double) 42.24f))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(REAL, (long) floatToRawIntBits(42.24f))), false));
        assertThat(getDomain(REAL, 10, doubleColumnStats(10L, 3.3, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(REAL, (long) floatToRawIntBits(3.3f))), false));

        assertThat(getDomain(REAL, 10, doubleColumnStats(5L, 3.3, (double) 42.24f))).isEqualTo(create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(3.3f), true, (long) floatToRawIntBits(42.24f), true)), true));
        assertThat(getDomain(REAL, 10, doubleColumnStats(5L, null, (double) 42.24f))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(REAL, (long) floatToRawIntBits(42.24f))), true));
        assertThat(getDomain(REAL, 10, doubleColumnStats(5L, 3.3, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(REAL, (long) floatToRawIntBits(3.3f))), true));
    }

    @Test
    public void testString()
    {
        assertThat(getDomain(VARCHAR, 0, null)).isEqualTo(none(VARCHAR));
        assertThat(getDomain(VARCHAR, 10, null)).isEqualTo(all(VARCHAR));

        assertThat(getDomain(VARCHAR, 0, stringColumnStats(null, null, null))).isEqualTo(none(VARCHAR));
        assertThat(getDomain(VARCHAR, 0, stringColumnStats(0L, null, null))).isEqualTo(none(VARCHAR));
        assertThat(getDomain(VARCHAR, 0, stringColumnStats(0L, "taco", "taco"))).isEqualTo(none(VARCHAR));

        assertThat(getDomain(VARCHAR, 10, stringColumnStats(0L, null, null))).isEqualTo(onlyNull(VARCHAR));
        assertThat(getDomain(VARCHAR, 10, stringColumnStats(10L, null, null))).isEqualTo(notNull(VARCHAR));

        assertThat(getDomain(VARCHAR, 10, stringColumnStats(10L, "taco", "taco"))).isEqualTo(singleValue(VARCHAR, utf8Slice("taco")));

        assertThat(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", "taco"))).isEqualTo(create(ValueSet.ofRanges(range(VARCHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertThat(getDomain(VARCHAR, 10, stringColumnStats(10L, null, "taco"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(VARCHAR, utf8Slice("taco"))), false));
        assertThat(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(VARCHAR, utf8Slice("apple"))), false));

        assertThat(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", "taco"))).isEqualTo(create(ValueSet.ofRanges(range(VARCHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertThat(getDomain(VARCHAR, 10, stringColumnStats(5L, null, "taco"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(VARCHAR, utf8Slice("taco"))), true));
        assertThat(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(VARCHAR, utf8Slice("apple"))), true));
    }

    @Test
    public void testChar()
    {
        assertThat(getDomain(CHAR, 0, null)).isEqualTo(none(CHAR));
        assertThat(getDomain(CHAR, 10, null)).isEqualTo(all(CHAR));

        assertThat(getDomain(CHAR, 0, stringColumnStats(null, null, null))).isEqualTo(none(CHAR));
        assertThat(getDomain(CHAR, 0, stringColumnStats(0L, null, null))).isEqualTo(none(CHAR));
        assertThat(getDomain(CHAR, 0, stringColumnStats(0L, "taco      ", "taco      "))).isEqualTo(none(CHAR));
        assertThat(getDomain(CHAR, 0, stringColumnStats(0L, "taco", "taco      "))).isEqualTo(none(CHAR));

        assertThat(getDomain(CHAR, 10, stringColumnStats(0L, null, null))).isEqualTo(onlyNull(CHAR));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, null, null))).isEqualTo(notNull(CHAR));

        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "taco      ", "taco      "))).isEqualTo(singleValue(CHAR, utf8Slice("taco")));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "taco", "taco      "))).isEqualTo(singleValue(CHAR, utf8Slice("taco")));

        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", "taco      "))).isEqualTo(create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", "taco"))).isEqualTo(create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, null, "taco      "))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), false));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, null, "taco"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), false));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "apple     ", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), false));
        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "apple", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), false));

        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", "taco      "))).isEqualTo(create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", "taco"))).isEqualTo(create(ValueSet.ofRanges(range(CHAR, utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, null, "taco      "))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), true));
        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, null, "taco"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(CHAR, utf8Slice("taco"))), true));
        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, "apple     ", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), true));
        assertThat(getDomain(CHAR, 10, stringColumnStats(5L, "apple", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(CHAR, utf8Slice("apple"))), true));

        assertThat(getDomain(CHAR, 10, stringColumnStats(10L, "\0 ", " "))).isEqualTo(create(ValueSet.ofRanges(range(CHAR, utf8Slice("\0"), true, utf8Slice(""), true)), false));
    }

    private static ColumnStatistics stringColumnStats(Long numberOfValues, String minimum, String maximum)
    {
        Slice minimumSlice = minimum == null ? null : utf8Slice(minimum);
        Slice maximumSlice = maximum == null ? null : utf8Slice(maximum);
        // sum and minAverageValueSizeInBytes are not used in this test; they could be arbitrary numbers
        return new ColumnStatistics(numberOfValues, 10L, null, null, null, null, new StringStatistics(minimumSlice, maximumSlice, 100L), null, null, null, null, null);
    }

    @Test
    public void testDate()
    {
        assertThat(getDomain(DATE, 0, null)).isEqualTo(none(DATE));
        assertThat(getDomain(DATE, 10, null)).isEqualTo(all(DATE));

        assertThat(getDomain(DATE, 0, dateColumnStats(null, null, null))).isEqualTo(none(DATE));
        assertThat(getDomain(DATE, 0, dateColumnStats(0L, null, null))).isEqualTo(none(DATE));
        assertThat(getDomain(DATE, 0, dateColumnStats(0L, 100, 100))).isEqualTo(none(DATE));

        assertThat(getDomain(DATE, 10, dateColumnStats(0L, null, null))).isEqualTo(onlyNull(DATE));
        assertThat(getDomain(DATE, 10, dateColumnStats(10L, null, null))).isEqualTo(notNull(DATE));

        assertThat(getDomain(DATE, 10, dateColumnStats(10L, 100, 100))).isEqualTo(singleValue(DATE, 100L));

        assertThat(getDomain(DATE, 10, dateColumnStats(10L, 0, 100))).isEqualTo(create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), false));
        assertThat(getDomain(DATE, 10, dateColumnStats(10L, null, 100))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(DATE, 100L)), false));
        assertThat(getDomain(DATE, 10, dateColumnStats(10L, 0, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(DATE, 0L)), false));

        assertThat(getDomain(DATE, 10, dateColumnStats(5L, 0, 100))).isEqualTo(create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), true));
        assertThat(getDomain(DATE, 10, dateColumnStats(5L, null, 100))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(DATE, 100L)), true));
        assertThat(getDomain(DATE, 10, dateColumnStats(5L, 0, null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(DATE, 0L)), true));
    }

    private static ColumnStatistics dateColumnStats(Long numberOfValues, Integer minimum, Integer maximum)
    {
        return new ColumnStatistics(numberOfValues, 5L, null, null, null, null, null, new DateStatistics(minimum, maximum), null, null, null, null);
    }

    @Test
    public void testTimestampMillis()
    {
        assertThat(getDomain(TIMESTAMP_MILLIS, 0, null))
                .isEqualTo(none(TIMESTAMP_MILLIS));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, null))
                .isEqualTo(all(TIMESTAMP_MILLIS));

        assertThat(getDomain(TIMESTAMP_MILLIS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_MILLIS));
        assertThat(getDomain(TIMESTAMP_MILLIS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_MILLIS));
        assertThat(getDomain(TIMESTAMP_MILLIS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_MILLIS));

        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_MILLIS));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_MILLIS));

        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MILLIS, 100000L, true, 101000L, true)), false));

        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MILLIS, 13000L, true, 101000L, true)), false));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_MILLIS, 101000L)), false));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_MILLIS, 13000L)), false));

        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MILLIS, 13000L, true, 101000L, true)), true));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_MILLIS, 101000L)), true));
        assertThat(getDomain(TIMESTAMP_MILLIS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_MILLIS, 13000L)), true));
    }

    @Test
    public void testTimestampMicros()
    {
        assertThat(getDomain(TIMESTAMP_MICROS, 0, null))
                .isEqualTo(none(TIMESTAMP_MICROS));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, null))
                .isEqualTo(all(TIMESTAMP_MICROS));

        assertThat(getDomain(TIMESTAMP_MICROS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_MICROS));
        assertThat(getDomain(TIMESTAMP_MICROS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_MICROS));
        assertThat(getDomain(TIMESTAMP_MICROS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_MICROS));

        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_MICROS));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_MICROS));

        long low13 = 13_000L;
        long low100 = 100_000L;
        long high100 = 101_000L;

        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MICROS, low100, true, high100, true)), false));

        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MICROS, low13, true, high100, true)), false));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_MICROS, high100)), false));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_MICROS, low13)), false));

        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_MICROS, low13, true, high100, true)), true));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_MICROS, high100)), true));
        assertThat(getDomain(TIMESTAMP_MICROS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_MICROS, low13)), true));
    }

    @Test
    public void testTimestampNanos()
    {
        assertThat(getDomain(TIMESTAMP_NANOS, 0, null))
                .isEqualTo(none(TIMESTAMP_NANOS));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, null))
                .isEqualTo(all(TIMESTAMP_NANOS));

        assertThat(getDomain(TIMESTAMP_NANOS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_NANOS));
        assertThat(getDomain(TIMESTAMP_NANOS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_NANOS));
        assertThat(getDomain(TIMESTAMP_NANOS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_NANOS));

        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_NANOS));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_NANOS));

        LongTimestamp low13 = new LongTimestamp(13_000L, 0);
        LongTimestamp low100 = new LongTimestamp(100_000L, 0);
        LongTimestamp high100 = new LongTimestamp(101_000L, 0);

        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_NANOS, low100, true, high100, true)), false));

        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_NANOS, low13, true, high100, true)), false));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_NANOS, high100)), false));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_NANOS, low13)), false));

        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_NANOS, low13, true, high100, true)), true));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_NANOS, high100)), true));
        assertThat(getDomain(TIMESTAMP_NANOS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_NANOS, low13)), true));
    }

    @Test
    public void testInstantMillis()
    {
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 0, null))
                .isEqualTo(none(TIMESTAMP_TZ_MILLIS));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, null))
                .isEqualTo(all(TIMESTAMP_TZ_MILLIS));

        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_MILLIS));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_MILLIS));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_TZ_MILLIS));

        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_TZ_MILLIS));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_TZ_MILLIS));

        long instant13 = packDateTimeWithZone(13, UTC_KEY);
        long instant100 = packDateTimeWithZone(100, UTC_KEY);

        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(singleValue(TIMESTAMP_TZ_MILLIS, instant100));

        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_MILLIS, instant13, true, instant100, true)), false));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_MILLIS, instant100)), false));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_MILLIS, instant13)), false));

        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_MILLIS, instant13, true, instant100, true)), true));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_MILLIS, instant100)), true));
        assertThat(getDomain(TIMESTAMP_TZ_MILLIS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_MILLIS, instant13)), true));
    }

    @Test
    public void testInstantMicros()
    {
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 0, null))
                .isEqualTo(none(TIMESTAMP_TZ_MICROS));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, null))
                .isEqualTo(all(TIMESTAMP_TZ_MICROS));

        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_MICROS));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_MICROS));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_TZ_MICROS));

        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_TZ_MICROS));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_TZ_MICROS));

        LongTimestampWithTimeZone low13 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(13L, 0, UTC_KEY);
        LongTimestampWithTimeZone low100 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(100L, 0, UTC_KEY);
        LongTimestampWithTimeZone high100 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(100L, 999_000_000, UTC_KEY);

        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_MICROS, low100, true, high100, true)), false));

        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_MICROS, low13, true, high100, true)), false));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_MICROS, high100)), false));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_MICROS, low13)), false));

        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_MICROS, low13, true, high100, true)), true));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_MICROS, high100)), true));
        assertThat(getDomain(TIMESTAMP_TZ_MICROS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_MICROS, low13)), true));
    }

    @Test
    public void testInstantNanos()
    {
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 0, null))
                .isEqualTo(none(TIMESTAMP_TZ_NANOS));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, null))
                .isEqualTo(all(TIMESTAMP_TZ_NANOS));

        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 0, timeStampColumnStats(null, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_NANOS));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 0, timeStampColumnStats(0L, null, null)))
                .isEqualTo(none(TIMESTAMP_TZ_NANOS));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 0, timeStampColumnStats(0L, 100L, 100L)))
                .isEqualTo(none(TIMESTAMP_TZ_NANOS));

        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(0L, null, null)))
                .isEqualTo(onlyNull(TIMESTAMP_TZ_NANOS));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(10L, null, null)))
                .isEqualTo(notNull(TIMESTAMP_TZ_NANOS));

        LongTimestampWithTimeZone low13 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(13L, 0, UTC_KEY);
        LongTimestampWithTimeZone low100 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(100L, 0, UTC_KEY);
        LongTimestampWithTimeZone high100 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(100L, 999_999_000, UTC_KEY);

        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(10L, 100L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_NANOS, low100, true, high100, true)), false));

        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(10L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_NANOS, low13, true, high100, true)), false));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(10L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_NANOS, high100)), false));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(10L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_NANOS, low13)), false));

        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(5L, 13L, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(range(TIMESTAMP_TZ_NANOS, low13, true, high100, true)), true));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(5L, null, 100L)))
                .isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(TIMESTAMP_TZ_NANOS, high100)), true));
        assertThat(getDomain(TIMESTAMP_TZ_NANOS, 10, timeStampColumnStats(5L, 13L, null)))
                .isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(TIMESTAMP_TZ_NANOS, low13)), true));
    }

    private static ColumnStatistics timeStampColumnStats(Long numberOfValues, Long minimum, Long maximum)
    {
        return new ColumnStatistics(numberOfValues, 5L, null, null, null, null, null, null, new TimestampStatistics(minimum, maximum), null, null, null);
    }

    @Test
    public void testDecimal()
    {
        assertThat(getDomain(SHORT_DECIMAL, 0, null)).isEqualTo(none(SHORT_DECIMAL));
        assertThat(getDomain(LONG_DECIMAL, 10, null)).isEqualTo(all(LONG_DECIMAL));

        assertThat(getDomain(SHORT_DECIMAL, 0, decimalColumnStats(null, null, null))).isEqualTo(none(SHORT_DECIMAL));
        assertThat(getDomain(LONG_DECIMAL, 0, decimalColumnStats(0L, null, null))).isEqualTo(none(LONG_DECIMAL));
        assertThat(getDomain(SHORT_DECIMAL, 0, decimalColumnStats(0L, "-999.99", "999.99"))).isEqualTo(none(SHORT_DECIMAL));

        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(0L, null, null))).isEqualTo(onlyNull(LONG_DECIMAL));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, null, null))).isEqualTo(notNull(SHORT_DECIMAL));

        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "999.99", "999.99"))).isEqualTo(singleValue(SHORT_DECIMAL, shortDecimal("999.99")));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "999.9", "999.9"))).isEqualTo(singleValue(SHORT_DECIMAL, shortDecimal("999.90")));
        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "1234567890.0987654321", "1234567890.0987654321"))).isEqualTo(singleValue(LONG_DECIMAL, longDecimal("1234567890.0987654321")));

        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "-999.99", "999.99"))).isEqualTo(create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("-999.99"), true, shortDecimal("999.99"), true)), false));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "10.5", "20"))).isEqualTo(create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("10.50"), true, shortDecimal("20.00"), true)), false));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, null, "999.99"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(SHORT_DECIMAL, shortDecimal("999.99"))), false));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(10L, "-999.99", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(SHORT_DECIMAL, shortDecimal("-999.99"))), false));

        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "-1234567890.0987654321", "1234567890.0987654321"))).isEqualTo(create(ValueSet.ofRanges(range(LONG_DECIMAL, longDecimal("-1234567890.0987654321"), true, longDecimal("1234567890.0987654321"), true)), false));
        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, null, "1234567890.0987654321"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(LONG_DECIMAL, longDecimal("1234567890.0987654321"))), false));
        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(10L, "-1234567890.0987654321", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(LONG_DECIMAL, longDecimal("-1234567890.0987654321"))), false));

        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, "-999.99", "999.99"))).isEqualTo(create(ValueSet.ofRanges(range(SHORT_DECIMAL, shortDecimal("-999.99"), true, shortDecimal("999.99"), true)), true));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, null, "999.99"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(SHORT_DECIMAL, shortDecimal("999.99"))), true));
        assertThat(getDomain(SHORT_DECIMAL, 10, decimalColumnStats(5L, "-999.99", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(SHORT_DECIMAL, shortDecimal("-999.99"))), true));

        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, "-1234567890.0987654321", "1234567890.0987654321"))).isEqualTo(create(ValueSet.ofRanges(range(LONG_DECIMAL, longDecimal("-1234567890.0987654321"), true, longDecimal("1234567890.0987654321"), true)), true));
        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, null, "1234567890.0987654321"))).isEqualTo(create(ValueSet.ofRanges(lessThanOrEqual(LONG_DECIMAL, longDecimal("1234567890.0987654321"))), true));
        assertThat(getDomain(LONG_DECIMAL, 10, decimalColumnStats(5L, "-1234567890.0987654321", null))).isEqualTo(create(ValueSet.ofRanges(greaterThanOrEqual(LONG_DECIMAL, longDecimal("-1234567890.0987654321"))), true));
    }

    private static ColumnStatistics decimalColumnStats(Long numberOfValues, String minimum, String maximum)
    {
        BigDecimal minimumDecimal = minimum == null ? null : new BigDecimal(minimum);
        BigDecimal maximumDecimal = maximum == null ? null : new BigDecimal(maximum);
        return new ColumnStatistics(numberOfValues, 9L, null, null, null, null, null, null, null, new DecimalStatistics(minimumDecimal, maximumDecimal, SHORT_DECIMAL_VALUE_BYTES), null, null);
    }

    @Test
    public void testBinary()
    {
        assertThat(getDomain(VARBINARY, 0, null)).isEqualTo(none(VARBINARY));
        assertThat(getDomain(VARBINARY, 10, null)).isEqualTo(all(VARBINARY));

        assertThat(getDomain(VARBINARY, 0, binaryColumnStats(null))).isEqualTo(none(VARBINARY));
        assertThat(getDomain(VARBINARY, 0, binaryColumnStats(0L))).isEqualTo(none(VARBINARY));
        assertThat(getDomain(VARBINARY, 0, binaryColumnStats(0L))).isEqualTo(none(VARBINARY));

        assertThat(getDomain(VARBINARY, 10, binaryColumnStats(0L))).isEqualTo(onlyNull(VARBINARY));
        assertThat(getDomain(VARBINARY, 10, binaryColumnStats(10L))).isEqualTo(notNull(VARBINARY));

        assertThat(getDomain(VARBINARY, 20, binaryColumnStats(10L))).isEqualTo(all(VARBINARY));
    }

    private static ColumnStatistics binaryColumnStats(Long numberOfValues)
    {
        // sum and minAverageValueSizeInBytes are not used in this test; they could be arbitrary numbers
        return new ColumnStatistics(numberOfValues, 10L, null, null, null, null, null, null, null, null, new BinaryStatistics(100L), null);
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValue();
    }

    private static Int128 longDecimal(String value)
    {
        return Decimals.valueOf(new BigDecimal(value));
    }
}
