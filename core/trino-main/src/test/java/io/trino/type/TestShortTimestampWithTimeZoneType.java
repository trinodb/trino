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
package io.trino.type;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestShortTimestampWithTimeZoneType
        extends AbstractTestType
{
    // the time zone occupies the low 12 bits of the packed value, so the epoch millis is a signed 52-bit number
    private static final long MIN_EPOCH_MILLIS = -(1L << 51);
    private static final long MAX_EPOCH_MILLIS = (1L << 51) - 1;

    public TestShortTimestampWithTimeZoneType()
    {
        super(TIMESTAMP_TZ_MILLIS, SqlTimestampWithTimeZone.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MILLIS.createFixedSizeBlockBuilder(15);
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(0)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(1)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1111, getTimeZoneKeyForOffset(2)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(3)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(4)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(5)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(6)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(2222, getTimeZoneKeyForOffset(7)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(8)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(3333, getTimeZoneKeyForOffset(9)));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(4444, getTimeZoneKeyForOffset(10)));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        // time zone doesn't matter for ordering
        return packDateTimeWithZone(unpackMillisUtc((Long) value) + 10, getTimeZoneKeyForOffset(33));
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(packDateTimeWithZone(MIN_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(packDateTimeWithZone(MIN_EPOCH_MILLIS + 1, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MIN_EPOCH_MILLIS, UTC_KEY)));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(packDateTimeWithZone(1110, UTC_KEY)));

        assertThat(type.getPreviousValue(packDateTimeWithZone(MAX_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MAX_EPOCH_MILLIS - 1, UTC_KEY)));
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(packDateTimeWithZone(MIN_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MIN_EPOCH_MILLIS + 1, UTC_KEY)));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(packDateTimeWithZone(1112, UTC_KEY)));

        assertThat(type.getNextValue(packDateTimeWithZone(MAX_EPOCH_MILLIS - 1, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MAX_EPOCH_MILLIS, UTC_KEY)));
        assertThat(type.getNextValue(packDateTimeWithZone(MAX_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.empty());
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testPreviousValueEveryPrecision(int precision, long step)
    {
        Type type = createTimestampWithTimeZoneType(precision);

        // there is no value before the minimum
        assertThat(type.getPreviousValue(packDateTimeWithZone(MIN_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(packDateTimeWithZone(MIN_EPOCH_MILLIS + step, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MIN_EPOCH_MILLIS, UTC_KEY)));

        // time zone doesn't matter for ordering
        assertThat(type.getPreviousValue(packDateTimeWithZone(5 * step, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(packDateTimeWithZone(4 * step, UTC_KEY)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, long step)
    {
        Type type = createTimestampWithTimeZoneType(precision);

        // there is no value after the maximum
        assertThat(type.getNextValue(packDateTimeWithZone(MAX_EPOCH_MILLIS, UTC_KEY)))
                .isEqualTo(Optional.empty());
        assertThat(type.getNextValue(packDateTimeWithZone(MAX_EPOCH_MILLIS - step, UTC_KEY)))
                .isEqualTo(Optional.of(packDateTimeWithZone(MAX_EPOCH_MILLIS, UTC_KEY)));

        // time zone doesn't matter for ordering
        assertThat(type.getNextValue(packDateTimeWithZone(4 * step, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(packDateTimeWithZone(5 * step, UTC_KEY)));
    }

    public static Stream<Arguments> testPreviousNextValueEveryPrecisionDataProvider()
    {
        // ShortTimestampWithTimeZoneType covers precisions 0..3; the step is the number of milliseconds per unit at that precision
        return Stream.of(
                Arguments.of(0, 1000L),
                Arguments.of(1, 100L),
                Arguments.of(2, 10L),
                Arguments.of(3, 1L));
    }
}
