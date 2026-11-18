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
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.Type;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeType
        extends AbstractTestType
{
    private static final long NOON = 43_200_000_000_000_000L;

    public TestTimeType()
    {
        super(TIME_MILLIS, SqlTime.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIME_MILLIS.createFixedSizeBlockBuilder(15);
        TIME_MILLIS.writeLong(blockBuilder, 1_111_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 1_111_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 1_111_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 2_222_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 2_222_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 2_222_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 2_222_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 2_222_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 3_333_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 3_333_000_000_000L);
        TIME_MILLIS.writeLong(blockBuilder, 4_444_000_000_000L);
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }

    @ParameterizedTest
    @MethodSource("everyPrecisionDataProvider")
    public void testRangeEveryPrecision(int precision, long maxValue, long step)
    {
        Range range = createTimeType(precision).getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(0L);
        assertThat(range.getMax()).isEqualTo(maxValue);
        assertThat(maxValue + step).isEqualTo(PICOSECONDS_PER_DAY);
    }

    @ParameterizedTest
    @MethodSource("everyPrecisionDataProvider")
    public void testPreviousValueEveryPrecision(int precision, long maxValue, long step)
    {
        Type timeType = createTimeType(precision);
        assertThat(timeType.getPreviousValue(0L)).isEqualTo(Optional.empty());
        assertThat(timeType.getPreviousValue(step)).isEqualTo(Optional.of(0L));
        assertThat(timeType.getPreviousValue(NOON)).isEqualTo(Optional.of(NOON - step));
        assertThat(timeType.getPreviousValue(maxValue)).isEqualTo(Optional.of(maxValue - step));
    }

    @ParameterizedTest
    @MethodSource("everyPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, long maxValue, long step)
    {
        Type timeType = createTimeType(precision);
        assertThat(timeType.getNextValue(0L)).isEqualTo(Optional.of(step));
        assertThat(timeType.getNextValue(NOON)).isEqualTo(Optional.of(NOON + step));
        assertThat(timeType.getNextValue(maxValue - step)).isEqualTo(Optional.of(maxValue));
        assertThat(timeType.getNextValue(maxValue)).isEqualTo(Optional.empty());
    }

    public static Stream<Arguments> everyPrecisionDataProvider()
    {
        return Stream.of(
                Arguments.of(0, 86_399_000_000_000_000L, 1_000_000_000_000L),
                Arguments.of(3, 86_399_999_000_000_000L, 1_000_000_000L),
                Arguments.of(6, 86_399_999_999_000_000L, 1_000_000L),
                Arguments.of(9, 86_399_999_999_999_000L, 1_000L),
                Arguments.of(12, 86_399_999_999_999_999L, 1L));
    }
}
