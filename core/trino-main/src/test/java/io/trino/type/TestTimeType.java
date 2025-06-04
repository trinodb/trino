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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeType
        extends AbstractTestType
{
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

    @Test
    public void testRange()
    {
        Type.Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(0);
        assertThat(range.getMax()).isEqualTo(PICOSECONDS_PER_DAY);
    }

    @Test
    public void testPreviousValue()
    {
        long minValue = 0;
        long maxValue = PICOSECONDS_PER_DAY;

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + 1_000_000_000))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(1110_000_000_000L));

        assertThat(type.getPreviousValue(maxValue - 1_000_000_000))
                .isEqualTo(Optional.of(maxValue - 2_000_000_000));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - 1_000_000_000));
    }

    @Test
    public void testNextValue()
    {
        long minValue = 0;
        long maxValue = PICOSECONDS_PER_DAY;

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + 1_000_000_000));
        assertThat(type.getNextValue(minValue + 1_000_000_000))
                .isEqualTo(Optional.of(minValue + 2_000_000_000));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(1112_000_000_000L));

        assertThat(type.getNextValue(maxValue - 1_000_000_000))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }
}
