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
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTinyintType
        extends AbstractTestType
{
    public TestTinyintType()
    {
        super(TINYINT, Byte.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TINYINT.createFixedSizeBlockBuilder(15);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 111);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 22);
        TINYINT.writeLong(blockBuilder, 33);
        TINYINT.writeLong(blockBuilder, 33);
        TINYINT.writeLong(blockBuilder, 44);
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
        Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo((long) Byte.MIN_VALUE);
        assertThat(range.getMax()).isEqualTo((long) Byte.MAX_VALUE);
    }

    @Test
    public void testPreviousValue()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(110L));

        assertThat(type.getPreviousValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValue()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(type.getNextValue(minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(112L));

        assertThat(type.getNextValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }
}
