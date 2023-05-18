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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type.Range;

import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTinyintType
        extends AbstractTestType
{
    public TestTinyintType()
    {
        super(TINYINT, Byte.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, 15);
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
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }

    @Override
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo((long) Byte.MIN_VALUE);
        assertThat(range.getMax()).isEqualTo((long) Byte.MAX_VALUE);
    }

    @Override
    public void testPreviousValue()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(type.getPreviousValue(minValue)).isEmpty();
        assertThat(type.getPreviousValue(minValue + 1)).hasValue(minValue);

        assertThat(type.getPreviousValue(getSampleValue())).hasValue(110L);

        assertThat(type.getPreviousValue(maxValue - 1)).hasValue(maxValue - 2);
        assertThat(type.getPreviousValue(maxValue)).hasValue(maxValue - 1);
    }

    @Override
    public void testNextValue()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(type.getNextValue(minValue)).hasValue(minValue + 1);
        assertThat(type.getNextValue(minValue + 1)).hasValue(minValue + 2);

        assertThat(type.getNextValue(getSampleValue())).hasValue(112L);

        assertThat(type.getNextValue(maxValue - 1)).hasValue(maxValue);
        assertThat(type.getNextValue(maxValue)).isEmpty();
    }
}
