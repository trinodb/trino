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
import org.junit.jupiter.api.Test;

import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntervalDayTimeType
        extends AbstractTestType
{
    public TestIntervalDayTimeType()
    {
        super(INTERVAL_DAY_TIME, SqlIntervalDayTime.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = INTERVAL_DAY_TIME.createFixedSizeBlockBuilder(15);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 1111);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 2222);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 3333);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 3333);
        INTERVAL_DAY_TIME.writeLong(blockBuilder, 4444);
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
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }
}
