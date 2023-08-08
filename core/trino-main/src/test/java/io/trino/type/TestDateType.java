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
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.DateType.DATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestDateType
        extends AbstractTestType
{
    public TestDateType()
    {
        super(DATE, SqlDate.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, 15);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 1111);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 2222);
        DATE.writeLong(blockBuilder, 3333);
        DATE.writeLong(blockBuilder, 3333);
        DATE.writeLong(blockBuilder, 4444);
        return blockBuilder.build();
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
        assertEquals(range.getMin(), (long) Integer.MIN_VALUE);
        assertEquals(range.getMax(), (long) Integer.MAX_VALUE);
    }

    @Test
    public void testPreviousValue()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + 1))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(1110L));

        assertThat(type.getPreviousValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValue()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(type.getNextValue(minValue + 1))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(1112L));

        assertThat(type.getNextValue(maxValue - 1))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }
}
