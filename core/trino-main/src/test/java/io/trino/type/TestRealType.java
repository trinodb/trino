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
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestRealType
        extends AbstractTestType
{
    public TestRealType()
    {
        super(REAL, Float.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, 30);
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(11.11F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(22.22F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(33.33F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(33.33F));
        REAL.writeLong(blockBuilder, floatToRawIntBits(44.44F));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        int bits = ((Long) value).intValue();
        float greaterValue = intBitsToFloat(bits) + 0.1f;
        return Long.valueOf(floatToRawIntBits(greaterValue));
    }

    @Test
    public void testNaNHash()
    {
        BlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 5);
        REAL.writeFloat(blockBuilder, Float.NaN);
        REAL.writeInt(blockBuilder, floatToIntBits(Float.NaN));
        REAL.writeInt(blockBuilder, floatToRawIntBits(Float.NaN));
        // the following two are the integer values of a float NaN
        REAL.writeInt(blockBuilder, -0x400000);
        REAL.writeInt(blockBuilder, 0x7fc00000);

        BlockPositionHashCode hashCodeOperator = blockTypeOperators.getHashCodeOperator(REAL);
        assertEquals(hashCodeOperator.hashCode(blockBuilder, 0), hashCodeOperator.hashCode(blockBuilder, 1));
        assertEquals(hashCodeOperator.hashCode(blockBuilder, 0), hashCodeOperator.hashCode(blockBuilder, 2));
        assertEquals(hashCodeOperator.hashCode(blockBuilder, 0), hashCodeOperator.hashCode(blockBuilder, 3));
        assertEquals(hashCodeOperator.hashCode(blockBuilder, 0), hashCodeOperator.hashCode(blockBuilder, 4));
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
