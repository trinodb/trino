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
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionXxHash64;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDoubleType
        extends AbstractTestType
{
    public TestDoubleType()
    {
        super(DOUBLE, Double.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 15);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 11.11);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 22.22);
        DOUBLE.writeDouble(blockBuilder, 33.33);
        DOUBLE.writeDouble(blockBuilder, 33.33);
        DOUBLE.writeDouble(blockBuilder, 44.44);
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Double) value) + 0.1;
    }

    @Test
    public void testNaNHash()
    {
        LongArrayBlockBuilder blockBuilder = (LongArrayBlockBuilder) DOUBLE.createBlockBuilder(null, 5);
        DOUBLE.writeDouble(blockBuilder, Double.NaN);
        blockBuilder.writeLong(doubleToLongBits(Double.NaN));
        blockBuilder.writeLong(doubleToRawLongBits(Double.NaN));
        // the following two are the long values of a double NaN
        blockBuilder.writeLong(-0x000fffffffffffffL);
        blockBuilder.writeLong(0x7ff8000000000000L);
        Block block = blockBuilder.build();

        BlockPositionHashCode hashCodeOperator = blockTypeOperators.getHashCodeOperator(DOUBLE);
        assertThat(hashCodeOperator.hashCode(block, 0)).isEqualTo(hashCodeOperator.hashCode(block, 1));
        assertThat(hashCodeOperator.hashCode(block, 0)).isEqualTo(hashCodeOperator.hashCode(block, 2));
        assertThat(hashCodeOperator.hashCode(block, 0)).isEqualTo(hashCodeOperator.hashCode(block, 3));
        assertThat(hashCodeOperator.hashCode(block, 0)).isEqualTo(hashCodeOperator.hashCode(block, 4));

        BlockPositionXxHash64 xxHash64Operator = blockTypeOperators.getXxHash64Operator(DOUBLE);
        assertThat(xxHash64Operator.xxHash64(block, 0)).isEqualTo(xxHash64Operator.xxHash64(block, 1));
        assertThat(xxHash64Operator.xxHash64(block, 0)).isEqualTo(xxHash64Operator.xxHash64(block, 2));
        assertThat(xxHash64Operator.xxHash64(block, 0)).isEqualTo(xxHash64Operator.xxHash64(block, 3));
        assertThat(xxHash64Operator.xxHash64(block, 0)).isEqualTo(xxHash64Operator.xxHash64(block, 4));
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
