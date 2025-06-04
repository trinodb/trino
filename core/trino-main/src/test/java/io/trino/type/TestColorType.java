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
import io.trino.spi.block.ValueBlock;
import org.junit.jupiter.api.Test;

import static io.trino.operator.scalar.ColorFunctions.rgb;
import static io.trino.type.ColorType.COLOR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColorType
        extends AbstractTestType
{
    public TestColorType()
    {
        super(COLOR, String.class, createTestBlock());
    }

    @Test
    public void testGetObjectValue()
    {
        int[] valuesOfInterest = new int[] {0, 1, 15, 16, 127, 128, 255};
        BlockBuilder builder = COLOR.createFixedSizeBlockBuilder(valuesOfInterest.length * valuesOfInterest.length * valuesOfInterest.length);
        for (int r : valuesOfInterest) {
            for (int g : valuesOfInterest) {
                for (int b : valuesOfInterest) {
                    COLOR.writeLong(builder, rgb(r, g, b));
                }
            }
        }

        Block block = builder.build();
        for (int position = 0; position < block.getPositionCount(); position++) {
            int value = COLOR.getInt(block, position);
            assertThat(COLOR.getObjectValue(null, block, position)).isEqualTo(format("#%02x%02x%02x", (value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF));
        }
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = COLOR.createFixedSizeBlockBuilder(15);
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(1, 1, 1));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(2, 2, 2));
        COLOR.writeLong(blockBuilder, rgb(3, 3, 3));
        COLOR.writeLong(blockBuilder, rgb(3, 3, 3));
        COLOR.writeLong(blockBuilder, rgb(4, 4, 4));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
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
        assertThatThrownBy(() -> type.getPreviousValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }

    @Test
    public void testNextValue()
    {
        assertThatThrownBy(() -> type.getNextValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }
}
