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
package io.trino.block;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;

class TestVariantBlock
        extends AbstractTestBlock
{
    @Test
    void test()
    {
        Variant[] expectedValues = createTestValue(17);
        assertFixedWithValues(expectedValues);
        assertFixedWithValues(alternatingNullValues(expectedValues));
    }

    @Test
    void testCopyRegion()
    {
        Variant[] expectedValues = createTestValue(100);
        Block block = createBlockBuilderWithValues(expectedValues).build();
        Block actual = block.copyRegion(10, 10);
        Block expected = createBlockBuilderWithValues(copyOfRange(expectedValues, 10, 20)).build();
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        assertThat(actual.getSizeInBytes()).isEqualTo(expected.getSizeInBytes());
    }

    @Test
    void testCopyPositions()
    {
        Variant[] expectedValues = alternatingNullValues(createTestValue(17));
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        assertBlockFilteredPositions(expectedValues, blockBuilder.build(), 0, 2, 4, 6, 7, 9, 10, 16);
    }

    private void assertFixedWithValues(Variant[] expectedValues)
    {
        Block block = createBlockBuilderWithValues(expectedValues).build();
        assertBlock(block, expectedValues);
    }

    private static BlockBuilder createBlockBuilderWithValues(Variant[] expectedValues)
    {
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(null, expectedValues.length);
        writeValues(expectedValues, blockBuilder);
        return blockBuilder;
    }

    private static void writeValues(Variant[] expectedValues, VariantBlockBuilder blockBuilder)
    {
        for (Variant expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(expectedValue);
            }
        }
    }

    private static Variant[] createTestValue(int positionCount)
    {
        Variant[] expectedValues = new Variant[positionCount];
        Random random = new Random(0);
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = Variant.ofInt(random.nextInt());
        }
        return expectedValues;
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        VariantBlock variantBlock = (VariantBlock) block;
        Variant variant = variantBlock.getVariant(position);
        assertThat(variant).isEqualTo(expectedValue);
    }
}
