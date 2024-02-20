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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.Arrays;

import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarTestUtils.alternatingNullValues;
import static io.trino.spi.block.ColumnarTestUtils.assertBlock;
import static io.trino.spi.block.ColumnarTestUtils.assertBlockPosition;
import static io.trino.spi.block.ColumnarTestUtils.createTestDictionaryBlock;
import static io.trino.spi.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static io.trino.spi.block.ColumnarTestUtils.createTestRleBlock;
import static io.trino.spi.block.ColumnarTestUtils.createTestRleExpectedValues;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnarArray
{
    private static final int[] ARRAY_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};
    private static final ArrayType ARRAY_TYPE = new ArrayType(VARCHAR);

    @Test
    public void test()
    {
        Slice[][] expectedValues = new Slice[ARRAY_SIZES.length][];
        for (int i = 0; i < ARRAY_SIZES.length; i++) {
            expectedValues[i] = new Slice[ARRAY_SIZES[i]];
            for (int j = 0; j < ARRAY_SIZES[i]; j++) {
                if (j % 3 != 1) {
                    expectedValues[i][j] = Slices.utf8Slice(format("%d.%d", i, j));
                }
            }
        }
        Block block = createBlockBuilderWithValues(expectedValues).build();
        verifyBlock(block, expectedValues);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(expectedValuesWithNull).build();
        verifyBlock(blockWithNull, expectedValuesWithNull);
    }

    private static <T> void verifyBlock(Block block, T[][] expectedValues)
    {
        assertBlock(ARRAY_TYPE, block, expectedValues);

        assertColumnarArray(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        T[][] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(ARRAY_TYPE, blockRegion, expectedValuesRegion);

        assertColumnarArray(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static <T> void assertDictionaryBlock(Block block, T[][] expectedValues)
    {
        Block dictionaryBlock = createTestDictionaryBlock(block);
        T[][] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(ARRAY_TYPE, dictionaryBlock, expectedDictionaryValues);
        assertColumnarArray(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static <T> void assertRunLengthEncodedBlock(Block block, T[][] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            T[][] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(ARRAY_TYPE, runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarArray(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static <T> void assertColumnarArray(Block block, T[][] expectedValues)
    {
        ColumnarArray columnarArray = toColumnarArray(block);
        assertThat(columnarArray.getPositionCount()).isEqualTo(expectedValues.length);

        Block elementsBlock = columnarArray.getElementsBlock();
        int elementsPosition = 0;
        for (int position = 0; position < expectedValues.length; position++) {
            T[] expectedArray = expectedValues[position];
            assertThat(columnarArray.isNull(position)).isEqualTo(expectedArray == null);
            assertThat(columnarArray.getLength(position)).isEqualTo(expectedArray == null ? 0 : Array.getLength(expectedArray));
            assertThat(elementsPosition).isEqualTo(columnarArray.getOffset(position));

            for (int i = 0; i < columnarArray.getLength(position); i++) {
                T expectedElement = expectedArray[i];
                assertBlockPosition(ARRAY_TYPE.getElementType(), elementsBlock, elementsPosition, expectedElement);
                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][] expectedValues)
    {
        BlockBuilder blockBuilder = ARRAY_TYPE.createBlockBuilder(null, 100, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, expectedValue.length);
                for (Slice v : expectedValue) {
                    if (v == null) {
                        elementBlockBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(elementBlockBuilder, v);
                    }
                }
                ARRAY_TYPE.writeObject(blockBuilder, elementBlockBuilder.build());
            }
        }
        return blockBuilder;
    }
}
