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
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.spi.block.ColumnarMap.toColumnarMap;
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

public class TestColumnarMap
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MapType MAP_TYPE = new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS);
    private static final int[] MAP_SIZES = new int[]{16, 0, 13, 1, 2, 11, 4, 7};

    @Test
    public void test()
    {
        Slice[][][] expectedValues = new Slice[MAP_SIZES.length][][];
        for (int mapIndex = 0; mapIndex < MAP_SIZES.length; mapIndex++) {
            expectedValues[mapIndex] = new Slice[MAP_SIZES[mapIndex]][];
            for (int entryIndex = 0; entryIndex < MAP_SIZES[mapIndex]; entryIndex++) {
                Slice[] entry = new Slice[2];
                entry[0] = Slices.utf8Slice(format("key.%d.%d", mapIndex, entryIndex));
                if (entryIndex % 3 != 1) {
                    entry[1] = Slices.utf8Slice(format("value.%d.%d", mapIndex, entryIndex));
                }
                expectedValues[mapIndex][entryIndex] = entry;
            }
        }
        Block block = createBlockBuilderWithValues(expectedValues).build();
        verifyBlock(block, expectedValues);

        Slice[][][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(expectedValuesWithNull).build();
        verifyBlock(blockWithNull, expectedValuesWithNull);
    }

    private static void verifyBlock(Block block, Slice[][][] expectedValues)
    {
        assertBlock(MAP_TYPE, block, expectedValues);

        assertColumnarMap(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        Slice[][][] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(MAP_TYPE, blockRegion, expectedValuesRegion);

        assertColumnarMap(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static void assertDictionaryBlock(Block block, Slice[][][] expectedValues)
    {
        Block dictionaryBlock = createTestDictionaryBlock(block);
        Slice[][][] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(MAP_TYPE, dictionaryBlock, expectedDictionaryValues);
        assertColumnarMap(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static void assertRunLengthEncodedBlock(Block block, Slice[][][] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            Slice[][][] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(MAP_TYPE, runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarMap(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static void assertColumnarMap(Block block, Slice[][][] expectedValues)
    {
        ColumnarMap columnarMap = toColumnarMap(block);
        assertThat(columnarMap.getPositionCount()).isEqualTo(expectedValues.length);

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();
        int elementsPosition = 0;
        for (int position = 0; position < expectedValues.length; position++) {
            Slice[][] expectedMap = expectedValues[position];
            assertThat(columnarMap.isNull(position)).isEqualTo(expectedMap == null);
            if (expectedMap == null) {
                assertThat(columnarMap.getEntryCount(position)).isEqualTo(0);
                continue;
            }

            assertThat(columnarMap.getEntryCount(position)).isEqualTo(expectedMap.length);
            assertThat(columnarMap.getOffset(position)).isEqualTo(elementsPosition);

            for (int i = 0; i < columnarMap.getEntryCount(position); i++) {
                Slice[] expectedEntry = expectedMap[i];

                Slice expectedKey = expectedEntry[0];
                assertBlockPosition(MAP_TYPE, keysBlock, elementsPosition, expectedKey);

                Slice expectedValue = expectedEntry[1];
                assertBlockPosition(MAP_TYPE, valuesBlock, elementsPosition, expectedValue);

                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][][] expectedValues)
    {
        MapBlockBuilder blockBuilder = MAP_TYPE.createBlockBuilder(null, 100);
        for (Slice[][] expectedMap : expectedValues) {
            if (expectedMap == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (Slice[] entry : expectedMap) {
                        Slice key = entry[0];
                        assertThat(key).isNotNull();
                        VARCHAR.writeSlice(keyBuilder, key);

                        Slice value = entry[1];
                        if (value == null) {
                            valueBuilder.appendNull();
                        }
                        else {
                            VARCHAR.writeSlice(valueBuilder, value);
                        }
                    }
                });
            }
        }
        return blockBuilder;
    }
}
