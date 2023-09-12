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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.block.ColumnarTestUtils.alternatingNullValues;
import static io.trino.block.ColumnarTestUtils.assertBlock;
import static io.trino.block.ColumnarTestUtils.assertBlockPosition;
import static io.trino.block.ColumnarTestUtils.createTestDictionaryBlock;
import static io.trino.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static io.trino.block.ColumnarTestUtils.createTestRleBlock;
import static io.trino.block.ColumnarTestUtils.createTestRleExpectedValues;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestColumnarMap
{
    private static final int[] MAP_SIZES = new int[] {16, 0, 13, 1, 2, 11, 4, 7};

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
        BlockBuilder blockBuilder = createBlockBuilderWithValues(expectedValues);
        verifyBlock(blockBuilder, expectedValues);
        verifyBlock(blockBuilder.build(), expectedValues);

        Slice[][][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        BlockBuilder blockBuilderWithNull = createBlockBuilderWithValues(expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull, expectedValuesWithNull);
        verifyBlock(blockBuilderWithNull.build(), expectedValuesWithNull);
    }

    private static void verifyBlock(Block block, Slice[][][] expectedValues)
    {
        assertBlock(block, expectedValues);

        assertColumnarMap(block, expectedValues);
        assertDictionaryBlock(block, expectedValues);
        assertRunLengthEncodedBlock(block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        Slice[][][] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(blockRegion, expectedValuesRegion);

        assertColumnarMap(blockRegion, expectedValuesRegion);
        assertDictionaryBlock(blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(blockRegion, expectedValuesRegion);
    }

    private static void assertDictionaryBlock(Block block, Slice[][][] expectedValues)
    {
        Block dictionaryBlock = createTestDictionaryBlock(block);
        Slice[][][] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(dictionaryBlock, expectedDictionaryValues);
        assertColumnarMap(dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(dictionaryBlock, expectedDictionaryValues);
    }

    private static void assertRunLengthEncodedBlock(Block block, Slice[][][] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            Slice[][][] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(runLengthEncodedBlock, expectedDictionaryValues);
            assertColumnarMap(runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static void assertColumnarMap(Block block, Slice[][][] expectedValues)
    {
        ColumnarMap columnarMap = toColumnarMap(block);
        assertEquals(columnarMap.getPositionCount(), expectedValues.length);

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();
        int elementsPosition = 0;
        for (int position = 0; position < expectedValues.length; position++) {
            Slice[][] expectedMap = expectedValues[position];
            assertEquals(columnarMap.isNull(position), expectedMap == null);
            if (expectedMap == null) {
                assertEquals(columnarMap.getEntryCount(position), 0);
                continue;
            }

            assertEquals(columnarMap.getEntryCount(position), expectedMap.length);
            assertEquals(columnarMap.getOffset(position), elementsPosition);

            for (int i = 0; i < columnarMap.getEntryCount(position); i++) {
                Slice[] expectedEntry = expectedMap[i];

                Slice expectedKey = expectedEntry[0];
                assertBlockPosition(keysBlock, elementsPosition, expectedKey);

                Slice expectedValue = expectedEntry[1];
                assertBlockPosition(valuesBlock, elementsPosition, expectedValue);

                elementsPosition++;
            }
        }
    }

    public static BlockBuilder createBlockBuilderWithValues(Slice[][][] expectedValues)
    {
        MapBlockBuilder blockBuilder = createMapBuilder(100);
        for (Slice[][] expectedMap : expectedValues) {
            if (expectedMap == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (Slice[] entry : expectedMap) {
                        Slice key = entry[0];
                        assertNotNull(key);
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

    private static MapBlockBuilder createMapBuilder(int expectedEntries)
    {
        MapType mapType = (MapType) TESTING_TYPE_MANAGER.getType(new TypeSignature(MAP, TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature()), TypeSignatureParameter.typeParameter(VARCHAR.getTypeSignature())));
        return new MapBlockBuilder(mapType, null, expectedEntries);
    }

    @SuppressWarnings("unused")
    public static long blockVarcharHashCode(Block block, int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }
}
