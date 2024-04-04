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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public final class ColumnarTestUtils
{
    private ColumnarTestUtils() {}

    public static <T> void assertBlock(Type type, Block block, T[] expectedValues)
    {
        assertBlockPositions(type, block, expectedValues);
    }

    private static <T> void assertBlockPositions(Type type, Block block, T[] expectedValues)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(type, block, position, expectedValues[position]);
        }
    }

    public static <T> void assertBlockPosition(Type type, Block block, int position, T expectedValue)
    {
        assertPositionValue(type, block, position, expectedValue);
        assertPositionValue(type, block.getSingleValueBlock(position), 0, expectedValue);
    }

    private static <T> void assertPositionValue(Type type, Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }
        assertThat(block.isNull(position)).isFalse();

        if (expectedValue instanceof Slice expected) {
            int length = block.getSliceLength(position);
            assertThat(length).isEqualTo(expected.length());

            Slice actual = block.getSlice(position, 0, length);
            assertThat(actual).isEqualTo(expected);
        }
        else if (type instanceof ArrayType arrayType) {
            Block actual = arrayType.getObject(block, position);
            assertBlock(type, actual, (Slice[]) expectedValue);
        }
        else if (type instanceof RowType rowType) {
            SqlRow actual = rowType.getObject(block, position);
            int rawIndex = actual.getRawIndex();
            List<Block> fieldBlocks = actual.getRawFieldBlocks();
            Slice[] expectedValues = (Slice[]) expectedValue;
            for (int fieldIndex = 0; fieldIndex < fieldBlocks.size(); fieldIndex++) {
                Block fieldBlock = fieldBlocks.get(fieldIndex);
                Type fieldType = rowType.getTypeParameters().get(fieldIndex);
                assertBlockPosition(fieldType, fieldBlock, rawIndex, expectedValues[fieldIndex]);
            }
        }
        else if (type instanceof MapType mapType) {
            Slice[][] expected = (Slice[][]) expectedValue;
            SqlMap actual = mapType.getObject(block, position);

            Block actualKeys = actual.getRawKeyBlock().getRegion(actual.getRawOffset(), actual.getSize());
            Slice[] expectedKeys = Arrays.stream(expected)
                    .map(pair -> pair[0])
                    .toArray(Slice[]::new);
            assertBlock(type, actualKeys, expectedKeys);

            Block actualValues = actual.getRawValueBlock().getRegion(actual.getRawOffset(), actual.getSize());
            Slice[] expectedValues = Arrays.stream(expected)
                    .map(pair -> pair[1])
                    .toArray(Slice[]::new);
            assertBlock(type, actualValues, expectedValues);
        }
        else {
            throw new IllegalArgumentException(expectedValue.getClass().getName());
        }
    }

    public static <T> T[] alternatingNullValues(T[] objects)
    {
        @SuppressWarnings("unchecked")
        T[] objectsWithNulls = (T[]) Array.newInstance(objects.getClass().getComponentType(), objects.length * 2 + 1);
        for (int i = 0; i < objects.length; i++) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = objects[i];
        }
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }

    public static Block createTestDictionaryBlock(Block block)
    {
        int[] dictionaryIndexes = createTestDictionaryIndexes(block.getPositionCount());
        return DictionaryBlock.create(dictionaryIndexes.length, block, dictionaryIndexes);
    }

    public static <T> T[] createTestDictionaryExpectedValues(T[] expectedValues)
    {
        int[] dictionaryIndexes = createTestDictionaryIndexes(expectedValues.length);
        T[] expectedDictionaryValues = Arrays.copyOf(expectedValues, dictionaryIndexes.length);
        for (int i = 0; i < dictionaryIndexes.length; i++) {
            int dictionaryIndex = dictionaryIndexes[i];
            T expectedValue = expectedValues[dictionaryIndex];
            expectedDictionaryValues[i] = expectedValue;
        }
        return expectedDictionaryValues;
    }

    private static int[] createTestDictionaryIndexes(int valueCount)
    {
        int[] dictionaryIndexes = new int[valueCount * 2];
        for (int i = 0; i < valueCount; i++) {
            dictionaryIndexes[i] = valueCount - i - 1;
            dictionaryIndexes[i + valueCount] = i;
        }
        return dictionaryIndexes;
    }

    public static <T> T[] createTestRleExpectedValues(T[] expectedValues, int position)
    {
        T[] expectedDictionaryValues = Arrays.copyOf(expectedValues, 10);
        for (int i = 0; i < 10; i++) {
            expectedDictionaryValues[i] = expectedValues[position];
        }
        return expectedDictionaryValues;
    }

    public static RunLengthEncodedBlock createTestRleBlock(Block block, int position)
    {
        return (RunLengthEncodedBlock) RunLengthEncodedBlock.create(block.getRegion(position, 1), 10);
    }
}
