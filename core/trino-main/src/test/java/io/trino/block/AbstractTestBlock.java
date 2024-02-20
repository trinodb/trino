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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.MapHashTables;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.fill;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestBlock
{
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = new TestingBlockEncodingSerde(TESTING_TYPE_MANAGER::getType);

    protected <T> void assertBlock(Block block, T[] expectedValues)
    {
        assertBlockSize(block);
        assertRetainedSize(block);

        assertBlockPositions(block, expectedValues);
        assertBlockPositions(copyBlockViaBlockSerde(block), expectedValues);

        Block blockWithNull = copyBlockViaBlockSerde(block).copyWithAppendedNull();
        T[] expectedValuesWithNull = Arrays.copyOf(expectedValues, expectedValues.length + 1);
        assertBlockPositions(blockWithNull, expectedValuesWithNull);

        assertBlockSize(block);
        assertRetainedSize(block);

        if (block.mayHaveNull()) {
            assertThatThrownBy(() -> block.isNull(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position -1 in block with %d positions", block.getPositionCount());

            assertThatThrownBy(() -> block.isNull(block.getPositionCount()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", block.getPositionCount(), block.getPositionCount());
        }

        if (block instanceof ValueBlock valueBlock) {
            assertBlockClassImplementation(valueBlock.getClass());
        }
    }

    private static void assertRetainedSize(Block block)
    {
        long retainedSize = instanceSize(block.getClass());
        Field[] fields = block.getClass().getDeclaredFields();
        try {
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                Class<?> type = field.getType();
                if (type.isPrimitive() || Type.class.isAssignableFrom(type)) {
                    continue;
                }

                field.setAccessible(true);

                if (type == Slice.class) {
                    Slice slice = (Slice) field.get(block);
                    if (slice != null) {
                        retainedSize += slice.getRetainedSize();
                    }
                }
                else if (type == BlockBuilderStatus.class) {
                    if (field.get(block) != null) {
                        retainedSize += BlockBuilderStatus.INSTANCE_SIZE;
                    }
                }
                else if (Block.class.isAssignableFrom(type)) {
                    retainedSize += ((Block) field.get(block)).getRetainedSizeInBytes();
                }
                else if (type == Block[].class) {
                    Block[] blocks = (Block[]) field.get(block);
                    for (Block innerBlock : blocks) {
                        assertRetainedSize(innerBlock);
                        retainedSize += innerBlock.getRetainedSizeInBytes();
                    }
                }
                else if (type == SliceOutput.class) {
                    retainedSize += ((SliceOutput) field.get(block)).getRetainedSize();
                }
                else if (type == int[].class) {
                    retainedSize += sizeOf((int[]) field.get(block));
                }
                else if (type == boolean[].class) {
                    retainedSize += sizeOf((boolean[]) field.get(block));
                }
                else if (type == byte[].class) {
                    retainedSize += sizeOf((byte[]) field.get(block));
                }
                else if (type == long[].class) {
                    retainedSize += sizeOf((long[]) field.get(block));
                }
                else if (type == short[].class) {
                    retainedSize += sizeOf((short[]) field.get(block));
                }
                else if (type == DictionaryId.class) {
                    retainedSize += instanceSize(DictionaryId.class);
                }
                else if (type == MapHashTables.class) {
                    retainedSize += ((MapHashTables) field.get(block)).getRetainedSizeInBytes();
                }
                else if (type == MethodHandle.class) {
                    // MethodHandles are only used in MapBlock,
                    // and they are shared among blocks created by the same MapType.
                    // So we don't account for the memory held onto by MethodHandle instances.
                    // Otherwise, we will be counting it multiple times.
                }
                else if (field.getName().equals("fieldBlocksList")) {
                    // RowBlockBuilder fieldBlockBuildersList is a simple wrapper around the
                    // array already accounted for in the instance
                }
                else {
                    throw new IllegalArgumentException(format("Unknown type encountered: %s", type));
                }
            }
        }
        catch (IllegalAccessException t) {
            throw new RuntimeException(t);
        }
        assertThat(block.getRetainedSizeInBytes()).isEqualTo(retainedSize);
    }

    protected <T> void assertBlockFilteredPositions(T[] expectedValues, Block block, int... positions)
    {
        Block filteredBlock = block.copyPositions(positions, 0, positions.length);
        T[] filteredExpectedValues = filter(expectedValues, positions);
        assertThat(filteredBlock.getPositionCount()).isEqualTo(positions.length);
        assertBlock(filteredBlock, filteredExpectedValues);
    }

    private static <T> T[] filter(T[] expectedValues, int[] positions)
    {
        @SuppressWarnings("unchecked")
        T[] prunedExpectedValues = (T[]) Array.newInstance(expectedValues.getClass().getComponentType(), positions.length);
        for (int i = 0; i < prunedExpectedValues.length; i++) {
            prunedExpectedValues[i] = expectedValues[positions[i]];
        }
        return prunedExpectedValues;
    }

    private <T> void assertBlockPositions(Block block, T[] expectedValues)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        for (int position = 0; position < block.getPositionCount(); position++) {
            assertBlockPosition(block, position, expectedValues[position]);
        }
    }

    protected static List<Block> splitBlock(Block block, int count)
    {
        double sizePerSplit = block.getPositionCount() * 1.0 / count;
        ImmutableList.Builder<Block> result = ImmutableList.builderWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            int startPosition = toIntExact(Math.round(sizePerSplit * i));
            int endPosition = toIntExact(Math.round(sizePerSplit * (i + 1)));
            result.add(block.getRegion(startPosition, endPosition - startPosition));
        }
        return result.build();
    }

    private static void assertBlockSize(Block block)
    {
        // Asserting on `block` is not very effective because most blocks passed to this method are compact.
        // Therefore, we split the `block` into two and assert again.
        long expectedBlockSize = getCompactedBlockSizeInBytes(block);
        assertThat(block.getSizeInBytes()).isEqualTo(expectedBlockSize);
        assertThat(block.getRegionSizeInBytes(0, block.getPositionCount())).isEqualTo(expectedBlockSize);

        List<Block> splitBlock = splitBlock(block, 2);
        Block firstHalf = splitBlock.get(0);
        long expectedFirstHalfSize = getCompactedBlockSizeInBytes(firstHalf);
        assertThat(firstHalf.getSizeInBytes()).isEqualTo(expectedFirstHalfSize);
        assertThat(block.getRegionSizeInBytes(0, firstHalf.getPositionCount())).isEqualTo(expectedFirstHalfSize);
        Block secondHalf = splitBlock.get(1);
        long expectedSecondHalfSize = getCompactedBlockSizeInBytes(secondHalf);
        assertThat(secondHalf.getSizeInBytes()).isEqualTo(expectedSecondHalfSize);
        assertThat(block.getRegionSizeInBytes(firstHalf.getPositionCount(), secondHalf.getPositionCount())).isEqualTo(expectedSecondHalfSize);

        boolean[] positions = new boolean[block.getPositionCount()];
        fill(positions, 0, firstHalf.getPositionCount(), true);
        assertThat(block.getPositionsSizeInBytes(positions, firstHalf.getPositionCount())).isEqualTo(expectedFirstHalfSize);
        fill(positions, true);
        assertThat(block.getPositionsSizeInBytes(positions, positions.length)).isEqualTo(expectedBlockSize);
        fill(positions, 0, firstHalf.getPositionCount(), false);
        assertThat(block.getPositionsSizeInBytes(positions, positions.length - firstHalf.getPositionCount())).isEqualTo(expectedSecondHalfSize);
    }

    private <T> void assertBlockPosition(Block block, int position, T expectedValue)
    {
        assertPositionValue(block, position, expectedValue);
        assertPositionValue(block.getSingleValueBlock(position), 0, expectedValue);

        assertPositionValue(block.getRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.getRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.getRegion(position, block.getPositionCount() - position), 0, expectedValue);

        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(position, 1)), 0, expectedValue);
        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(0, position + 1)), position, expectedValue);
        assertPositionValue(copyBlockViaBlockSerde(block.getRegion(position, block.getPositionCount() - position)), 0, expectedValue);

        assertPositionValue(block.copyRegion(position, 1), 0, expectedValue);
        assertPositionValue(block.copyRegion(0, position + 1), position, expectedValue);
        assertPositionValue(block.copyRegion(position, block.getPositionCount() - position), 0, expectedValue);

        assertPositionValue(block.copyPositions(new int[] {position}, 0, 1), 0, expectedValue);
    }

    protected abstract <T> void assertPositionValue(Block block, int position, T expectedValue);

    private static long getCompactedBlockSizeInBytes(Block block)
    {
        if (block instanceof DictionaryBlock) {
            // dictionary blocks might become unwrapped when copyRegion is called on a block that is already compact
            return ((DictionaryBlock) block).compact().getSizeInBytes();
        }
        return copyBlockViaCopyRegion(block).getSizeInBytes();
    }

    private static Block copyBlockViaCopyRegion(Block block)
    {
        return block.copyRegion(0, block.getPositionCount());
    }

    private static Block copyBlockViaBlockSerde(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BLOCK_ENCODING_SERDE.writeBlock(sliceOutput, block);
        return BLOCK_ENCODING_SERDE.readBlock(sliceOutput.slice().getInput());
    }

    protected static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    protected static <T> T[] alternatingNullValues(T[] objects)
    {
        T[] objectsWithNulls = Arrays.copyOf(objects, objects.length * 2 + 1);
        for (int i = 0; i < objects.length; i++) {
            objectsWithNulls[i * 2] = null;
            objectsWithNulls[i * 2 + 1] = objects[i];
        }
        objectsWithNulls[objectsWithNulls.length - 1] = null;
        return objectsWithNulls;
    }

    protected static void assertEstimatedDataSizeForStats(BlockBuilder blockBuilder, Slice[] expectedSliceValues)
    {
        Block block = blockBuilder.build();
        assertThat(block.getPositionCount()).isEqualTo(expectedSliceValues.length);
        for (int i = 0; i < block.getPositionCount(); i++) {
            int expectedSize = expectedSliceValues[i] == null ? 0 : expectedSliceValues[i].length();
            assertThat(block.getEstimatedDataSizeForStats(i)).isEqualTo(expectedSize);
        }

        Block nullValueBlock = blockBuilder.newBlockBuilderLike(null).appendNull().build();
        assertThat(nullValueBlock.getEstimatedDataSizeForStats(0)).isEqualTo(0);
    }

    private static void testCopyRegionCompactness(Block block)
    {
        assertCompact(block.copyRegion(0, block.getPositionCount()));
        if (block.getPositionCount() > 0) {
            assertCompact(block.copyRegion(0, block.getPositionCount() - 1));
            assertCompact(block.copyRegion(1, block.getPositionCount() - 1));
        }
    }

    protected static void assertCompact(Block block)
    {
        assertThat(block.copyRegion(0, block.getPositionCount())).isSameAs(block);
    }

    private static void assertNotCompact(Block block)
    {
        assertThat(block.copyRegion(0, block.getPositionCount())).isNotSameAs(block);
    }

    protected static void testCompactBlock(Block block)
    {
        assertCompact(block);
        testCopyRegionCompactness(block);
    }

    protected static void testNotCompactBlock(Block block)
    {
        assertNotCompact(block);
        testCopyRegionCompactness(block);
    }

    private static void assertBlockClassImplementation(Class<? extends ValueBlock> clazz)
    {
        for (Method method : clazz.getMethods()) {
            if (method.getReturnType() == ValueBlock.class && !method.isBridge()) {
                throw new AssertionError(format("ValueBlock method %s should override return type to be %s", method, clazz.getSimpleName()));
            }
        }
    }
}
