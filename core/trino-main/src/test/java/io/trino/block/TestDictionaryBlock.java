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
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDictionaryBlock
        extends AbstractTestBlock
{
    @Test
    public void testConstructionNoPositions()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block dictionary = createSlicesBlock(expectedValues);

        Block block = DictionaryBlock.create(0, dictionary, new int[] {1, 5, 9});
        assertThat(block).isInstanceOf(VariableWidthBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(0);
    }

    @Test
    public void testConstructionOnePositions()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block dictionary = createSlicesBlock(expectedValues);

        Block block = DictionaryBlock.create(1, dictionary, new int[] {1, 5, 9});
        assertThat(block).isInstanceOf(VariableWidthBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(VARBINARY.getSlice(block, 0)).isEqualTo(expectedValues[1]);
    }

    @Test
    public void testConstructionUnnestDictionary()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block innerDictionary = createSlicesBlock(expectedValues);
        DictionaryBlock dictionary = (DictionaryBlock) DictionaryBlock.create(4, innerDictionary, new int[] {1, 3, 5, 7});

        Block block = DictionaryBlock.create(2, dictionary, new int[] {1, 3});
        assertThat(block).isInstanceOf(DictionaryBlock.class);
        assertBlock(block, new Slice[] {expectedValues[3], expectedValues[7]});

        Block actualDictionary = ((DictionaryBlock) block).getDictionary();
        assertThat(actualDictionary).isSameAs(innerDictionary);
    }

    @Test
    public void testSizeInBytes()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        ValueBlock dictionary = dictionaryBlock.getDictionary();
        double averageEntrySize = dictionary.getSizeInBytes() / (double) dictionary.getPositionCount();
        assertThat(dictionaryBlock.getSizeInBytes()).isEqualTo((long) (averageEntrySize * 100) + (100 * SIZE_OF_INT));
    }

    @Test
    public void testCopyRegionCreatesCompactBlock()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        DictionaryBlock copyRegionDictionaryBlock = (DictionaryBlock) dictionaryBlock.copyRegion(1, 3);
        assertThat(copyRegionDictionaryBlock.isCompact()).isTrue();
    }

    @Test
    public void testCopyPositionsWithCompaction()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Slice firstExpectedValue = expectedValues[0];
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        int[] positionsToCopy = new int[] {0, 10, 20, 30, 40};
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertThat(copiedBlock.getDictionary().getPositionCount()).isEqualTo(1);
        assertThat(copiedBlock.getPositionCount()).isEqualTo(positionsToCopy.length);
        assertBlock(copiedBlock.getDictionary(), new Slice[] {firstExpectedValue});
        assertBlock(copiedBlock, new Slice[] {
                firstExpectedValue, firstExpectedValue, firstExpectedValue, firstExpectedValue, firstExpectedValue});
    }

    @Test
    public void testCopyPositionsWithCompactionsAndReorder()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        int[] positionsToCopy = new int[] {50, 55, 40, 45, 60};

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertThat(copiedBlock.getDictionary().getPositionCount()).isEqualTo(2);
        assertThat(copiedBlock.getPositionCount()).isEqualTo(positionsToCopy.length);

        assertBlock(copiedBlock.getDictionary(), new Slice[] {expectedValues[0], expectedValues[5]});
        assertDictionaryIds(copiedBlock, 0, 1, 0, 1, 0);
    }

    @Test
    public void testCopyPositionsSamePosition()
    {
        Slice[] expectedValues = createExpectedValues(10);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);
        int[] positionsToCopy = new int[] {52, 52, 52};

        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertThat(copiedBlock.getDictionary().getPositionCount()).isEqualTo(1);
        assertThat(copiedBlock.getPositionCount()).isEqualTo(positionsToCopy.length);

        assertBlock(copiedBlock.getDictionary(), new Slice[] {expectedValues[2]});
        assertDictionaryIds(copiedBlock, 0, 0, 0);
    }

    @Test
    public void testCopyPositionsNoCompaction()
    {
        Slice[] expectedValues = createExpectedValues(1);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 100);

        int[] positionsToCopy = new int[] {0, 2, 4, 5};
        DictionaryBlock copiedBlock = (DictionaryBlock) dictionaryBlock.copyPositions(positionsToCopy, 0, positionsToCopy.length);

        assertThat(copiedBlock.getPositionCount()).isEqualTo(positionsToCopy.length);
        assertBlock(copiedBlock.getDictionary(), expectedValues);
    }

    @Test
    public void testCompact()
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlockWithUnreferencedKeys(expectedValues, 10);

        assertThat(dictionaryBlock.isCompact()).isEqualTo(false);
        DictionaryBlock compactBlock = dictionaryBlock.compact();
        assertThat(dictionaryBlock.getDictionarySourceId())
                .isNotEqualTo(compactBlock.getDictionarySourceId());

        assertThat(compactBlock.getDictionary().getPositionCount()).isEqualTo((expectedValues.length / 2) + 1);
        assertBlock(compactBlock.getDictionary(), new Slice[] {expectedValues[0], expectedValues[1], expectedValues[3]});
        assertDictionaryIds(compactBlock, 0, 1, 1, 2, 2, 0, 1, 1, 2, 2);
        assertThat(compactBlock.isCompact()).isEqualTo(true);

        DictionaryBlock reCompactedBlock = compactBlock.compact();
        assertThat(reCompactedBlock.getDictionarySourceId()).isEqualTo(compactBlock.getDictionarySourceId());
    }

    @Test
    public void testCompactAllKeysReferenced()
    {
        Slice[] expectedValues = createExpectedValues(5);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, 10);
        DictionaryBlock compactBlock = dictionaryBlock.compact();

        // When there is nothing to compact, we return the same block
        assertThat(compactBlock.getDictionary()).isEqualTo(dictionaryBlock.getDictionary());
        assertThat(compactBlock.getPositionCount()).isEqualTo(dictionaryBlock.getPositionCount());
        for (int position = 0; position < compactBlock.getPositionCount(); position++) {
            assertThat(compactBlock.getId(position)).isEqualTo(dictionaryBlock.getId(position));
        }
        assertThat(compactBlock.isCompact()).isEqualTo(true);
    }

    @Test
    public void testBasicGetPositions()
    {
        Slice[] expectedValues = createExpectedValues(10);
        Block dictionaryBlock = DictionaryBlock.create(6, createSlicesBlock(expectedValues), new int[] {0, 1, 2, 3, 4, 5});
        assertBlock(dictionaryBlock, new Slice[] {
                expectedValues[0], expectedValues[1], expectedValues[2], expectedValues[3], expectedValues[4], expectedValues[5]});
        DictionaryId dictionaryId = ((DictionaryBlock) dictionaryBlock).getDictionarySourceId();

        // first getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 8, 1, 2, 4, 5, 7, 9}, 2, 4);
        assertBlock(dictionaryBlock, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[4], expectedValues[5]});
        assertThat(((DictionaryBlock) dictionaryBlock).getDictionarySourceId()).isEqualTo(dictionaryId);

        // second getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 1, 3, 0, 0}, 0, 3);
        assertBlock(dictionaryBlock, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[5]});
        assertThat(((DictionaryBlock) dictionaryBlock).getDictionarySourceId()).isEqualTo(dictionaryId);

        // third getPositions; we do not validate if -1 is an invalid position
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {-1, -1, 0, 1, 2}, 2, 3);
        assertBlock(dictionaryBlock, new Slice[] {expectedValues[1], expectedValues[2], expectedValues[5]});
        assertThat(((DictionaryBlock) dictionaryBlock).getDictionarySourceId()).isEqualTo(dictionaryId);

        // mixed getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {0, 2, 2}, 0, 3);
        assertBlock(dictionaryBlock, new Slice[] {expectedValues[1], expectedValues[5], expectedValues[5]});
        assertThat(((DictionaryBlock) dictionaryBlock).getDictionarySourceId()).isEqualTo(dictionaryId);

        // duplicated getPositions
        dictionaryBlock = dictionaryBlock.getPositions(new int[] {1, 1, 1, 1, 1}, 0, 5);
        assertBlock(dictionaryBlock, new Slice[] {
                expectedValues[5], expectedValues[5], expectedValues[5], expectedValues[5], expectedValues[5]});
        assertThat(((DictionaryBlock) dictionaryBlock).getDictionarySourceId()).isEqualTo(dictionaryId);

        // out of range
        final Block finalDictionaryBlock = dictionaryBlock;
        for (int position : ImmutableList.of(-1, 6)) {
            assertThatThrownBy(() -> finalDictionaryBlock.getPositions(new int[] {position}, 0, 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid position %d in block with %d positions", position, finalDictionaryBlock.getPositionCount());
        }

        for (int offset : ImmutableList.of(-1, 6)) {
            assertThatThrownBy(() -> finalDictionaryBlock.getPositions(new int[] {0}, offset, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Invalid offset %d and length 1 in array with 1 elements", offset);
        }

        for (int length : ImmutableList.of(-1, 6)) {
            assertThatThrownBy(() -> finalDictionaryBlock.getPositions(new int[] {0}, 0, length))
                    .isInstanceOf(IndexOutOfBoundsException.class)
                    .hasMessage("Invalid offset 0 and length %d in array with 1 elements", length);
        }
    }

    @Test
    public void testCompactGetPositions()
    {
        DictionaryBlock block = (DictionaryBlock) DictionaryBlock.create(6, createSlicesBlock(createExpectedValues(10)), new int[] {0, 1, 2, 3, 4, 5});
        block = block.compact();

        // 3, 3, 4, 5, 2, 0, 1, 1
        block = (DictionaryBlock) block.getPositions(new int[] {3, 3, 4, 5, 2, 0, 1, 1}, 0, 7);
        assertThat(block.isCompact()).isTrue();

        // 3, 3, 4, 5, 2, 0, 1, 1, 0, 2, 5, 4, 3
        block = (DictionaryBlock) block.getPositions(new int[] {0, 1, 2, 3, 4, 5, 6, 6, 5, 4, 3, 2, 1}, 0, 12);
        assertThat(block.isCompact()).isTrue();

        // 3, 4, 3, 4, 3
        block = (DictionaryBlock) block.getPositions(new int[] {0, 2, 0, 2, 0}, 0, 5);
        assertThat(block.isCompact()).isFalse();

        block = block.compact();
        // 3, 4, 4, 4
        block = (DictionaryBlock) block.getPositions(new int[] {0, 1, 1, 1}, 0, 4);
        assertThat(block.isCompact()).isTrue();

        // 4, 4, 4, 4
        block = (DictionaryBlock) block.getPositions(new int[] {1, 1, 1, 1}, 0, 4);
        assertThat(block.isCompact()).isFalse();

        block = block.compact();
        // 4
        block = (DictionaryBlock) block.getPositions(new int[] {0}, 0, 1);
        assertThat(block.isCompact()).isTrue();

        // empty
        block = (DictionaryBlock) block.getPositions(new int[] {}, 0, 0);
        assertThat(block.isCompact()).isFalse();

        block = block.compact();
        // empty
        block = (DictionaryBlock) block.getPositions(new int[] {}, 0, 0);
        assertThat(block.isCompact()).isTrue();
    }

    @Test
    public void testEstimatedDataSizeForStats()
    {
        int positionCount = 10;
        int dictionaryPositionCount = 100;
        Slice[] expectedValues = createExpectedValues(positionCount);
        DictionaryBlock dictionaryBlock = createDictionaryBlock(expectedValues, dictionaryPositionCount);
        for (int position = 0; position < dictionaryPositionCount; position++) {
            assertThat(dictionaryBlock.getEstimatedDataSizeForStats(position)).isEqualTo(expectedValues[position % positionCount].length());
        }
    }

    @Test
    public void testDictionarySizes()
    {
        assertDictionarySizeMethods(new IntArrayBlock(100, Optional.empty(), IntStream.range(0, 100).toArray()));
        assertDictionarySizeMethods(createSlicesBlock(createExpectedValues(100)));
    }

    private static void assertDictionarySizeMethods(Block dictionary)
    {
        assertThat(dictionary).isNotInstanceOf(DictionaryBlock.class);

        int entryCount = dictionary.getPositionCount();
        assertThat(entryCount > 0).isTrue();

        double averageEntrySize = dictionary.getSizeInBytes() / (double) entryCount;

        int[] allIds = IntStream.range(0, entryCount).toArray();
        assertThat(DictionaryBlock.create(allIds.length, dictionary, allIds).getSizeInBytes()).isEqualTo(dictionary.getSizeInBytes() + (Integer.BYTES * (long) entryCount));

        int firstHalfLength = entryCount / 2;
        int secondHalfLength = entryCount - firstHalfLength;
        int[] firstHalfIds = IntStream.range(0, firstHalfLength).toArray();
        int[] secondHalfIds = IntStream.range(firstHalfLength, entryCount).toArray();

        assertThat(DictionaryBlock.create(firstHalfIds.length, dictionary, firstHalfIds).getSizeInBytes()).isEqualTo((long) (averageEntrySize * firstHalfLength) + (Integer.BYTES * (long) firstHalfLength));
        assertThat(DictionaryBlock.create(secondHalfIds.length, dictionary, secondHalfIds).getSizeInBytes()).isEqualTo((long) (averageEntrySize * secondHalfLength) + (Integer.BYTES * (long) secondHalfLength));
        assertThat(DictionaryBlock.create(allIds.length, dictionary, allIds).getRegionSizeInBytes(0, firstHalfLength)).isEqualTo((long) (averageEntrySize * firstHalfLength) + (Integer.BYTES * (long) firstHalfLength));
        assertThat(DictionaryBlock.create(allIds.length, dictionary, allIds).getRegionSizeInBytes(firstHalfLength, secondHalfLength)).isEqualTo((long) (averageEntrySize * secondHalfLength) + (Integer.BYTES * (long) secondHalfLength));
    }

    private static DictionaryBlock createDictionaryBlockWithUnreferencedKeys(Slice[] expectedValues, int positionCount)
    {
        checkArgument(positionCount >= 2, "positionCount must be at least 2 for a dictionary block");

        // adds references to 0 and all odd indexes
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            int index = i % dictionarySize;
            if (index % 2 == 0 && index != 0) {
                index--;
            }
            ids[i] = index;
        }
        return (DictionaryBlock) DictionaryBlock.create(ids.length, createSlicesBlock(expectedValues), ids);
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        checkArgument(positionCount >= 2, "positionCount must be at least 2 for a dictionary block");
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return (DictionaryBlock) DictionaryBlock.create(ids.length, createSlicesBlock(expectedValues), ids);
    }

    private static BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 100, 1);
    }

    private static void assertDictionaryIds(DictionaryBlock dictionaryBlock, int... expected)
    {
        assertThat(dictionaryBlock.getPositionCount()).isEqualTo(expected.length);
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            assertThat(dictionaryBlock.getId(position)).isEqualTo(expected[position]);
        }
    }

    @Override
    protected <T> void assertPositionValue(Block block, int position, T expectedValue)
    {
        if (expectedValue == null) {
            assertThat(block.isNull(position)).isTrue();
            return;
        }

        assertThat(block.isNull(position)).isFalse();
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        assertThat(variableWidthBlock.getSlice(block.getUnderlyingValuePosition(position))).isEqualTo(expectedValue);
    }
}
