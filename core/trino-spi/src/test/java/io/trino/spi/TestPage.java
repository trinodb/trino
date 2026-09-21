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
package io.trino.spi;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.spi.block.DictionaryBlock.createProjectedDictionaryBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPage
{
    @Test
    public void testGetRegion()
    {
        Page page = new Page(10);
        assertThat(page.getRegion(5, 5).getPositionCount()).isEqualTo(5);
        assertThat(page.getRegion(0, 10)).isSameAs(page);
    }

    @Test
    public void testGetEmptyRegion()
    {
        assertThat(new Page(0).getRegion(0, 0).getPositionCount()).isEqualTo(0);
        assertThat(new Page(10).getRegion(5, 0).getPositionCount()).isEqualTo(0);
    }

    @Test
    public void testGetRegionExceptions()
    {
        assertThatThrownBy(() -> new Page(0).getRegion(1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Invalid position 1 and length 1 in page with 0 positions");
    }

    @Test
    public void testGetRegionFromNoColumnPage()
    {
        assertThat(new Page(100).getRegion(0, 10).getPositionCount()).isEqualTo(10);
    }

    @Test
    public void testSizesForNoColumnPage()
    {
        Page page = new Page(100);
        assertThat(page.getSizeInBytes()).isEqualTo(0);
    }

    @Test
    public void testCompactDictionaryBlocks()
    {
        int positionCount = 100;

        // Create 2 dictionary blocks with the same source id
        DictionaryId commonSourceId = randomDictionaryId();
        int commonDictionaryUsedPositions = 20;
        int[] commonDictionaryIds = getDictionaryIds(positionCount, commonDictionaryUsedPositions);

        // first dictionary contains "varbinary" values
        Slice[] dictionaryValues1 = createExpectedValues(50);
        Block dictionary1 = createSlicesBlock(dictionaryValues1);
        Block commonSourceIdBlock1 = createProjectedDictionaryBlock(positionCount, dictionary1, commonDictionaryIds, commonSourceId);

        // second dictionary block is "length(firstColumn)"
        Block commonSourceIdBlock2 = createProjectedDictionaryBlock(positionCount, createLengthsBlock(dictionaryValues1), commonDictionaryIds, commonSourceId);

        // Create block with a different source id, dictionary size, used
        int otherDictionaryUsedPositions = 30;
        int[] otherDictionaryIds = getDictionaryIds(positionCount, otherDictionaryUsedPositions);
        Block dictionary3 = createSlicesBlock(createExpectedValues(70));
        Block randomSourceIdBlock = DictionaryBlock.create(otherDictionaryIds.length, dictionary3, otherDictionaryIds);

        Page page = new Page(commonSourceIdBlock1, randomSourceIdBlock, commonSourceIdBlock2);
        page.compact();

        // dictionary blocks should all be compact
        assertThat(((DictionaryBlock) page.getBlock(0)).isCompact()).isTrue();
        assertThat(((DictionaryBlock) page.getBlock(1)).isCompact()).isTrue();
        assertThat(((DictionaryBlock) page.getBlock(2)).isCompact()).isTrue();
        assertThat(((DictionaryBlock) page.getBlock(0)).getDictionary().getPositionCount()).isEqualTo(commonDictionaryUsedPositions);
        assertThat(((DictionaryBlock) page.getBlock(1)).getDictionary().getPositionCount()).isEqualTo(otherDictionaryUsedPositions);
        assertThat(((DictionaryBlock) page.getBlock(2)).getDictionary().getPositionCount()).isEqualTo(commonDictionaryUsedPositions);

        // Blocks that had the same source id before compacting page should have the same source id after compacting page
        assertThat(((DictionaryBlock) page.getBlock(0)).getDictionarySourceId())
                .isNotEqualTo(((DictionaryBlock) page.getBlock(1)).getDictionarySourceId());

        assertThat(((DictionaryBlock) page.getBlock(0)).getDictionarySourceId())
                .isEqualTo(((DictionaryBlock) page.getBlock(2)).getDictionarySourceId());
    }

    @Test
    public void testCompactSingleEntryRelatedDictionaryBlocks()
    {
        int positionCount = 100;
        DictionaryId commonSourceId = randomDictionaryId();
        int[] dictionaryIds = new int[positionCount];

        Slice[] dictionaryValues = createExpectedValues(1000);
        int dictionaryPosition = 723;
        Arrays.fill(dictionaryIds, dictionaryPosition);
        Block dictionary1 = createSlicesBlock(dictionaryValues);
        Block dictionaryBlock1 = createProjectedDictionaryBlock(positionCount, dictionary1, dictionaryIds, commonSourceId);

        Block dictionaryBlock2 = createProjectedDictionaryBlock(positionCount, createLengthsBlock(dictionaryValues), dictionaryIds, commonSourceId);

        Page page = new Page(dictionaryBlock1, dictionaryBlock2);
        page.compact();

        assertThat(page.getBlock(0)).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(page.getBlock(1)).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(VARBINARY.getSlice(page.getBlock(0), 0)).isEqualTo(dictionaryValues[dictionaryPosition]);
        assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(dictionaryValues[dictionaryPosition].length());
    }

    @Test
    public void testCompactIdentityRelatedDictionaryBlocks()
    {
        int positionCount = 100;
        DictionaryId commonSourceId = randomDictionaryId();
        int[] dictionaryIds = new int[positionCount];
        Arrays.setAll(dictionaryIds, position -> position);

        Slice[] dictionaryValues = createExpectedValues(positionCount);
        Block dictionary1 = createSlicesBlock(dictionaryValues);
        Block dictionary2 = createLengthsBlock(dictionaryValues);
        Block dictionaryBlock1 = createProjectedDictionaryBlock(positionCount, dictionary1, dictionaryIds, commonSourceId);
        Block dictionaryBlock2 = createProjectedDictionaryBlock(positionCount, dictionary2, dictionaryIds, commonSourceId);

        Page page = new Page(dictionaryBlock1, dictionaryBlock2);
        page.compact();

        assertThat(page.getBlock(0)).isSameAs(dictionary1);
        assertThat(page.getBlock(1)).isSameAs(dictionary2);
    }

    @Test
    public void testCompactIdentityRelatedDictionaryBlocksWithDifferentSources()
    {
        int positionCount = 2;
        Slice[] dictionaryValues = createExpectedValues(positionCount);
        Block dictionary = createSlicesBlock(dictionaryValues);
        DictionaryBlock firstBlock = (DictionaryBlock) createProjectedDictionaryBlock(
                positionCount,
                dictionary,
                new int[] {0, 1},
                randomDictionaryId());
        DictionaryBlock secondBlock = (DictionaryBlock) createProjectedDictionaryBlock(
                positionCount,
                dictionary,
                new int[] {1, 0},
                randomDictionaryId());

        assertThatThrownBy(() -> DictionaryBlock.compactRelatedBlocks(ImmutableList.of(firstBlock, secondBlock)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dictionarySourceIds must be the same");
    }

    @Test
    public void testCompactUniqueRelatedDictionaryBlocks()
    {
        int positionCount = 100;
        DictionaryId commonSourceId = randomDictionaryId();
        int[] dictionaryIds = new int[positionCount];
        Arrays.setAll(dictionaryIds, position -> position * 2);

        Slice[] dictionaryValues = createExpectedValues(positionCount * 2);
        Block dictionaryBlock1 = createProjectedDictionaryBlock(positionCount, createSlicesBlock(dictionaryValues), dictionaryIds, commonSourceId);
        Block dictionaryBlock2 = createProjectedDictionaryBlock(positionCount, createLengthsBlock(dictionaryValues), dictionaryIds, commonSourceId);

        Page page = new Page(dictionaryBlock1, dictionaryBlock2);
        page.compact();

        assertThat(page.getBlock(0)).isInstanceOf(VariableWidthBlock.class);
        assertThat(page.getBlock(1)).isInstanceOf(LongArrayBlock.class);
        assertThat(VARBINARY.getSlice(page.getBlock(0), 50)).isEqualTo(dictionaryValues[100]);
        assertThat(BIGINT.getLong(page.getBlock(1), 50)).isEqualTo(dictionaryValues[100].length());
    }

    @Test
    public void testGetPositions()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();

        Page page = new Page(block, block, block).getPositions(new int[] {0, 1, 1, 1, 2, 5, 5}, 1, 5);
        assertThat(page.getPositionCount()).isEqualTo(5);
        for (int i = 0; i < 3; i++) {
            Block testBlock = page.getBlock(i);
            assertThat(BIGINT.getLong(testBlock, 0)).isEqualTo(1);
            assertThat(BIGINT.getLong(testBlock, 1)).isEqualTo(1);
            assertThat(BIGINT.getLong(testBlock, 2)).isEqualTo(1);
            assertThat(BIGINT.getLong(testBlock, 3)).isEqualTo(2);
            assertThat(BIGINT.getLong(testBlock, 4)).isEqualTo(5);
        }
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    private static int[] getDictionaryIds(int positionCount, int dictionarySize)
    {
        checkArgument(positionCount > dictionarySize);
        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return ids;
    }

    private static Block createSlicesBlock(Slice[] values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 100);

        for (Slice value : values) {
            verifyNotNull(value);
            VARBINARY.writeSlice(builder, value);
        }
        return builder.build();
    }

    private static Block createLengthsBlock(Slice[] values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (Slice value : values) {
            BIGINT.writeLong(builder, value.length());
        }
        return builder.build();
    }
}
