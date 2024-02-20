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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.LazyBlock;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.DictionaryBlock.createProjectedDictionaryBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPage
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
        assertThat(page.getLogicalSizeInBytes()).isEqualTo(0);
        assertThat(page.getRetainedSizeInBytes()).isEqualTo(Page.INSTANCE_SIZE); // does not include the blocks array
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
        BlockBuilder dictionary2 = BIGINT.createBlockBuilder(null, dictionary1.getPositionCount());
        for (Slice expectedValue : dictionaryValues1) {
            BIGINT.writeLong(dictionary2, expectedValue.length());
        }
        Block commonSourceIdBlock2 = createProjectedDictionaryBlock(positionCount, dictionary2.build(), commonDictionaryIds, commonSourceId);

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
    public void testGetPositions()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
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

    @Test
    public void testGetLoadedPage()
    {
        int entries = 10;
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();

        LazyBlock lazyBlock = lazyWrapper(block);
        Page page = new Page(lazyBlock);
        long lazyPageRetainedSize = Page.INSTANCE_SIZE + sizeOf(new Block[] {block}) + lazyBlock.getRetainedSizeInBytes();
        assertThat(page.getRetainedSizeInBytes()).isEqualTo(lazyPageRetainedSize);
        Page loadedPage = page.getLoadedPage();
        // Retained size of page remains the same
        assertThat(page.getRetainedSizeInBytes()).isEqualTo(lazyPageRetainedSize);
        long loadedPageRetainedSize = Page.INSTANCE_SIZE + sizeOf(new Block[] {block}) + block.getRetainedSizeInBytes();
        // Retained size of loaded page depends on the loaded block
        assertThat(loadedPage.getRetainedSizeInBytes()).isEqualTo(loadedPageRetainedSize);

        lazyBlock = lazyWrapper(block);
        page = new Page(lazyBlock);
        assertThat(page.getRetainedSizeInBytes()).isEqualTo(lazyPageRetainedSize);
        loadedPage = page.getLoadedPage(new int[] {0}, new int[] {0});
        // Retained size of page is updated based on loaded block
        assertThat(page.getRetainedSizeInBytes()).isEqualTo(loadedPageRetainedSize);
        assertThat(loadedPage.getRetainedSizeInBytes()).isEqualTo(loadedPageRetainedSize);
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
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
}
