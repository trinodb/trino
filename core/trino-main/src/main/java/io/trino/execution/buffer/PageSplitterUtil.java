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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;

public final class PageSplitterUtil
{
    private static final int PAGE_SPLIT_THRESHOLD_IN_BYTES = 2 * 1024 * 1024;

    private PageSplitterUtil() {}

    public static List<Slice> splitAndSerializePage(Page page, PageSerializer serializer)
    {
        List<Page> inputPages = splitPage(page, PAGE_SPLIT_THRESHOLD_IN_BYTES);
        ImmutableList.Builder<Slice> serializedPages = ImmutableList.builderWithExpectedSize(inputPages.size());
        for (Page inputPage : inputPages) {
            serializedPages.add(serializer.serialize(inputPage));
        }
        return serializedPages.build();
    }

    public static List<Page> splitPage(Page page, long maxPageSizeInBytes)
    {
        return splitPage(page, maxPageSizeInBytes, Long.MAX_VALUE);
    }

    private static List<Page> splitPage(Page page, long maxPageSizeInBytes, long previousPageSize)
    {
        checkArgument(page.getPositionCount() > 0, "page is empty");
        checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be > 0");

        // In case the size in bytes remains constant through the recursive calls,
        // the recursion would only terminate when page.getPositionCount() == 1
        // and create potentially a large number of Page's of size 1. So we check here that
        // if the size of the page doesn't improve from the previous call we terminate the recursion.
        long currentPageSize = getPageSizeForSplit(page);
        if (currentPageSize == previousPageSize || currentPageSize <= maxPageSizeInBytes || page.getPositionCount() == 1) {
            return ImmutableList.of(page);
        }

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        int positionCount = page.getPositionCount();
        int half = positionCount / 2;

        Page leftHalf = page.getRegion(0, half);
        outputPages.addAll(splitPage(leftHalf, maxPageSizeInBytes, currentPageSize));

        Page rightHalf = page.getRegion(half, positionCount - half);
        outputPages.addAll(splitPage(rightHalf, maxPageSizeInBytes, currentPageSize));

        return outputPages.build();
    }

    /**
     * Returns the size of the page in bytes, used to decide whether to split it.
     * Unlike {@link Page#getSizeInBytes()}, an RLE block counts only its underlying value block,
     * so an RLE-only page with many positions is not split into many small pages.
     * A dictionary block counts at most its dictionary plus its ids, which bounds its serialized size.
     * The ids count only the block's own positions, so each region is smaller than its parent.
     */
    private static long getPageSizeForSplit(Page page)
    {
        long size = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            switch (block) {
                case RunLengthEncodedBlock rleBlock -> size += rleBlock.getUnderlyingValueBlock().getSizeInBytes();
                case DictionaryBlock dictionaryBlock -> size += getDictionaryBlockSizeForSplit(dictionaryBlock);
                case ValueBlock _ -> size += block.getSizeInBytes();
            }
        }
        return size;
    }

    private static long getDictionaryBlockSizeForSplit(DictionaryBlock dictionaryBlock)
    {
        long valuesSizeInBytes = min(dictionaryBlock.getSizeInBytes(), dictionaryBlock.getDictionary().getSizeInBytes());
        return valuesSizeInBytes + Integer.BYTES * (long) dictionaryBlock.getPositionCount();
    }
}
