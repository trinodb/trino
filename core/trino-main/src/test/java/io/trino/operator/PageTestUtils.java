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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.block.BlockAssertions;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.block.BlockAssertions.Encoding.DICTIONARY;
import static io.trino.block.BlockAssertions.Encoding.RUN_LENGTH;
import static io.trino.block.BlockAssertions.createAllNullsBlock;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomLongsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;

public class PageTestUtils
{
    public static Page createPageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of());
    }

    public static Page createDictionaryPageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of(DICTIONARY));
    }

    public static Page createRlePageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of(RUN_LENGTH));
    }

    public static Page createPageWithRandomData(
            List<Type> types,
            int positionCount,
            boolean addPreComputedHashBlock,
            boolean addNullBlock,
            float primitiveNullRate,
            float nestedNullRate,
            boolean useBlockView,
            List<BlockAssertions.Encoding> wrappings)
    {
        int channelCount = types.size();

        List<Block> blocks = new ArrayList<>();

        if (addPreComputedHashBlock) {
            blocks.add(createRandomLongsBlock(positionCount, 0.0f));
        }

        for (int i = 0; i < channelCount; i++) {
            blocks.add(createRandomBlockForType(types.get(i), positionCount, primitiveNullRate, nestedNullRate, useBlockView, wrappings));
        }

        if (addNullBlock) {
            blocks.add(createAllNullsBlock(BIGINT, positionCount));
        }

        return new Page(positionCount, blocks.toArray(new Block[0]));
    }

    public static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int totalPositionCount = 0;
        for (Page page : pages) {
            verify(page.getChannelCount() == types.size(), format("Number of channels in page %d is not equal to number of types %d", page.getChannelCount(), types.size()));

            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writePositionTo(position, blockBuilder);
                    }
                }
            }
            totalPositionCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(totalPositionCount);
        return pageBuilder.build();
    }

    public static List<Type> createUpdatedBlockTypesWithHashBlockAndNullBlock(List<Type> types, boolean addPreComputedHashBlock, boolean addNullBlock)
    {
        ImmutableList.Builder<Type> newTypes = ImmutableList.builder();

        if (addPreComputedHashBlock) {
            newTypes.add(BIGINT);
        }

        newTypes.addAll(types);

        if (addNullBlock) {
            newTypes.add(BIGINT);
        }

        return newTypes.build();
    }

    private PageTestUtils()
    {
    }
}
