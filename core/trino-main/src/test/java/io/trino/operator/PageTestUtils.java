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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomDictionaryBlock;
import static io.trino.block.BlockAssertions.createRandomRleBlock;
import static io.trino.operator.PageTestUtils.Wrapping.DICTIONARY;
import static io.trino.operator.PageTestUtils.Wrapping.RUN_LENGTH;
import static io.trino.type.TypeTestUtils.getHashBlock;

public class PageTestUtils
{
    public enum Wrapping
    {
        DICTIONARY {
            @Override
            public Block wrap(Block block, int positionCount)
            {
                return createRandomDictionaryBlock(block, positionCount);
            }
        },
        RUN_LENGTH {
            @Override
            public Block wrap(Block block, int positionCount)
            {
                return createRandomRleBlock(block, positionCount);
            }
        };

        public abstract Block wrap(Block block, int positionCount);
    }

    private PageTestUtils() {}

    public static Page createRandomPage(List<Type> types, int positionCount, float nullRate)
    {
        return createRandomPage(types, positionCount, Optional.of(ImmutableList.of(0)), nullRate, Optional.empty());
    }

    public static Page createRandomDictionaryPage(List<Type> types, int positionCount, float nullRate)
    {
        return createRandomDictionaryPage(types, positionCount, positionCount, nullRate);
    }

    public static Page createRandomDictionaryPage(List<Type> types, int positionCount, int dictionaryPositionCount, float nullRate)
    {
        return createRandomPage(types, positionCount, Optional.of(ImmutableList.of(0)), nullRate, Optional.of(DICTIONARY), dictionaryPositionCount);
    }

    public static Page createRandomRlePage(List<Type> types, int positionCount, float nullRate)
    {
        return createRandomPage(types, positionCount, Optional.of(ImmutableList.of(0)), nullRate, Optional.of(RUN_LENGTH));
    }

    public static Page createRandomPage(
            List<Type> types,
            int positionCount,
            Optional<List<Integer>> hashChannels,
            float nullRate,
            Optional<Wrapping> wrapping)
    {
        return createRandomPage(types, positionCount, hashChannels, nullRate, wrapping, positionCount);
    }

    public static Page createRandomPage(
            List<Type> types,
            int positionCount,
            Optional<List<Integer>> hashChannels,
            float nullRate,
            Optional<Wrapping> wrapping,
            int wrappedPositionCount)
    {
        int channelCount = types.size();
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();

        int blockPositionCount = wrapping.isPresent() ? wrappedPositionCount : positionCount;
        for (int i = 0; i < channelCount; i++) {
            Block block = createRandomBlockForType(types.get(i), blockPositionCount, nullRate);
            blocks.add(wrapping.map(w -> w.wrap(block, positionCount)).orElse(block));
        }

        hashChannels.ifPresent(channels -> {
            ImmutableList<Block> blocksWithoutHash = blocks.build();

            blocks.add(getHashBlock(
                    channels.stream()
                            .map(types::get)
                            .collect(toImmutableList()),
                    channels.stream().map(blocksWithoutHash::get).toArray(Block[]::new)));
        });

        return new Page(positionCount, blocks.build().toArray(Block[]::new));
    }
}
