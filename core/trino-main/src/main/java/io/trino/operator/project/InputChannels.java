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
package io.trino.operator.project;

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InputChannels
{
    private final int[] inputChannels;
    @Nullable
    private final boolean[] eagerlyLoad;

    public InputChannels(int... inputChannels)
    {
        this.inputChannels = inputChannels.clone();
        this.eagerlyLoad = null;
    }

    public InputChannels(List<Integer> inputChannels)
    {
        this.inputChannels = inputChannels.stream().mapToInt(Integer::intValue).toArray();
        this.eagerlyLoad = null;
    }

    public InputChannels(List<Integer> inputChannels, Set<Integer> eagerlyLoadedChannels)
    {
        this.inputChannels = inputChannels.stream().mapToInt(Integer::intValue).toArray();
        this.eagerlyLoad = new boolean[this.inputChannels.length];
        for (int i = 0; i < this.inputChannels.length; i++) {
            eagerlyLoad[i] = eagerlyLoadedChannels.contains(this.inputChannels[i]);
        }
    }

    public int size()
    {
        return inputChannels.length;
    }

    public List<Integer> getInputChannels()
    {
        return Collections.unmodifiableList(Ints.asList(inputChannels));
    }

    public SourcePage getInputChannels(SourcePage page)
    {
        return new InputChannelsSourcePage(page, inputChannels, eagerlyLoad);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(Arrays.toString(inputChannels))
                .toString();
    }

    private static final class InputChannelsSourcePage
            implements SourcePage
    {
        private final SourcePage sourcePage;
        private final int[] channels;
        private final Block[] blocks;

        private InputChannelsSourcePage(SourcePage sourcePage, int[] channels, @Nullable boolean[] eagerlyLoad)
        {
            requireNonNull(sourcePage, "sourcePage is null");
            requireNonNull(channels, "channels is null");

            this.sourcePage = sourcePage;
            this.channels = channels;
            this.blocks = new Block[channels.length];

            if (eagerlyLoad != null) {
                for (int channel = 0; channel < eagerlyLoad.length; channel++) {
                    if (eagerlyLoad[channel]) {
                        this.blocks[channel] = sourcePage.getBlock(channels[channel]).getLoadedBlock();
                    }
                }
            }
        }

        @Override
        public int getPositionCount()
        {
            return sourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sourcePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return sourcePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            Block block = blocks[channel];
            if (block == null) {
                block = sourcePage.getBlock(channels[channel]);
                blocks[channel] = block;
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            for (int i = 0; i < blocks.length; i++) {
                getBlock(i);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            Block[] blocks = new Block[channels.length];
            for (int i = 0; i < channels.length; i++) {
                blocks[i] = getBlock(channels[i]);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            sourcePage.selectPositions(positions, offset, size);
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block != null) {
                    blocks[i] = block.getPositions(positions, offset, size);
                }
            }
        }
    }
}
