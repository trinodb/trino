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
import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private static final int INSTANCE_SIZE = instanceSize(SimplePagesHashStrategy.class);
    private final Type[] types;
    @Nullable
    private final BlockPositionComparison comparisonOperator; // null when sort channel is absent
    private final int[] outputChannels;
    private final List<ObjectArrayList<Block>> channels;
    private final int[] hashChannels;
    private final OptionalInt sortChannel;
    private final BlockPositionEqual[] equalOperators;
    private final BlockPositionHashCode[] hashCodeOperators;
    private final BlockPositionIsIdentical[] identicalOperators;
    private long channelsRetainedSize = -1;

    public SimplePagesHashStrategy(
            List<Type> types,
            List<Integer> outputChannels,
            List<ObjectArrayList<Block>> channels,
            List<Integer> hashChannels,
            Optional<Integer> sortChannel,
            BlockTypeOperators blockTypeOperators)
    {
        this.types = toTypesArray(requireNonNull(types, "types is null"));
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));

        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = Ints.toArray(requireNonNull(hashChannels, "hashChannels is null"));
        this.sortChannel = requireNonNull(sortChannel, "sortChannel is null").isEmpty() ? OptionalInt.empty() : OptionalInt.of(sortChannel.get());
        if (this.sortChannel.isPresent() && this.types[this.sortChannel.getAsInt()].isOrderable()) {
            this.comparisonOperator = blockTypeOperators.getComparisonUnorderedLastOperator(this.types[this.sortChannel.getAsInt()]);
        }
        else {
            this.comparisonOperator = null;
        }

        this.equalOperators = new BlockPositionEqual[this.hashChannels.length];
        this.hashCodeOperators = new BlockPositionHashCode[this.hashChannels.length];
        this.identicalOperators = new BlockPositionIsIdentical[this.hashChannels.length];
        for (int i = 0; i < this.hashChannels.length; i++) {
            Type type = this.types[this.hashChannels[i]];
            equalOperators[i] = blockTypeOperators.getEqualOperator(type);
            hashCodeOperators[i] = blockTypeOperators.getHashCodeOperator(type);
            identicalOperators[i] = blockTypeOperators.getIdenticalOperator(type);
        }
    }

    @Override
    public int getChannelCount()
    {
        return outputChannels.length;
    }

    @Override
    public long getSizeInBytes()
    {
        if (channelsRetainedSize < 0) {
            // compute retained size on first access
            channelsRetainedSize = 0;
            for (ObjectArrayList<Block> blocks : channels) {
                channelsRetainedSize += sizeOf(blocks.elements());
                for (Block block : blocks) {
                    channelsRetainedSize += block.getRetainedSizeInBytes();
                }
            }
        }
        return INSTANCE_SIZE + channelsRetainedSize;
    }

    @Override
    public void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int outputIndex : outputChannels) {
            Type type = types[outputIndex];
            List<Block> channel = channels.get(outputIndex);
            Block block = channel.get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public long hashPosition(int blockIndex, int position)
    {
        long result = 0;
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = channels.get(hashChannels[i]).get(blockIndex);
            result = result * 31 + hashCodeOperators[i].hashCodeNullSafe(block, position);
        }
        return result;
    }

    @Override
    public long hashRow(int position, Page page)
    {
        long result = 0;
        for (int i = 0; i < hashChannels.length; i++) {
            Block block = page.getBlock(i);
            result = result * 31 + hashCodeOperators[i].hashCodeNullSafe(block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperators[i].equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean rowIdenticalToRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (!identicalOperators[i].isIdentical(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            Block leftBlock = channels.get(hashChannels[i]).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperators[i].equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionIdenticalToRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            Block leftBlock = channels.get(hashChannels[i]).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!identicalOperators[i].isIdentical(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            BlockPositionEqual equalOperator = equalOperators[i];
            Block leftBlock = channels.get(hashChannels[i]).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperator.equal(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionIdenticalToRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            int hashChannel = hashChannels[i];
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightChannels[i]);
            BlockPositionIsIdentical identical = identicalOperators[i];
            if (!identical.isIdentical(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            List<Block> channel = channels.get(hashChannels[i]);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!equalOperators[i].equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionIdenticalToPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            List<Block> channel = channels.get(hashChannels[i]);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!identicalOperators[i].isIdentical(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            List<Block> channel = channels.get(hashChannels[i]);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!equalOperators[i].equal(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPositionNull(int blockIndex, int blockPosition)
    {
        for (int hashChannel : hashChannels) {
            if (isChannelPositionNull(hashChannel, blockIndex, blockPosition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        int channel = getSortChannel();
        Block leftBlock = channels.get(channel).get(leftBlockIndex);
        Block rightBlock = channels.get(channel).get(rightBlockIndex);

        if (comparisonOperator == null) {
            throw new IllegalArgumentException("type is not orderable");
        }
        return (int) comparisonOperator.compare(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
    }

    @Override
    public boolean isSortChannelPositionNull(int blockIndex, int blockPosition)
    {
        return isChannelPositionNull(getSortChannel(), blockIndex, blockPosition);
    }

    private boolean isChannelPositionNull(int channelIndex, int blockIndex, int blockPosition)
    {
        List<Block> channel = channels.get(channelIndex);
        Block block = channel.get(blockIndex);
        return block.isNull(blockPosition);
    }

    private int getSortChannel()
    {
        return sortChannel.getAsInt();
    }

    private static Type[] toTypesArray(List<Type> types)
    {
        Type[] array = types.toArray(Type[]::new);
        for (Type type : array) {
            if (type == null) {
                throw new IllegalArgumentException("types contains null element");
            }
        }
        return array;
    }
}
