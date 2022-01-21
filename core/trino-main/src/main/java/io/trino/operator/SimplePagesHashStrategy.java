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
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionComparison;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SimplePagesHashStrategy.class).instanceSize();
    private final List<Type> types;
    private final List<Optional<BlockPositionComparison>> comparisonOperators;
    private final List<Integer> outputChannels;
    private final List<List<Block>> channels;
    private final List<Integer> hashChannels;
    private final List<Block> precomputedHashChannel;
    private final Optional<Integer> sortChannel;
    private final List<BlockPositionEqual> equalOperators;
    private final List<BlockPositionHashCode> hashCodeOperators;
    private final List<BlockPositionIsDistinctFrom> isDistinctFromOperators;

    public SimplePagesHashStrategy(
            List<Type> types,
            List<Integer> outputChannels,
            List<List<Block>> channels,
            List<Integer> hashChannels,
            OptionalInt precomputedHashChannel,
            Optional<Integer> sortChannel,
            BlockTypeOperators blockTypeOperators)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.comparisonOperators = types.stream()
                .map(type -> type.isOrderable() ? Optional.of(blockTypeOperators.getComparisonUnorderedLastOperator(type)) : Optional.<BlockPositionComparison>empty())
                .collect(toImmutableList());
        this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));

        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        if (precomputedHashChannel.isPresent()) {
            this.precomputedHashChannel = channels.get(precomputedHashChannel.getAsInt());
        }
        else {
            this.precomputedHashChannel = null;
        }
        this.sortChannel = requireNonNull(sortChannel, "sortChannel is null");

        this.equalOperators = hashChannels.stream()
                .map(types::get)
                .map(blockTypeOperators::getEqualOperator)
                .collect(toImmutableList());
        this.hashCodeOperators = hashChannels.stream()
                .map(types::get)
                .map(blockTypeOperators::getHashCodeOperator)
                .collect(toImmutableList());
        this.isDistinctFromOperators = hashChannels.stream()
                .map(types::get)
                .map(blockTypeOperators::getDistinctFromOperator)
                .collect(toImmutableList());
    }

    @Override
    public int getChannelCount()
    {
        return outputChannels.size();
    }

    @Override
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + channels.stream()
                .flatMap(List::stream)
                .mapToLong(Block::getRetainedSizeInBytes)
                .sum();
    }

    @Override
    public void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int outputIndex : outputChannels) {
            Type type = types.get(outputIndex);
            List<Block> channel = channels.get(outputIndex);
            Block block = channel.get(blockIndex);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public long hashPosition(int blockIndex, int position)
    {
        if (precomputedHashChannel != null) {
            return BIGINT.getLong(precomputedHashChannel.get(blockIndex), position);
        }
        long result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            Block block = channels.get(hashChannels.get(i)).get(blockIndex);
            result = result * 31 + hashCodeOperators.get(i).hashCodeNullSafe(block, position);
        }
        return result;
    }

    @Override
    public long hashRow(int position, Page page)
    {
        long result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            Block block = page.getBlock(i);
            result = result * 31 + hashCodeOperators.get(i).hashCodeNullSafe(block, position);
        }
        return result;
    }

    @Override
    public boolean rowEqualsRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperators.get(i).equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean rowNotDistinctFromRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            Block leftBlock = leftPage.getBlock(i);
            Block rightBlock = rightPage.getBlock(i);
            if (isDistinctFromOperators.get(i).isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            Block leftBlock = channels.get(hashChannels.get(i)).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperators.get(i).equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionNotDistinctFromRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            Block leftBlock = channels.get(hashChannels.get(i)).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (isDistinctFromOperators.get(i).isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            BlockPositionEqual equalOperator = equalOperators.get(i);
            Block leftBlock = channels.get(hashChannels.get(i)).get(leftBlockIndex);
            Block rightBlock = rightPage.getBlock(i);
            if (!equalOperator.equal(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionNotDistinctFromRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            Block rightBlock = page.getBlock(rightChannels[i]);
            BlockPositionIsDistinctFrom isDistinctFromOperator = isDistinctFromOperators.get(i);
            if (isDistinctFromOperator.isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            List<Block> channel = channels.get(hashChannels.get(i));
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!equalOperators.get(i).equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionNotDistinctFromPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            List<Block> channel = channels.get(hashChannels.get(i));
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (isDistinctFromOperators.get(i).isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            List<Block> channel = channels.get(hashChannels.get(i));
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!equalOperators.get(i).equal(leftBlock, leftPosition, rightBlock, rightPosition)) {
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

        return (int) comparisonOperators.get(channel)
            .orElseThrow(() -> new IllegalArgumentException("type is not orderable"))
            .compare(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
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
        return sortChannel.get();
    }
}
