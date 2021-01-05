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
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionEqualsAndHash
        implements PageWithPositionEqualsAndHash
{
    private final IntList equalityChannels;
    private final List<BlockPositionIsDistinctFrom> distinctFromOperators;
    private final List<BlockPositionHashCode> hashOperators;

    public SimplePageWithPositionEqualsAndHash(List<Type> channelTypes, List<Integer> equalityChannels, BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(channelTypes, "channelTypes is null");
        this.equalityChannels = new IntArrayList(requireNonNull(equalityChannels, "equalityChannels is null"));
        checkArgument(channelTypes.size() >= equalityChannels.size(), "channelTypes cannot have fewer columns then equalityChannels");

        // Use IS DISTINCT FROM for equality, because it evaluates NULL and NaN values as distinct (unlike SQL EQUALS)
        ImmutableList.Builder<BlockPositionIsDistinctFrom> distinctFromOperators = ImmutableList.builder();
        ImmutableList.Builder<BlockPositionHashCode> hashOperators = ImmutableList.builder();
        for (int index = 0; index < equalityChannels.size(); index++) {
            Type type = channelTypes.get(this.equalityChannels.getInt(index));
            distinctFromOperators.add(blockTypeOperators.getDistinctFromOperator(type));
            hashOperators.add(blockTypeOperators.getHashCodeOperator(type));
        }
        this.distinctFromOperators = distinctFromOperators.build();
        this.hashOperators = hashOperators.build();
    }

    @Override
    public boolean equals(Page left, int leftPosition, Page right, int rightPosition)
    {
        for (int i = 0; i < equalityChannels.size(); i++) {
            int equalityChannel = equalityChannels.getInt(i);
            Block leftBlock = left.getBlock(equalityChannel);
            Block rightBlock = right.getBlock(equalityChannel);
            if (distinctFromOperators.get(i).isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long hashCode(Page page, int position)
    {
        long hashCode = 0;
        for (int i = 0; i < equalityChannels.size(); i++) {
            int equalityChannel = equalityChannels.getInt(i);
            Block block = page.getBlock(equalityChannel);
            hashCode = 31 * hashCode + hashOperators.get(i).hashCodeNullSafe(block, position);
        }
        return hashCode;
    }
}
