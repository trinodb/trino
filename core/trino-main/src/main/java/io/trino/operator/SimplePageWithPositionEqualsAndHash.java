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

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionEqualsAndHash
        implements PageWithPositionEqualsAndHash
{
    private final int[] equalityChannels;
    private final BlockPositionIsIdentical[] identicalOperators;
    private final BlockPositionHashCode[] hashOperators;

    public SimplePageWithPositionEqualsAndHash(List<Type> channelTypes, List<Integer> equalityChannels, BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(channelTypes, "channelTypes is null");
        checkArgument(channelTypes.size() == equalityChannels.size(), "channelTypes and equalityChannels must have the same size");
        this.equalityChannels = Ints.toArray(requireNonNull(equalityChannels, "equalityChannels is null"));

        // Use IS DISTINCT FROM for equality, because it evaluates NULL and NaN values as distinct (unlike SQL EQUALS)
        this.identicalOperators = new BlockPositionIsIdentical[this.equalityChannels.length];
        this.hashOperators = new BlockPositionHashCode[this.equalityChannels.length];
        for (int index = 0; index < this.identicalOperators.length; index++) {
            Type type = channelTypes.get(index);
            identicalOperators[index] = blockTypeOperators.getIdenticalOperator(type);
            hashOperators[index] = blockTypeOperators.getHashCodeOperator(type);
        }
    }

    @Override
    public boolean equals(Page left, int leftPosition, Page right, int rightPosition)
    {
        for (int i = 0; i < equalityChannels.length; i++) {
            int equalityChannel = equalityChannels[i];
            Block leftBlock = left.getBlock(equalityChannel);
            Block rightBlock = right.getBlock(equalityChannel);
            if (!identicalOperators[i].isIdentical(leftBlock, leftPosition, rightBlock, rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long hashCode(Page page, int position)
    {
        long hashCode = 0;
        for (int i = 0; i < equalityChannels.length; i++) {
            int equalityChannel = equalityChannels[i];
            Block block = page.getBlock(equalityChannel);
            hashCode = 31 * hashCode + hashOperators[i].hashCodeNullSafe(block, position);
        }
        return hashCode;
    }
}
