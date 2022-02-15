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
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InterpretedHashGenerator
        implements HashGenerator
{
    private final List<Type> hashChannelTypes;
    @Nullable
    private final int[] hashChannels; // null value indicates that the identity channel mapping is used
    private final BlockPositionHashCode[] hashCodeOperators;

    public static InterpretedHashGenerator createPositionalWithTypes(List<Type> hashChannelTypes, BlockTypeOperators blockTypeOperators)
    {
        return new InterpretedHashGenerator(hashChannelTypes, null, blockTypeOperators, true);
    }

    public InterpretedHashGenerator(List<Type> hashChannelTypes, List<Integer> hashChannels, BlockTypeOperators blockTypeOperators)
    {
        this(hashChannelTypes, Ints.toArray(requireNonNull(hashChannels, "hashChannels is null")), blockTypeOperators);
    }

    public InterpretedHashGenerator(List<Type> hashChannelTypes, int[] hashChannels, BlockTypeOperators blockTypeOperators)
    {
        this(hashChannelTypes, requireNonNull(hashChannels, "hashChannels is null"), blockTypeOperators, false);
    }

    private InterpretedHashGenerator(List<Type> hashChannelTypes, @Nullable int[] hashChannels, BlockTypeOperators blockTypeOperators, boolean positional)
    {
        this.hashChannelTypes = ImmutableList.copyOf(requireNonNull(hashChannelTypes, "hashChannelTypes is null"));
        this.hashCodeOperators = createHashCodeOperators(hashChannelTypes, blockTypeOperators);
        checkArgument(hashCodeOperators.length == hashChannelTypes.size());
        if (positional) {
            checkArgument(hashChannels == null, "hashChannels must be null");
            this.hashChannels = null;
        }
        else {
            requireNonNull(hashChannels, "hashChannels is null");
            checkArgument(hashChannels.length == hashCodeOperators.length);
            // simple positional indices are converted to null
            this.hashChannels = isPositionalChannels(hashChannels) ? null : hashChannels;
        }
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, IntFunction<Block> blockProvider)
        long result = 0;
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Block block = page.getBlock(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, hashCodeOperators[i].hashCodeNullSafe(block, position));
        }
        return result;
    }

    public long hashPosition(int position, IntFunction<Block> blockProvider)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, Page page)
        long result = 0;
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Block block = blockProvider.apply(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, hashCodeOperators[i].hashCodeNullSafe(block, position));
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hashChannelTypes", hashChannelTypes)
                .add("hashChannels", hashChannels == null ? "<identity>" : Arrays.toString(hashChannels))
                .toString();
    }

    private static boolean isPositionalChannels(int[] hashChannels)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            if (hashChannels[i] != i) {
                return false; // hashChannels is not a simple positional identity mapping
            }
        }
        return true;
    }

    private static BlockPositionHashCode[] createHashCodeOperators(List<Type> hashChannelTypes, BlockTypeOperators blockTypeOperators)
    {
        requireNonNull(hashChannelTypes, "hashChannelTypes is null");
        requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        BlockPositionHashCode[] hashCodeOperators = new BlockPositionHashCode[hashChannelTypes.size()];
        for (int i = 0; i < hashCodeOperators.length; i++) {
            hashCodeOperators[i] = blockTypeOperators.getHashCodeOperator(hashChannelTypes.get(i));
        }
        return hashCodeOperators;
    }
}
