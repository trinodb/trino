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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.optimizations.HashGenerationOptimizer;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

// TODO this class could be made more efficient by replacing the hashChannels array making the channel a constant in the
//  method handle. Additionally, the method handles could be combined into a single method handle using method handle
//  combinators. To do all of this, we would need to add a cache for instances of this class since the method handles
//  would be modified for each instance.
public class InterpretedHashGenerator
        implements HashGenerator
{
    private final List<Type> hashChannelTypes;
    @Nullable
    private final int[] hashChannels; // null value indicates that the identity channel mapping is used
    private final MethodHandle[] hashCodeOperators;

    public static InterpretedHashGenerator createPagePrefixHashGenerator(List<Type> hashChannelTypes, TypeOperators typeOperators)
    {
        return new InterpretedHashGenerator(hashChannelTypes, null, typeOperators);
    }

    public static InterpretedHashGenerator createChannelsHashGenerator(List<Type> hashChannelTypes, int[] hashChannels, TypeOperators typeOperators)
    {
        return new InterpretedHashGenerator(hashChannelTypes, hashChannels, typeOperators);
    }

    private InterpretedHashGenerator(List<Type> hashChannelTypes, @Nullable int[] hashChannels, TypeOperators blockTypeOperators)
    {
        this.hashChannelTypes = ImmutableList.copyOf(requireNonNull(hashChannelTypes, "hashChannelTypes is null"));
        this.hashCodeOperators = new MethodHandle[hashChannelTypes.size()];
        for (int i = 0; i < hashCodeOperators.length; i++) {
            hashCodeOperators[i] = blockTypeOperators.getHashCodeOperator(hashChannelTypes.get(i), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));
        }
        if (hashChannels == null) {
            this.hashChannels = null;
        }
        else {
            checkArgument(hashChannels.length == hashCodeOperators.length);
            // simple positional indices are converted to null
            this.hashChannels = isPositionalChannels(hashChannels) ? null : hashChannels;
        }
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, IntFunction<Block> blockProvider)
        long result = HashGenerationOptimizer.INITIAL_HASH_VALUE;
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Block block = page.getBlock(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, nullSafeHash(i, block, position));
        }
        return result;
    }

    public long hashPosition(int position, IntFunction<Block> blockProvider)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, Page page)
        long result = HashGenerationOptimizer.INITIAL_HASH_VALUE;
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Block block = blockProvider.apply(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, nullSafeHash(i, block, position));
        }
        return result;
    }

    private long nullSafeHash(int operatorIndex, Block block, int position)
    {
        try {
            return block.isNull(position) ? NULL_HASH_CODE : (long) hashCodeOperators[operatorIndex].invokeExact(block, position);
        }
        catch (Throwable e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
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
}
