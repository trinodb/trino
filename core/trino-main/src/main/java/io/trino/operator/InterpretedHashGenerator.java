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
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InterpretedHashGenerator
        implements HashGenerator
{
    private final List<Type> hashChannelTypes;
    @Nullable
    private final int[] hashChannels; // null value indicates that the identity channel mapping is used
    private final NullSafeHash[] hashCodeOperators;

    public static InterpretedHashGenerator createPagePrefixHashGenerator(List<Type> hashChannelTypes, NullSafeHashCompiler hashCompiler)
    {
        return new InterpretedHashGenerator(hashChannelTypes, null, hashCompiler);
    }

    public static InterpretedHashGenerator createChannelsHashGenerator(List<Type> hashChannelTypes, int[] hashChannels, NullSafeHashCompiler hashCompiler)
    {
        return new InterpretedHashGenerator(hashChannelTypes, hashChannels, hashCompiler);
    }

    private InterpretedHashGenerator(List<Type> hashChannelTypes, @Nullable int[] hashChannels, NullSafeHashCompiler hashCompiler)
    {
        this.hashChannelTypes = ImmutableList.copyOf(requireNonNull(hashChannelTypes, "hashChannelTypes is null"));
        this.hashCodeOperators = new NullSafeHash[hashChannelTypes.size()];
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Type type = hashChannelTypes.get(i);
            hashCodeOperators[i] = hashCompiler.compileHash(type);
        }
        if (hashChannels == null) {
            this.hashChannels = null;
        }
        else {
            checkArgument(hashChannels.length == hashChannelTypes.size());
            // simple positional indices are converted to null
            this.hashChannels = isPositionalChannels(hashChannels) ? null : hashChannels;
        }
    }

    @Override
    public void hash(Page page, int positionOffset, int length, long[] hashes)
    {
        // Note: this code must logically match hashPosition(position, Page page) for all positions
        for (int operatorIndex = 0; operatorIndex < hashCodeOperators.length; operatorIndex++) {
            Block rawBlock = page.getBlock(hashChannels == null ? operatorIndex : hashChannels[operatorIndex]);
            if (operatorIndex == 0) {
                hashFirstBlock(rawBlock, positionOffset, length, hashCodeOperators[operatorIndex], hashes);
            }
            else {
                hashBlockWithCombine(rawBlock, positionOffset, length, hashCodeOperators[operatorIndex], hashes);
            }
        }
    }

    public void hashBlocksBatched(Block[] blocks, long[] hashes, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        // Note: this code must logically match hashPosition(position, Page page) for all positions
        for (int index = 0; index < blocks.length; index++) {
            Block rawBlock = blocks[index];
            if (index == 0) {
                hashFirstBlock(rawBlock, offset, length, hashCodeOperators[index], hashes);
            }
            else {
                hashBlockWithCombine(rawBlock, offset, length, hashCodeOperators[index], hashes);
            }
        }
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        long result = INITIAL_HASH_VALUE;
        for (int i = 0; i < hashCodeOperators.length; i++) {
            Block block = page.getBlock(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, hashCodeOperators[i].hash(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position)));
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

    private static void hashFirstBlock(Block rawBlock, int positionOffset, int length, NullSafeHash hashOperator, long[] hashes)
    {
        switch (rawBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                long hash = hashOperator.hash(rleBlock.getUnderlyingValueBlock(), 0);
                Arrays.fill(hashes, 0, length, hash);
            }
            case DictionaryBlock dictionaryBlock -> {
                if (isDictionaryProcessingFaster(dictionaryBlock, length)) {
                    ValueBlock dictionary = dictionaryBlock.getDictionary();
                    long[] dictionaryHashes = new long[dictionary.getPositionCount()];
                    hashOperator.hashBatched(dictionary, dictionaryHashes, 0, dictionary.getPositionCount());
                    for (int i = 0; i < length; i++) {
                        hashes[i] = dictionaryHashes[dictionaryBlock.getId(i + positionOffset)];
                    }
                }
                else {
                    hashOperator.hashBatchedDictionary(dictionaryBlock, hashes, positionOffset, length);
                }
            }
            case ValueBlock valueBlock -> hashOperator.hashBatched(valueBlock, hashes, positionOffset, length);
        }
    }

    private static void hashBlockWithCombine(Block rawBlock, int positionOffset, int length, NullSafeHash hashOperator, long[] hashes)
    {
        switch (rawBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                long hash = hashOperator.hash(rleBlock.getUnderlyingValueBlock(), 0);
                CombineHashFunction.combineAllHashesWithConstant(hashes, 0, length, hash);
            }
            case DictionaryBlock dictionaryBlock -> {
                if (isDictionaryProcessingFaster(dictionaryBlock, length)) {
                    ValueBlock dictionary = dictionaryBlock.getDictionary();
                    long[] dictionaryHashes = new long[dictionary.getPositionCount()];
                    hashOperator.hashBatched(dictionary, dictionaryHashes, 0, dictionary.getPositionCount());
                    for (int i = 0; i < length; i++) {
                        long hash = dictionaryHashes[dictionaryBlock.getId(i + positionOffset)];
                        hashes[i] = CombineHashFunction.getHash(hashes[i], hash);
                    }
                }
                else {
                    hashOperator.hashBatchedDictionaryWithCombine(dictionaryBlock, hashes, positionOffset, length);
                }
            }
            case ValueBlock valueBlock -> hashOperator.hashBatchedWithCombine(valueBlock, hashes, positionOffset, length);
        }
    }

    private static boolean isDictionaryProcessingFaster(DictionaryBlock dictionaryBlock, int length)
    {
        // if the input positions length is greater than the number of elements in the dictionary by
        // at least 20%, it will be faster to compute hash for the dictionary values only once and
        // re-use it instead of recalculating it.
        return length > dictionaryBlock.getDictionary().getPositionCount() * 1.2;
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
