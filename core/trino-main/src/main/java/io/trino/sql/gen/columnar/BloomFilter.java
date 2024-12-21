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
package io.trino.sql.gen.columnar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.operator.project.InputChannels;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class BloomFilter
{
    private BloomFilter() {}

    public static boolean canUseBloomFilter(Domain domain)
    {
        Type type = domain.getType();
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            verify(type.getJavaType() == Slice.class, "Type is not backed by Slice");
            return !domain.isNone() && !domain.isAll() && domain.isNullableDiscreteSet();
        }
        return false;
    }

    public static Supplier<FilterEvaluator> createBloomFilterEvaluator(Domain domain, int inputChannel)
    {
        return () -> new ColumnarFilterEvaluator(
                new DictionaryAwareColumnarFilter(
                        new ColumnarBloomFilter(domain.getNullableDiscreteSet(), inputChannel, domain.getType())));
    }

    private static final class ColumnarBloomFilter
            implements ColumnarFilter
    {
        private final SliceBloomFilter filter;
        private final boolean isNullAllowed;
        private final InputChannels inputChannels;

        public ColumnarBloomFilter(Domain.DiscreteSet discreteSet, int inputChannel, Type type)
        {
            this.isNullAllowed = discreteSet.containsNull();
            this.filter = new SliceBloomFilter((List<Slice>) (List<?>) discreteSet.getNonNullValues(), type);
            this.inputChannels = new InputChannels(ImmutableList.of(inputChannel), ImmutableList.of(inputChannel));
        }

        @Override
        public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, Page page)
        {
            ValueBlock block = (ValueBlock) page.getBlock(0);
            int selectedPositionsCount = 0;
            for (int position = offset; position < offset + size; position++) {
                boolean result = block.isNull(position) ? isNullAllowed : filter.test(block, position);
                outputPositions[selectedPositionsCount] = position;
                selectedPositionsCount += result ? 1 : 0;
            }
            return selectedPositionsCount;
        }

        @Override
        public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, Page page)
        {
            ValueBlock block = (ValueBlock) page.getBlock(0);
            int selectedPositionsCount = 0;
            for (int index = offset; index < offset + size; index++) {
                int position = activePositions[index];
                boolean result = block.isNull(position) ? isNullAllowed : filter.test(block, position);
                outputPositions[selectedPositionsCount] = position;
                selectedPositionsCount += result ? 1 : 0;
            }
            return selectedPositionsCount;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return inputChannels;
        }
    }

    public static final class SliceBloomFilter
    {
        private final long[] bloom;
        private final int bloomSizeMask;
        private final Type type;

        /**
         * A Bloom filter for a set of Slice values.
         * This is approx 2X faster than the Bloom filter implementations in ORC and parquet because
         * it uses single hash function and uses that to set 3 bits within a 64 bit word.
         * The memory footprint is up to (4 * values.size()) bytes, which is much smaller than maintaining a hash set of strings.
         *
         * @param values List of values used for filtering
         */
        public SliceBloomFilter(List<Slice> values, Type type)
        {
            this.type = requireNonNull(type, "type is null");
            int bloomSize = getBloomFilterSize(values.size());
            bloom = new long[bloomSize];
            bloomSizeMask = bloomSize - 1;
            for (Slice value : values) {
                long hashCode = XxHash64.hash(value);
                // Set 3 bits in a 64 bit word
                bloom[bloomIndex(hashCode)] |= bloomMask(hashCode);
            }
        }

        private static int getBloomFilterSize(int valuesCount)
        {
            // Linear hash table size is the highest power of two less than or equal to number of values * 4. This means that the
            // table is under half full, e.g. 127 elements gets 256 slots.
            int hashTableSize = Integer.highestOneBit(valuesCount * 4);
            // We will allocate 8 bits in the bloom filter for every slot in a comparable hash table.
            // The bloomSize is a count of longs, hence / 8.
            return Math.max(1, hashTableSize / 8);
        }

        public boolean test(Block block, int position)
        {
            return contains(type.getSlice(block, position));
        }

        public boolean contains(Slice data)
        {
            long hashCode = XxHash64.hash(data);
            long mask = bloomMask(hashCode);
            return mask == (bloom[bloomIndex(hashCode)] & mask);
        }

        @VisibleForTesting
        public boolean contains(Slice data, int offset, int length)
        {
            long hashCode = XxHash64.hash(data, offset, length);
            long mask = bloomMask(hashCode);
            return mask == (bloom[bloomIndex(hashCode)] & mask);
        }

        private int bloomIndex(long hashCode)
        {
            // Lower 21 bits are not used by bloomMask
            // These are enough for the maximum size array that will be used here
            return (int) (hashCode & bloomSizeMask);
        }

        private static long bloomMask(long hashCode)
        {
            // returned mask sets 3 bits based on portions of given hash
            // Extract 38th to 43rd bits
            return (1L << ((hashCode >> 21) & 63))
                    // Extract 32nd to 37th bits
                    | (1L << ((hashCode >> 27) & 63))
                    // Extract 26th to 31st bits
                    | (1L << ((hashCode >> 33) & 63));
        }
    }
}
