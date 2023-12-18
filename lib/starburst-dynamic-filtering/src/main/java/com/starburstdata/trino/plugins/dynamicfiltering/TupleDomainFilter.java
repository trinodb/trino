/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public interface TupleDomainFilter
{
    boolean isNullAllowed();

    Type getType();

    boolean testContains(Block block, int position);

    abstract class AbstractTupleDomainFilter
            implements TupleDomainFilter
    {
        protected final boolean nullAllowed;
        protected final Type type;

        public AbstractTupleDomainFilter(boolean nullAllowed, Type type)
        {
            this.nullAllowed = nullAllowed;
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public boolean isNullAllowed()
        {
            return nullAllowed;
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AbstractTupleDomainFilter)) {
                return false;
            }
            AbstractTupleDomainFilter that = (AbstractTupleDomainFilter) o;
            return nullAllowed == that.nullAllowed && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nullAllowed, type);
        }
    }

    final class LongCustomHashSetFilter
            extends AbstractTupleDomainFilter
    {
        private final LongOpenCustomHashSet hashSet;

        /**
         * Based on io.trino.util.FastutilSetHelper.
         */
        public LongCustomHashSetFilter(boolean nullAllowed, Type type, MethodHandle hashCodeHandle, MethodHandle equalsHandle, List<Long> values)
        {
            super(nullAllowed, type);
            requireNonNull(values, "values is null");
            checkArgument(!values.isEmpty(), "values must not be empty");
            this.hashSet = new LongOpenCustomHashSet(values, 0.25f, new LongStrategy(hashCodeHandle, equalsHandle));
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return hashSet.contains(type.getLong(block, position));
        }

        // Allows easier benchmarking without going through Block APIs
        public boolean testContains(long value)
        {
            return hashSet.contains(value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            LongCustomHashSetFilter that = (LongCustomHashSetFilter) o;
            return Objects.equals(hashSet, that.hashSet);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), hashSet);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nullAllowed", nullAllowed)
                    .add("type", type)
                    .add("hashSet", hashSet)
                    .toString();
        }

        private static final class LongStrategy
                implements LongHash.Strategy
        {
            private final MethodHandle hashCodeHandle;
            private final MethodHandle equalsHandle;

            public LongStrategy(MethodHandle hashCodeHandle, MethodHandle equalsHandle)
            {
                this.hashCodeHandle = requireNonNull(hashCodeHandle, "hashCodeHandle is null");
                this.equalsHandle = requireNonNull(equalsHandle, "equalsHandle is null");
            }

            @Override
            public int hashCode(long value)
            {
                try {
                    return Long.hashCode((long) hashCodeHandle.invokeExact(value));
                }
                catch (Throwable t) {
                    throwIfInstanceOf(t, Error.class);
                    throwIfInstanceOf(t, TrinoException.class);
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
                }
            }

            @Override
            public boolean equals(long a, long b)
            {
                try {
                    Boolean result = (Boolean) equalsHandle.invokeExact(a, b);
                    // FastutilHashSet is not intended be used for indeterminate values lookup
                    verifyNotNull(result, "result is null");
                    return TRUE.equals(result);
                }
                catch (Throwable t) {
                    throwIfInstanceOf(t, Error.class);
                    throwIfInstanceOf(t, TrinoException.class);
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
    }

    final class LongHashSetFilter
            extends AbstractTupleDomainFilter
    {
        private final LongOpenHashSet hashSet;
        private final long min;
        private final long max;

        public LongHashSetFilter(boolean nullAllowed, Type type, List<Long> values, long min, long max)
        {
            super(nullAllowed, type);
            requireNonNull(values, "values is null");
            checkArgument(min <= max, "min must be less than or equal to max");
            checkArgument(!values.isEmpty(), "values must not be empty");
            this.hashSet = new LongOpenHashSet(values, 0.25f);
            this.min = min;
            this.max = max;
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return testContains(type.getLong(block, position));
        }

        // Allows easier benchmarking without going through Block APIs
        public boolean testContains(long value)
        {
            if (value < min || value > max) {
                return false;
            }
            return hashSet.contains(value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            LongHashSetFilter that = (LongHashSetFilter) o;
            return min == that.min &&
                    max == that.max &&
                    Objects.equals(hashSet, that.hashSet);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), hashSet, min, max);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nullAllowed", nullAllowed)
                    .add("type", type)
                    .add("hashSet", hashSet)
                    .add("min", min)
                    .add("max", max)
                    .toString();
        }
    }

    final class LongBitSetFilter
            extends AbstractTupleDomainFilter
    {
        private final BitSet bitmask;
        private final long min;
        private final long max;
        private final int size;

        public LongBitSetFilter(boolean nullAllowed, Type type, List<Long> values, long min, long max)
        {
            super(nullAllowed, type);
            requireNonNull(values, "values is null");
            checkArgument(min <= max, "min must be less than or equal to max");
            checkArgument(!values.isEmpty(), "values must not be empty");
            this.size = values.size();
            this.min = min;
            this.max = max;
            long range = max - min + 1;
            checkArgument((int) range == range, format("Values range %d is outside integer range", range));
            bitmask = new BitSet((int) range);

            for (long value : values) {
                bitmask.set((int) (value - min));
            }
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return testContains(type.getLong(block, position));
        }

        // Allows easier benchmarking without going through Block APIs
        public boolean testContains(long value)
        {
            if (value < min || value > max) {
                return false;
            }

            return bitmask.get((int) (value - min));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            LongBitSetFilter that = (LongBitSetFilter) o;
            return min == that.min &&
                    max == that.max &&
                    size == that.size &&
                    Objects.equals(bitmask, that.bitmask);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), bitmask, min, max, size);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("min", min)
                    .add("max", max)
                    .add("bitmask", bitmask)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    final class LongRangeFilter
            extends AbstractTupleDomainFilter
    {
        private final long lower;
        private final long upper;

        /**
         * A filter for a range of long values between lower and upper with both ends of the range included.
         *
         * @param nullAllowed true if NULLs should be included
         * @param type Type of the values being filtered
         * @param lower minimum allowed value
         * @param upper maximum allowed value
         */
        public LongRangeFilter(boolean nullAllowed, Type type, long lower, long upper)
        {
            super(nullAllowed, type);
            checkArgument(lower <= upper, "lower must be less than or equal to upper");
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            long value = type.getLong(block, position);
            return value >= lower && value <= upper;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            LongRangeFilter that = (LongRangeFilter) o;
            return lower == that.lower && upper == that.upper;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), lower, upper);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("upper", upper)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    final class SliceBloomFilter
            extends AbstractTupleDomainFilter
    {
        private final long[] bloom;
        private final int bloomSizeMask;

        /**
         * A Bloom filter for a set of Slice values.
         * This is approx 2X faster than Standard Bloom filter because
         * it uses single hash function and uses that to set 3 bits within a 64 bit word.
         * It has low memory footprint, up to (4 * values.size()) bytes
         *
         * @param nullAllowed true if NULLs should be included
         * @param type Type of the values being filtered
         * @param values List of Slice values used for filtering
         */
        public SliceBloomFilter(boolean nullAllowed, Type type, List<Slice> values)
        {
            super(nullAllowed, type);

            requireNonNull(values, "values is null");
            checkArgument(values.size() > 0, "values must not be empty");

            int bloomSize = getBloomFilterSize(values.size());
            bloom = new long[bloomSize];
            bloomSizeMask = bloomSize - 1;
            for (Slice value : values) {
                long hashCode = XxHash64.hash(value);
                // Set 3 bits in a 64 bit word
                bloom[bloomIndex(hashCode)] |= bloomMask(hashCode);
            }
        }

        public static int getBloomFilterSize(int valuesCount)
        {
            // Linear hash table size is the highest power of two less than or equal to number of values * 4. This means that the
            // table is under half full, e.g. 127 elements gets 256 slots.
            int hashTableSize = Integer.highestOneBit(valuesCount * 4);
            // We will allocate 8 bits in the bloom filter for every slot in a comparable hash table.
            // The bloomSize is a count of longs, hence / 8.
            return Math.max(1, hashTableSize / 8);
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return testContains(type.getSlice(block, position));
        }

        @VisibleForTesting
        public boolean testContains(Slice value)
        {
            long hashCode = XxHash64.hash(value);
            long mask = bloomMask(hashCode);
            return mask == (bloom[bloomIndex(hashCode)] & mask);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            SliceBloomFilter that = (SliceBloomFilter) o;
            return bloomSizeMask == that.bloomSizeMask
                    && Arrays.equals(bloom, that.bloom);
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash(super.hashCode(), bloomSizeMask);
            result = 31 * result + Arrays.hashCode(bloom);
            return result;
        }

        private int bloomIndex(long hashCode)
        {
            // Lower 20 bits are not used by bloomMask
            // These are enough for the maximum size array that will be used here
            return (int) (hashCode & bloomSizeMask);
        }

        private static long bloomMask(long hashCode)
        {
            // returned mask sets 3 bits based on portions of given hash
            // Extract 39 to 44th bits
            return (1L << ((hashCode >> 20) & 63))
                    // Extract 33rd to 38th bits
                    | (1L << ((hashCode >> 26) & 63))
                    // Extract 27th to 32nd bits
                    | (1L << ((hashCode >> 32) & 63));
        }
    }

    final class AlwaysFalse
            extends AbstractTupleDomainFilter
    {
        public AlwaysFalse(Type type)
        {
            super(false, type);
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }

    final class IsNullFilter
            extends AbstractTupleDomainFilter
    {
        public IsNullFilter(Type type)
        {
            super(true, type);
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }

    final class IsNotNullFilter
            extends AbstractTupleDomainFilter
    {
        public IsNotNullFilter(Type type)
        {
            super(false, type);
        }

        @Override
        public boolean testContains(Block block, int position)
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }

        @Override
        public int hashCode()
        {
            return super.hashCode();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }
}
