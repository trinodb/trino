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

import io.trino.array.LongBigArray;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import static it.unimi.dsi.fastutil.HashCommon.mix;
import static java.util.Objects.requireNonNull;

/**
 * Optimized hash table for streaming Top N peer group lookup operations.
 */
// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/) Long2LongOpenCustomHashMap.
// Copyright (C) 2002-2019 Sebastiano Vigna
public class TopNPeerGroupLookup
{
    private static final int INSTANCE_SIZE = instanceSize(TopNPeerGroupLookup.class);

    /**
     * The buffer containing key and value data.
     */
    private Buffer buffer;

    /**
     * The mask for wrapping a position counter.
     */
    private long mask;

    /**
     * The hash strategy.
     */
    private final RowIdHashStrategy strategy;

    /**
     * The current allocated table size.
     */
    private long tableSize;

    /**
     * Threshold after which we rehash.
     */
    private long maxFill;

    /**
     * The acceptable load factor.
     */
    private final float fillFactor;

    /**
     * Number of entries in the set.
     */
    private long entryCount;

    /**
     * The value denoting unmapped group IDs. Since group IDs need to co-exist at all times with row IDs,
     * we only need to use one of the two IDs to indicate that a slot is unused. Group IDs have been arbitrarily selected
     * for that purpose.
     */
    private final long unmappedGroupId;

    /**
     * The default return value for {@code get()}, {@code put()} and {@code remove()}.
     */
    private final long defaultReturnValue;

    /**
     * Standard hash table parameters are expected. {@code unmappedGroupId} specifies the internal marker value for unmapped group IDs.
     */
    public TopNPeerGroupLookup(long expected, float fillFactor, RowIdHashStrategy strategy, long unmappedGroupId, long defaultReturnValue)
    {
        checkArgument(expected >= 0, "The expected number of elements must be nonnegative");
        checkArgument(fillFactor > 0 && fillFactor <= 1, "Load factor must be greater than 0 and smaller than or equal to 1");
        this.fillFactor = fillFactor;
        this.strategy = requireNonNull(strategy, "strategy is null");
        this.unmappedGroupId = unmappedGroupId;
        this.defaultReturnValue = defaultReturnValue;

        tableSize = bigArraySize(expected, fillFactor);
        mask = tableSize - 1;
        maxFill = maxFill(tableSize, fillFactor);
        buffer = new Buffer(tableSize, unmappedGroupId);
    }

    public TopNPeerGroupLookup(long expected, RowIdHashStrategy strategy, long unmappedGroupId, long defaultReturnValue)
    {
        this(expected, 0.75f, strategy, unmappedGroupId, defaultReturnValue);
    }

    /**
     * Returns the size of this hash map in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + buffer.sizeOf();
    }

    public long size()
    {
        return entryCount;
    }

    public boolean isEmpty()
    {
        return entryCount == 0;
    }

    public long get(long groupId, long rowId)
    {
        checkArgument(groupId != unmappedGroupId, "Group ID cannot be the unmapped group ID");

        long hash = hash(groupId, rowId);
        long index = hash & mask;
        if (buffer.isEmptySlot(index)) {
            return defaultReturnValue;
        }
        if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
            return buffer.getValue(index);
        }
        // There's always an unused entry.
        while (true) {
            index = (index + 1) & mask;
            if (buffer.isEmptySlot(index)) {
                return defaultReturnValue;
            }
            if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
                return buffer.getValue(index);
            }
        }
    }

    public long get(long groupId, RowReference rowReference)
    {
        checkArgument(groupId != unmappedGroupId, "Group ID cannot be the unmapped group ID");

        long hash = hash(groupId, rowReference);
        long index = hash & mask;
        if (buffer.isEmptySlot(index)) {
            return defaultReturnValue;
        }
        if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowReference, index)) {
            return buffer.getValue(index);
        }
        // There's always an unused entry.
        while (true) {
            index = (index + 1) & mask;
            if (buffer.isEmptySlot(index)) {
                return defaultReturnValue;
            }
            if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowReference, index)) {
                return buffer.getValue(index);
            }
        }
    }

    public long put(long groupId, long rowId, long value)
    {
        checkArgument(groupId != unmappedGroupId, "Group ID cannot be the unmapped group ID");

        long hash = hash(groupId, rowId);

        long index = find(groupId, rowId, hash);
        if (index < 0) {
            insert(twosComplement(index), groupId, rowId, hash, value);
            return defaultReturnValue;
        }
        long oldValue = buffer.getValue(index);
        buffer.setValue(index, value);
        return oldValue;
    }

    private long hash(long groupId, long rowId)
    {
        return mix(groupId * 31 + strategy.hashCode(rowId));
    }

    private long hash(long groupId, RowReference rowReference)
    {
        return mix(groupId * 31 + rowReference.hash(strategy));
    }

    private boolean equals(long groupId, long rowId, long index)
    {
        return groupId == buffer.getGroupId(index) && strategy.equals(rowId, buffer.getRowId(index));
    }

    private boolean equals(long groupId, RowReference rowReference, long index)
    {
        return groupId == buffer.getGroupId(index) && rowReference.equals(strategy, buffer.getRowId(index));
    }

    private void insert(long index, long groupId, long rowId, long precomputedHash, long value)
    {
        buffer.set(index, groupId, rowId, precomputedHash, value);
        entryCount++;
        if (entryCount > maxFill) {
            rehash(bigArraySize(entryCount + 1, fillFactor));
        }
    }

    /**
     * Locate the index for the specified {@code groupId} and {@code rowId} key pair. If the index is unpopulated,
     * then return the index as the two's complement value (which will be negative).
     */
    private long find(long groupId, long rowId, long precomputedHash)
    {
        long index = precomputedHash & mask;
        if (buffer.isEmptySlot(index)) {
            return twosComplement(index);
        }
        if (precomputedHash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
            return index;
        }
        // There's always an unused entry.
        while (true) {
            index = (index + 1) & mask;
            if (buffer.isEmptySlot(index)) {
                return twosComplement(index);
            }
            if (precomputedHash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
                return index;
            }
        }
    }

    public long remove(long groupId, long rowId)
    {
        checkArgument(groupId != unmappedGroupId, "Group ID cannot be the unmapped group ID");

        long hash = hash(groupId, rowId);
        long index = hash & mask;
        if (buffer.isEmptySlot(index)) {
            return defaultReturnValue;
        }
        if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
            return removeEntry(index);
        }
        while (true) {
            index = (index + 1) & mask;
            if (buffer.isEmptySlot(index)) {
                return defaultReturnValue;
            }
            if (hash == buffer.getPrecomputedHash(index) && equals(groupId, rowId, index)) {
                return removeEntry(index);
            }
        }
    }

    private long removeEntry(long index)
    {
        long oldValue = buffer.getValue(index);
        entryCount--;
        shiftKeys(index);
        return oldValue;
    }

    /**
     * Shifts left entries with the specified hash code, starting at the specified
     * index, and empties the resulting free entry.
     *
     * @param index a starting position.
     */
    private void shiftKeys(long index)
    {
        // Shift entries with the same hash.
        while (true) {
            long currentHash;

            long initialIndex = index;
            index = ((index) + 1) & mask;
            while (true) {
                if (buffer.isEmptySlot(index)) {
                    buffer.clear(initialIndex);
                    return;
                }
                currentHash = buffer.getPrecomputedHash(index);
                long slot = currentHash & mask;
                // Yes, this is dense logic. See fastutil Long2LongOpenCustomHashMap#shiftKeys implementation.
                if (initialIndex <= index ? initialIndex >= slot || slot > index : initialIndex >= slot && slot > index) {
                    break;
                }
                index = (index + 1) & mask;
            }
            buffer.set(initialIndex, buffer.getGroupId(index), buffer.getRowId(index), currentHash, buffer.getValue(index));
        }
    }

    private void rehash(long newTableSize)
    {
        long newMask = newTableSize - 1; // Note that this is used by the hashing macro
        Buffer newBuffer = new Buffer(newTableSize, unmappedGroupId);
        long index = tableSize;
        for (long i = entryCount; i > 0; i--) {
            index--;
            while (buffer.isEmptySlot(index)) {
                index--;
            }
            long hash = buffer.getPrecomputedHash(index);
            long newIndex = hash & newMask;
            if (!newBuffer.isEmptySlot(newIndex)) {
                newIndex = (newIndex + 1) & newMask;
                while (!newBuffer.isEmptySlot(newIndex)) {
                    newIndex = (newIndex + 1) & newMask;
                }
            }
            newBuffer.set(newIndex, buffer.getGroupId(index), buffer.getRowId(index), hash, buffer.getValue(index));
        }
        tableSize = newTableSize;
        mask = newMask;
        maxFill = maxFill(tableSize, fillFactor);
        buffer = newBuffer;
    }

    private static long twosComplement(long value)
    {
        return -(value + 1);
    }

    private static class Buffer
    {
        private static final long INSTANCE_SIZE = instanceSize(Buffer.class);

        private static final int POSITIONS_PER_ENTRY = 4;
        private static final int ROW_ID_OFFSET = 1;
        private static final int PRECOMPUTED_HASH_OFFSET = 2;
        private static final int VALUE_OFFSET = 3;

        /*
         *  Memory layout:
         *  [LONG] groupId1, [LONG] rowId1, [LONG] precomputedHash1, [LONG] value1
         *  [LONG] groupId2, [LONG] rowId2, [LONG] precomputedHash2, [LONG] value2
         *  ...
         */
        private final LongBigArray buffer;
        private final long unmappedGroupId;

        public Buffer(long positions, long unmappedGroupId)
        {
            buffer = new LongBigArray(unmappedGroupId);
            buffer.ensureCapacity(positions * POSITIONS_PER_ENTRY);
            this.unmappedGroupId = unmappedGroupId;
        }

        public void set(long index, long groupId, long rowId, long precomputedHash, long value)
        {
            buffer.set(index * POSITIONS_PER_ENTRY, groupId);
            buffer.set(index * POSITIONS_PER_ENTRY + ROW_ID_OFFSET, rowId);
            buffer.set(index * POSITIONS_PER_ENTRY + PRECOMPUTED_HASH_OFFSET, precomputedHash);
            buffer.set(index * POSITIONS_PER_ENTRY + VALUE_OFFSET, value);
        }

        public void clear(long index)
        {
            // Since all fields of an index are set/unset together as a unit, we only need to choose one field to serve
            // as a marker for empty slots. Group IDs have been arbitrarily selected for that purpose.
            buffer.set(index * POSITIONS_PER_ENTRY, unmappedGroupId);
        }

        public boolean isEmptySlot(long index)
        {
            return getGroupId(index) == unmappedGroupId;
        }

        public long getGroupId(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY);
        }

        public long getRowId(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + ROW_ID_OFFSET);
        }

        public long getPrecomputedHash(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + PRECOMPUTED_HASH_OFFSET);
        }

        public long getValue(long index)
        {
            return buffer.get(index * POSITIONS_PER_ENTRY + VALUE_OFFSET);
        }

        public void setValue(long index, long value)
        {
            buffer.set(index * POSITIONS_PER_ENTRY + VALUE_OFFSET, value);
        }

        public long sizeOf()
        {
            return INSTANCE_SIZE + buffer.sizeOf();
        }
    }
}
