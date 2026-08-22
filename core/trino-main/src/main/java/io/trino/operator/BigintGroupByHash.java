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

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BigintType;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A group-by hash for a single BIGINT channel that stores quotiented hashes instead of values
 * (see Knuth 6.4 exercise 13, and "Succinct and Fast Tiny Pointer Hash Tables", VLDB 2026).
 * <p>
 * The bucket index is the low {@code log2(capacity)} bits of an invertible hash of the value,
 * so a slot only stores the bits the position does not imply: the remaining hash bits and the
 * linear-probing displacement, packed into one long, with the group id in a parallel int
 * array. Together they recover the home bucket and full hash, which makes probe matches exact
 * and lets output reconstruct values by inverting the hash. This reduces memory by ~20% at
 * large group counts. The split arrays keep every probe load aligned (packed variable-width
 * slots straddle cache lines, which some cores penalize heavily), and the two loads overlap
 * through memory-level parallelism.
 * <p>
 * Hashing starts as the identity function, which is collision-free for dense keys and requires
 * no computation before the probe load. When clustering is measured during rehash, the table
 * switches to the murmur3 finalizer by re-hashing the recovered values. Displacement overflow,
 * or a high average insert displacement sampled per batch, grows the table instead, since dense
 * keys wrapping around an intermediate capacity cluster only transiently; keys that defeat
 * identity bucketing at any capacity still cluster after the growth, which the rehash detects.
 */
public class BigintGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(BigintGroupByHash.class);
    private static final int BATCH_SIZE = 1024;

    private static final float FILL_RATIO = 0.75f;

    // at 0.75 fill, P(displacement > 4095) is ~e^-154 per element, so 12 bits never overflow
    private static final int MAX_DISPLACEMENT_BITS = 12;

    private static final int MAX_CAPACITY = 1 << 30;

    // identity hashing is abandoned when rehash measures average displacement above this;
    // uniform keys average ~0.3 right after doubling, so this only fires on structured keys
    private static final long MAX_AVERAGE_DISPLACEMENT = 2;

    // an average displacement of inserts since the last rehash above this triggers an early
    // grow, so clustered keys stop paying for long probe chains after about one batch instead
    // of a full fill cycle; the post-rehash measurement then decides whether to also switch
    // the hash function, since dense keys wrapping around an intermediate capacity cluster
    // just as hard but disperse on growth. Uniform keys average under ~3 even at peak load
    private static final long MAX_INSERT_AVERAGE_DISPLACEMENT = 8;
    private static final int MIN_INSERTS_TO_MEASURE_CLUSTERING = 256;

    // Tables larger than the cache are probed in two passes over mini-batches: the first pass
    // computes hashes and touches each row's home slot so the misses overlap, and the second
    // pass completes the probes against warm cache lines. The threshold is set above the
    // capacities that still fit in the last-level cache, where the touch pass is wasted work
    private static final int PREFETCH_BATCH = 32;
    private static final int PREFETCH_MIN_CAPACITY = 1 << 22;

    // true until clustering is detected; from then on the murmur3 finalizer is used
    private boolean identityHashing;

    private int hashCapacity;
    private int maxFill;
    private int mask;
    private int log2Capacity;
    private int remainderBits;
    private int maxDisplacement;

    // records[slot] packs (displacement << remainderBits) | remainder; groupIdsPlusOne[slot]
    // holds groupId + 1, where zero means empty so freshly allocated arrays form a valid
    // empty table without a fill pass
    private long[] records;
    private int[] groupIdsPlusOne;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the slot holding its record
    private int[] slotsByGroupId;

    // values in groupId order, materialized by startReleasingOutput, which frees the table
    private long[] releasedValues;

    private int nextGroupId;
    private DictionaryLookBack dictionaryLookBack;

    // cumulative displacement of new-group inserts since the last rehash, sampled once per
    // batch to detect identity-hash clustering without waiting for a fill-triggered rehash
    private long insertDisplacementSum;
    private int groupCountAtLastRehash;

    private final long[] scratchValues = new long[PREFETCH_BATCH];
    private final long[] scratchHashes = new long[PREFETCH_BATCH];
    // consumes the first-pass touch loads so they cannot be eliminated as dead code
    @SuppressWarnings("UnusedVariable")
    private long prefetchSink;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHash(int expectedSize, UpdateMemory updateMemory)
    {
        this(expectedSize, true, updateMemory);
    }

    @VisibleForTesting
    BigintGroupByHash(int expectedSize, boolean identityHashing, UpdateMemory updateMemory)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");
        this.identityHashing = identityHashing;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);
        checkArgument(hashCapacity <= MAX_CAPACITY, "expectedSize is too large");

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        updateRecordLayout();
        records = new long[hashCapacity];
        groupIdsPlusOne = new int[hashCapacity];

        slotsByGroupId = new int[maxFill];

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    private BigintGroupByHash(BigintGroupByHash other)
    {
        identityHashing = other.identityHashing;
        hashCapacity = other.hashCapacity;
        maxFill = other.maxFill;
        mask = other.mask;
        log2Capacity = other.log2Capacity;
        remainderBits = other.remainderBits;
        maxDisplacement = other.maxDisplacement;
        records = copyOfNullable(other.records);
        groupIdsPlusOne = copyOfNullable(other.groupIdsPlusOne);
        nullGroupId = other.nullGroupId;
        slotsByGroupId = copyOfNullable(other.slotsByGroupId);
        releasedValues = copyOfNullable(other.releasedValues);
        nextGroupId = other.nextGroupId;
        dictionaryLookBack = other.dictionaryLookBack == null ? null : other.dictionaryLookBack.copy();
        insertDisplacementSum = other.insertDisplacementSum;
        groupCountAtLastRehash = other.groupCountAtLastRehash;
        updateMemory = other.updateMemory;
        preallocatedMemoryInBytes = other.preallocatedMemoryInBytes;
        currentPageSizeInBytes = other.currentPageSizeInBytes;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(records) +
                sizeOf(groupIdsPlusOne) +
                sizeOf(slotsByGroupId) +
                sizeOf(releasedValues) +
                preallocatedMemoryInBytes;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void startReleasingOutput()
    {
        dictionaryLookBack = null;
        currentPageSizeInBytes = 0;
        if (releasedValues != null) {
            return;
        }

        // the reservation result is ignored: the release frees more than it allocates, so
        // pausing here would mean waiting for memory in order to free memory
        preallocatedMemoryInBytes = sizeOfLongArray(nextGroupId);
        updateMemory.update();

        // materialize values in groupId order so output appends become sequential reads,
        // freeing the reverse index first to limit the transient footprint
        slotsByGroupId = null;
        long[] values = new long[nextGroupId];
        for (int slot = 0; slot < hashCapacity; slot++) {
            int groupIdPlusOne = groupIdsPlusOne[slot];
            if (groupIdPlusOne != 0) {
                values[groupIdPlusOne - 1] = reconstructValue(slot);
            }
        }
        releasedValues = values;
        records = null;
        groupIdsPlusOne = null;

        preallocatedMemoryInBytes = 0;
        // report the reduced memory usage
        updateMemory.update();
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else if (releasedValues != null) {
            BIGINT.writeLong(blockBuilder, releasedValues[groupId]);
        }
        else {
            BIGINT.writeLong(blockBuilder, reconstructValue(slotsByGroupId[groupId]));
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        checkState(releasedValues == null, "output is being released");
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(0);
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            return new AddRunLengthEncodedPageWork(rleBlock);
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            return new AddDictionaryPageWork(dictionaryBlock);
        }

        return new AddPageWork(block);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        checkState(releasedValues == null, "output is being released");
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(0);
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            return new GetRunLengthEncodedGroupIdsWork(rleBlock);
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            return new GetDictionaryGroupIdsWork(dictionaryBlock);
        }

        return new GetGroupIdsWork(block);
    }

    @Override
    public long getRawHash(int groupId)
    {
        if (releasedValues != null) {
            return BigintType.hash(releasedValues[groupId]);
        }
        return BigintType.hash(reconstructValue(slotsByGroupId[groupId]));
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    @VisibleForTesting
    boolean isIdentityHashing()
    {
        return identityHashing;
    }

    @Override
    public GroupByHash copy()
    {
        return new BigintGroupByHash(this);
    }

    private long hashValue(long value)
    {
        if (identityHashing) {
            return value;
        }
        return murmurHash3(value);
    }

    private long invertHash(long hash)
    {
        if (identityHashing) {
            return hash;
        }
        return invMurmurHash3(hash);
    }

    private void updateRecordLayout()
    {
        log2Capacity = Integer.numberOfTrailingZeros(hashCapacity);
        remainderBits = 64 - log2Capacity;
        int displacementBits = min(log2Capacity, MAX_DISPLACEMENT_BITS);
        maxDisplacement = (1 << displacementBits) - 1;
    }

    private long reconstructValue(int slot)
    {
        long record = records[slot];
        int displacement = (int) (record >>> remainderBits);
        int homePosition = (slot - displacement) & mask;
        long hash = ((record & remainderMask()) << log2Capacity) | homePosition;
        return invertHash(hash);
    }

    private long remainderMask()
    {
        return (1L << remainderBits) - 1;
    }

    /**
     * Inverse of {@link it.unimi.dsi.fastutil.HashCommon#murmurHash3(long)}: xorshift by 33 is
     * an involution, and the multipliers are the modular inverses of the murmur3 constants.
     */
    @VisibleForTesting
    static long invMurmurHash3(long x)
    {
        x ^= x >>> 33;
        x *= 0x9CB4B2F8129337DBL;
        x ^= x >>> 33;
        x *= 0x4F74430C22A54005L;
        x ^= x >>> 33;
        return x;
    }

    private int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        return putValueIfAbsent(value);
    }

    private int putValueIfAbsent(long value)
    {
        return putValueIfAbsent(value, hashValue(value));
    }

    private int putValueIfAbsent(long value, long hash)
    {
        while (true) {
            int hashPosition = (int) (hash & mask);
            // each probe step advances the displacement field of the expected record, so record
            // equality implies an equal home bucket and hash, and therefore an equal value
            long expectedRecord = hash >>> log2Capacity;
            long displacementIncrement = 1L << remainderBits;
            int displacement = 0;

            while (true) {
                int groupIdPlusOne = groupIdsPlusOne[hashPosition];
                if (groupIdPlusOne == 0) {
                    insertDisplacementSum += displacement;
                    return addNewGroup(hashPosition, expectedRecord);
                }

                if (records[hashPosition] == expectedRecord) {
                    return groupIdPlusOne - 1;
                }

                // increment position and mask to handle wrap around
                hashPosition = (hashPosition + 1) & mask;
                displacement++;
                expectedRecord += displacementIncrement;
                if (displacement > maxDisplacement) {
                    break;
                }
            }

            // Displacement overflow: grow and retry. Identity-hashing clusters caused by dense
            // keys wrapping around a small capacity disappear after doubling; if they persist,
            // the post-rehash check switches the hash function.
            boolean rehashed;
            if (identityHashing && hashCapacity * 2L > MAX_CAPACITY) {
                rehashed = tryRehash(hashCapacity, false);
            }
            else {
                rehashed = tryRehash();
            }
            if (!rehashed) {
                throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Cannot rehash hash table needed to store displaced entry");
            }
            // the rehash may have switched the hash function
            hash = hashValue(value);
        }
    }

    private void putBatch(Block block, int offset, int count, int[] groupIdsOut, int outOffset)
    {
        long[] values = scratchValues;
        long[] hashes = scratchHashes;
        boolean identityHashingAtStart = identityHashing;
        long sink = 0;
        for (int i = 0; i < count; i++) {
            long value = BIGINT.getLong(block, offset + i);
            long hash = hashValue(value);
            values[i] = value;
            hashes[i] = hash;
            int home = (int) (hash & mask);
            sink += groupIdsPlusOne[home] + records[home];
        }
        prefetchSink += sink;

        for (int i = 0; i < count; i++) {
            int groupId;
            if (identityHashing != identityHashingAtStart) {
                // a mid-batch rehash switched the hash function, invalidating precomputed hashes
                groupId = putValueIfAbsent(values[i]);
            }
            else {
                groupId = putValueIfAbsent(values[i], hashes[i]);
            }
            if (groupIdsOut != null) {
                groupIdsOut[outOffset + i] = groupId;
            }
        }
    }

    private void putRange(Block block, int offset, int count, int[] groupIdsOut)
    {
        if (hashCapacity >= PREFETCH_MIN_CAPACITY) {
            for (int i = 0; i < count; i += PREFETCH_BATCH) {
                putBatch(block, offset + i, min(PREFETCH_BATCH, count - i), groupIdsOut, offset + i);
            }
        }
        else if (groupIdsOut != null) {
            for (int i = offset; i < offset + count; i++) {
                groupIdsOut[i] = putValueIfAbsent(BIGINT.getLong(block, i));
            }
        }
        else {
            for (int i = offset; i < offset + count; i++) {
                putValueIfAbsent(BIGINT.getLong(block, i));
            }
        }
    }

    private int addNewGroup(int hashPosition, long record)
    {
        int groupId = nextGroupId++;

        records[hashPosition] = record;
        groupIdsPlusOne[hashPosition] = groupId + 1;
        slotsByGroupId[groupId] = hashPosition;

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private boolean tryRehash()
    {
        return tryRehash(hashCapacity * 2L, identityHashing);
    }

    private boolean tryRehash(long newCapacityLong, boolean newIdentityHashing)
    {
        if (newCapacityLong > MAX_CAPACITY) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        int newLog2Capacity = Integer.numberOfTrailingZeros(newCapacity);
        int newRemainderBits = 64 - newLog2Capacity;
        int newMaxDisplacement = (1 << min(newLog2Capacity, MAX_DISPLACEMENT_BITS)) - 1;

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for records and group ids as well as slotsByGroupId and the size of the current page
        preallocatedMemoryInBytes = newCapacity * ((long) Long.BYTES + Integer.BYTES) + ((long) calculateMaxFill(newCapacity)) * Integer.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }

        int newMask = newCapacity - 1;
        long[] newRecords = new long[newCapacity];
        int[] newGroupIdsPlusOne = new int[newCapacity];
        long newDisplacementIncrement = 1L << newRemainderBits;
        long totalDisplacement = 0;
        int entryCount = 0;

        for (int i = 0; i < hashCapacity; i++) {
            int groupIdPlusOne = groupIdsPlusOne[i];
            if (groupIdPlusOne == 0) {
                continue;
            }

            long record = records[i];
            int displacement = (int) (record >>> remainderBits);
            int homePosition = (i - displacement) & mask;
            long hash = ((record & remainderMask()) << log2Capacity) | homePosition;
            if (newIdentityHashing != identityHashing) {
                // switching is only ever from identity hashing, where the hash is the value
                hash = murmurHash3(hash);
            }

            // find an empty slot for the record
            int hashPosition = (int) (hash & newMask);
            long newRecord = hash >>> newLog2Capacity;
            int newDisplacement = 0;
            while (newGroupIdsPlusOne[hashPosition] != 0) {
                hashPosition = (hashPosition + 1) & newMask;
                newRecord += newDisplacementIncrement;
                newDisplacement++;
            }
            if (newDisplacement > newMaxDisplacement) {
                // identity clusters can survive the doubling when several dense key ranges
                // alias onto the same buckets (offsets congruent modulo the new capacity), so
                // the reinsert itself can overflow the displacement field; redo the rehash
                // with the murmur3 finalizer, which disperses any key structure. The old table
                // is untouched until the rehash commits, so retrying is safe
                verify(newIdentityHashing, "displacement overflow after rehash");
                return tryRehash(newCapacityLong, false);
            }
            totalDisplacement += newDisplacement;
            entryCount++;

            newRecords[hashPosition] = newRecord;
            newGroupIdsPlusOne[hashPosition] = groupIdPlusOne;
            slotsByGroupId[groupIdPlusOne - 1] = hashPosition;
        }

        identityHashing = newIdentityHashing;
        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        updateRecordLayout();
        records = newRecords;
        groupIdsPlusOne = newGroupIdsPlusOne;

        this.slotsByGroupId = Arrays.copyOf(slotsByGroupId, maxFill);

        insertDisplacementSum = 0;
        groupCountAtLastRehash = nextGroupId;

        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();

        // switch off identity hashing if the reinsertion pass measured clustering; the nested
        // rehash cannot recurse since it commits identityHashing = false before this check
        if (identityHashing && totalDisplacement > MAX_AVERAGE_DISPLACEMENT * entryCount) {
            tryRehash(hashCapacity, false);
        }
        return true;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private boolean insertsAreClustered()
    {
        int inserted = nextGroupId - groupCountAtLastRehash;
        return inserted >= MIN_INSERTS_TO_MEASURE_CLUSTERING && insertDisplacementSum > MAX_INSERT_AVERAGE_DISPLACEMENT * inserted;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private static int[] copyOfNullable(int[] array)
    {
        return array == null ? null : Arrays.copyOf(array, array.length);
    }

    private static long[] copyOfNullable(long[] array)
    {
        return array == null ? null : Arrays.copyOf(array, array.length);
    }

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    private int registerGroupId(Block dictionary, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, dictionary);
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    @VisibleForTesting
    class AddPageWork
            implements Work<Void>
    {
        private final Block block;

        private int lastPosition;

        public AddPageWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                if (block.mayHaveNull()) {
                    for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                        putIfAbsent(i, block);
                    }
                }
                else {
                    putRange(block, lastPosition, batchSize, null);
                }

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class AddDictionaryPageWork
            implements Work<Void>
    {
        private final Block dictionary;
        private final DictionaryBlock block;

        private int lastPosition;

        public AddDictionaryPageWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                registerGroupId(dictionary, positionInDictionary);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final RunLengthEncodedBlock block;

        private boolean finished;

        public AddRunLengthEncodedPageWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (block.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(0, block.getValue());
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class GetGroupIdsWork
            implements Work<int[]>
    {
        private final int[] groupIds;
        private final Block block;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            this.groupIds = new int[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                if (block.mayHaveNull()) {
                    for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                        groupIds[i] = putIfAbsent(i, block);
                    }
                }
                else {
                    putRange(block, lastPosition, batchSize, groupIds);
                }

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetDictionaryGroupIdsWork
            implements Work<int[]>
    {
        private final int[] groupIds;
        private final Block dictionary;
        private final DictionaryBlock block;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);

            this.groupIds = new int[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                int groupId = registerGroupId(dictionary, positionInDictionary);
                groupIds[lastPosition] = groupId;
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetRunLengthEncodedGroupIdsWork
            implements Work<int[]>
    {
        private final RunLengthEncodedBlock block;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (block.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(0, block.getValue());
            processFinished = true;
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            int[] result = new int[block.getPositionCount()];
            Arrays.fill(result, groupId);
            return result;
        }
    }

    private boolean ensureHashTableSize(int batchSize)
    {
        if (identityHashing && insertsAreClustered()) {
            // grow rather than switch: the rehash measures displacement in the new table and
            // switches the hash function only if the clustering survives the doubling
            boolean rehashed = hashCapacity * 2L > MAX_CAPACITY ? tryRehash(hashCapacity, false) : tryRehash();
            if (!rehashed) {
                return false;
            }
        }

        int positionCountUntilRehash = maxFill - nextGroupId;
        while (positionCountUntilRehash < batchSize) {
            if (!tryRehash()) {
                return false;
            }
            positionCountUntilRehash = maxFill - nextGroupId;
        }
        return true;
    }

    private static final class DictionaryLookBack
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
        }

        private DictionaryLookBack(DictionaryLookBack other)
        {
            this.dictionary = other.dictionary;
            this.processed = Arrays.copyOf(other.processed, other.processed.length);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != -1;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }

        public DictionaryLookBack copy()
        {
            return new DictionaryLookBack(this);
        }
    }
}
