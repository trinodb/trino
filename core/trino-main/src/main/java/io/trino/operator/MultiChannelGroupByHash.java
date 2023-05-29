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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public class MultiChannelGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(MultiChannelGroupByHash.class);
    private static final float FILL_RATIO = 0.75f;
    private static final int BATCH_SIZE = 1024;
    // Max (page value count / cumulative dictionary size) to trigger the low cardinality case
    private static final double SMALL_DICTIONARIES_MAX_CARDINALITY_RATIO = .25;
    private static final int VALUES_PAGE_BITS = 14; // 16k positions
    private static final int VALUES_PAGE_MAX_ROW_COUNT = 1 << VALUES_PAGE_BITS;
    private static final int VALUES_PAGE_MASK = VALUES_PAGE_MAX_ROW_COUNT - 1;

    private final List<Type> types;
    private final List<Type> hashTypes;
    private final int[] channels;

    private final PagesHashStrategy hashStrategy;
    private final List<ObjectArrayList<Block>> channelBuilders;
    private final Optional<Integer> inputHashChannel;
    private final HashGenerator hashGenerator;
    private final OptionalInt precomputedHashChannel;
    private final boolean processDictionary;
    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int hashCapacity;
    private int maxFill;
    private int mask;
    // Group ids are assigned incrementally. Therefore, since values page size is constant and power of two,
    // the group id is also an address (slice index and position within slice) to group row in channelBuilders.
    private int[] groupIdsByHash;
    private byte[] rawHashByHashPosition;

    private int nextGroupId;
    private DictionaryLookBack dictionaryLookBack;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public MultiChannelGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        this.hashTypes = ImmutableList.copyOf(requireNonNull(hashTypes, "hashTypes is null"));

        requireNonNull(joinCompiler, "joinCompiler is null");
        requireNonNull(hashChannels, "hashChannels is null");
        checkArgument(hashTypes.size() == hashChannels.length, "hashTypes and hashChannels have different sizes");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.inputHashChannel = requireNonNull(inputHashChannel, "inputHashChannel is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : this.hashTypes;
        this.channels = hashChannels.clone();

        this.hashGenerator = inputHashChannel.isPresent() ? new PrecomputedHashGenerator(inputHashChannel.get()) : new InterpretedHashGenerator(this.hashTypes, hashChannels, blockTypeOperators);
        this.processDictionary = processDictionary;

        // For each hashed channel, create an appendable list to hold the blocks (builders).  As we
        // add new values we append them to the existing block builder until it fills up and then
        // we add a new block builder to each list.
        ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channelBuilders = ImmutableList.builder();
        for (int i = 0; i < hashChannels.length; i++) {
            outputChannels.add(i);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        if (inputHashChannel.isPresent()) {
            this.precomputedHashChannel = OptionalInt.of(hashChannels.length);
            channelBuilders.add(ObjectArrayList.wrap(new Block[1024], 0));
        }
        else {
            this.precomputedHashChannel = OptionalInt.empty();
        }
        this.channelBuilders = channelBuilders.build();
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(this.types, outputChannels.build());
        hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(this.channelBuilders, this.precomputedHashChannel);

        startNewPage();

        // reserve memory for the arrays
        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        rawHashByHashPosition = new byte[hashCapacity];
        groupIdsByHash = new int[hashCapacity];
        Arrays.fill(groupIdsByHash, -1);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getRawHash(int groupId)
    {
        int blockIndex = groupId >> VALUES_PAGE_BITS;
        int position = groupId & VALUES_PAGE_MASK;
        return hashStrategy.hashPosition(blockIndex, position);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupIdsByHash) +
                sizeOf(rawHashByHashPosition) +
                preallocatedMemoryInBytes +
                (dictionaryLookBack != null ? dictionaryLookBack.getRetainedSizeInBytes() : 0);
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        int blockIndex = groupId >> VALUES_PAGE_BITS;
        int position = groupId & VALUES_PAGE_MASK;
        hashStrategy.appendTo(blockIndex, position, pageBuilder, 0);
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new AddRunLengthEncodedPageWork(page);
        }
        if (canProcessDictionary(page)) {
            return new AddDictionaryPageWork(page);
        }
        if (canProcessLowCardinalityDictionary(page)) {
            return new AddLowCardinalityDictionaryPageWork(page);
        }

        return new AddNonDictionaryPageWork(page);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new GetRunLengthEncodedGroupIdsWork(page);
        }
        if (canProcessDictionary(page)) {
            return new GetDictionaryGroupIdsWork(page);
        }
        if (canProcessLowCardinalityDictionary(page)) {
            return new GetLowCardinalityDictionaryGroupIdsWork(page);
        }

        return new GetNonDictionaryGroupIdsWork(page);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        long rawHash = hashStrategy.hashRow(position, page);
        return contains(position, page, hashChannels, rawHash);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        int hashPosition = getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupIdsByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupIdsByHash[hashPosition], hashPosition, position, page, (byte) rawHash, hashChannels)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    private int putIfAbsent(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return putIfAbsent(position, page, rawHash);
    }

    private int putIfAbsent(int position, Page page, long rawHash)
    {
        int hashPosition = getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupIdsByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupIdsByHash[hashPosition], hashPosition, position, page, (byte) rawHash, channels)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page, rawHash);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, int position, Page page, long rawHash)
    {
        // add the row to the open page
        for (int i = 0; i < channels.length; i++) {
            int hashChannel = channels[i];
            Type type = types.get(i);
            type.appendTo(page.getBlock(hashChannel), position, currentPageBuilder.getBlockBuilder(i));
        }
        if (precomputedHashChannel.isPresent()) {
            BIGINT.writeLong(currentPageBuilder.getBlockBuilder(precomputedHashChannel.getAsInt()), rawHash);
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);
        // -1 is reserved for marking hash position as empty
        checkState(address != -1, "Address cannot be -1");

        // record group id in hash
        int groupId = nextGroupId++;

        rawHashByHashPosition[hashPosition] = (byte) rawHash;
        groupIdsByHash[hashPosition] = groupId;

        // create new page builder if this page is full
        if (currentPageBuilder.getPositionCount() == VALUES_PAGE_MAX_ROW_COUNT) {
            startNewPage();
        }

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private void startNewPage()
    {
        if (currentPageBuilder != null) {
            completedPagesMemorySize += currentPageBuilder.getRetainedSizeInBytes();
            currentPageBuilder.reset(currentPageBuilder.getPositionCount());
        }
        else {
            currentPageBuilder = new PageBuilder(types);
        }

        for (int i = 0; i < types.size(); i++) {
            channelBuilders.get(i).add(currentPageBuilder.getBlockBuilder(i));
        }
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for rawHashByHashPosition, groupIdsByHash as well as the size of the current page
        preallocatedMemoryInBytes = newCapacity * (long) (Integer.BYTES + Byte.BYTES)
                + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }

        int newMask = newCapacity - 1;
        byte[] rawHashes = new byte[newCapacity];
        int[] newGroupIdByHash = new int[newCapacity];
        Arrays.fill(newGroupIdByHash, -1);

        for (int i = 0; i < hashCapacity; i++) {
            // seek to the next used slot
            int groupId = groupIdsByHash[i];
            if (groupId == -1) {
                continue;
            }

            long rawHash = hashPosition(groupId);
            // find an empty slot for the address
            int pos = getHashPosition(rawHash, newMask);
            while (newGroupIdByHash[pos] != -1) {
                pos = (pos + 1) & newMask;
            }

            // record the mapping
            rawHashes[pos] = (byte) rawHash;
            newGroupIdByHash[pos] = groupId;
        }

        this.mask = newMask;
        this.hashCapacity = newCapacity;
        this.maxFill = calculateMaxFill(newCapacity);
        this.rawHashByHashPosition = rawHashes;
        this.groupIdsByHash = newGroupIdByHash;

        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();
        return true;
    }

    private long hashPosition(int groupId)
    {
        int blockIndex = groupId >> VALUES_PAGE_BITS;
        int blockPosition = groupId & VALUES_PAGE_MASK;
        if (precomputedHashChannel.isPresent()) {
            return getRawHash(blockIndex, blockPosition, precomputedHashChannel.getAsInt());
        }
        return hashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private long getRawHash(int sliceIndex, int position, int hashChannel)
    {
        return channelBuilders.get(hashChannel).get(sliceIndex).getLong(position, 0);
    }

    private boolean positionNotDistinctFromCurrentRow(int groupId, int hashPosition, int position, Page page, byte rawHash, int[] hashChannels)
    {
        if (rawHashByHashPosition[hashPosition] != rawHash) {
            return false;
        }
        int blockIndex = groupId >> VALUES_PAGE_BITS;
        int blockPosition = groupId & VALUES_PAGE_MASK;
        return hashStrategy.positionNotDistinctFromRow(blockIndex, blockPosition, position, page, hashChannels);
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return (int) (murmurHash3(rawHash) & mask); // mask is int so casting is safe
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

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    // For a page that contains DictionaryBlocks, create a new page in which
    // the dictionaries from the DictionaryBlocks are extracted into the corresponding channels
    // From Page(DictionaryBlock1, DictionaryBlock2) create new page with Page(dictionary1, dictionary2)
    private Page createPageWithExtractedDictionary(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        Block dictionary = ((DictionaryBlock) page.getBlock(channels[0])).getDictionary();

        // extract data dictionary
        blocks[channels[0]] = dictionary;

        // extract hash dictionary
        inputHashChannel.ifPresent(integer -> blocks[integer] = ((DictionaryBlock) page.getBlock(integer)).getDictionary());

        return new Page(dictionary.getPositionCount(), blocks);
    }

    private boolean canProcessDictionary(Page page)
    {
        if (!this.processDictionary || channels.length > 1 || !(page.getBlock(channels[0]) instanceof DictionaryBlock)) {
            return false;
        }

        if (inputHashChannel.isPresent()) {
            Block inputHashBlock = page.getBlock(inputHashChannel.get());
            DictionaryBlock inputDataBlock = (DictionaryBlock) page.getBlock(channels[0]);

            if (!(inputHashBlock instanceof DictionaryBlock)) {
                // data channel is dictionary encoded but hash channel is not
                return false;
            }
            // dictionarySourceIds of data block and hash block do not match
            return ((DictionaryBlock) inputHashBlock).getDictionarySourceId().equals(inputDataBlock.getDictionarySourceId());
        }

        return true;
    }

    private boolean canProcessLowCardinalityDictionary(Page page)
    {
        // We don't have to rely on 'optimizer.dictionary-aggregations' here since there is little to none chance of regression
        int positionCount = page.getPositionCount();
        long cardinality = 1;
        for (int channel : channels) {
            if (!(page.getBlock(channel) instanceof DictionaryBlock)) {
                return false;
            }
            cardinality = multiplyExact(cardinality, ((DictionaryBlock) page.getBlock(channel)).getDictionary().getPositionCount());
            if (cardinality > positionCount * SMALL_DICTIONARIES_MAX_CARDINALITY_RATIO
                    || cardinality > Short.MAX_VALUE) { // Need to fit into short array
                return false;
            }
        }

        return true;
    }

    private boolean isRunLengthEncoded(Page page)
    {
        for (int channel : channels) {
            if (!(page.getBlock(channel) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    private int registerGroupId(HashGenerator hashGenerator, Page page, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, page, hashGenerator.hashPosition(positionInDictionary, page));
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    private static final class DictionaryLookBack
    {
        private static final int INSTANCE_SIZE = instanceSize(DictionaryLookBack.class);
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, -1);
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

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    sizeOf(processed) +
                    dictionary.getRetainedSizeInBytes();
        }
    }

    @VisibleForTesting
    class AddNonDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;
        private int lastPosition;

        public AddNonDictionaryPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    putIfAbsent(i, page);
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
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private int lastPosition;

        public AddDictionaryPageWork(Page page)
        {
            verify(canProcessDictionary(page), "invalid call to addDictionaryPage");
            this.page = requireNonNull(page, "page is null");
            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                registerGroupId(hashGenerator, dictionaryPage, positionInDictionary);
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

    class AddLowCardinalityDictionaryPageWork
            implements Work<Void>
    {
        private final Page page;
        @Nullable
        private int[] combinationIdToPosition;
        private int nextCombinationId;

        public AddLowCardinalityDictionaryPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            if (combinationIdToPosition == null) {
                combinationIdToPosition = calculateCombinationIdToPositionMapping(page);
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            for (int combinationId = nextCombinationId; combinationId < combinationIdToPosition.length; combinationId++) {
                int position = combinationIdToPosition[combinationId];
                if (position != -1) {
                    if (needRehash()) {
                        nextCombinationId = combinationId;
                        return false;
                    }
                    putIfAbsent(position, page);
                }
            }
            return true;
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
        private final Page page;

        private boolean finished;

        public AddRunLengthEncodedPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (page.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(0, page);
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
    class GetNonDictionaryGroupIdsWork
            implements Work<int[]>
    {
        private final int[] groupIds;
        private final Page page;

        private boolean finished;
        private int lastPosition;

        public GetNonDictionaryGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            groupIds = new int[page.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    // output the group id for this row
                    groupIds[i] = putIfAbsent(i, page);
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
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetLowCardinalityDictionaryGroupIdsWork
            implements Work<int[]>
    {
        private final Page page;
        private final int[] groupIds;
        @Nullable
        private short[] positionToCombinationId;
        @Nullable
        private int[] combinationIdToGroupId;
        private int nextPosition;
        private boolean finished;

        public GetLowCardinalityDictionaryGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            groupIds = new int[page.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            if (positionToCombinationId == null) {
                positionToCombinationId = new short[groupIds.length];
                int maxCardinality = calculatePositionToCombinationIdMapping(page, positionToCombinationId);
                combinationIdToGroupId = new int[maxCardinality];
                Arrays.fill(combinationIdToGroupId, -1);
            }

            for (int position = nextPosition; position < groupIds.length; position++) {
                short combinationId = positionToCombinationId[position];
                int groupId = combinationIdToGroupId[combinationId];
                if (groupId == -1) {
                    // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
                    // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
                    if (needRehash()) {
                        nextPosition = position;
                        return false;
                    }
                    groupId = putIfAbsent(position, page);
                    combinationIdToGroupId[combinationId] = groupId;
                }
                groupIds[position] = groupId;
            }
            return true;
        }

        @Override
        public int[] getResult()
        {
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
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            verify(canProcessDictionary(page), "invalid call to processDictionary");

            this.dictionaryBlock = (DictionaryBlock) page.getBlock(channels[0]);
            updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = createPageWithExtractedDictionary(page);
            groupIds = new int[page.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
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
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                groupIds[lastPosition] = registerGroupId(hashGenerator, dictionaryPage, positionInDictionary);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetRunLengthEncodedGroupIdsWork
            implements Work<int[]>
    {
        private final Page page;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (page.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(0, page);
            processFinished = true;
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            int[] groupIds = new int[page.getPositionCount()];
            Arrays.fill(groupIds, groupId);
            return groupIds;
        }
    }

    /**
     * Returns an array containing a position that corresponds to the low cardinality
     * dictionary combinationId, or a value of -1 if no position exists within the page
     * for that combinationId.
     */
    private int[] calculateCombinationIdToPositionMapping(Page page)
    {
        short[] positionToCombinationId = new short[page.getPositionCount()];
        int maxCardinality = calculatePositionToCombinationIdMapping(page, positionToCombinationId);

        int[] combinationIdToPosition = new int[maxCardinality];
        Arrays.fill(combinationIdToPosition, -1);
        for (int position = 0; position < positionToCombinationId.length; position++) {
            combinationIdToPosition[positionToCombinationId[position]] = position;
        }
        return combinationIdToPosition;
    }

    /**
     * Returns the number of combinations of all dictionary ids in input page blocks and populates
     * positionToCombinationIds with the combinationId for each position in the input Page
     */
    private int calculatePositionToCombinationIdMapping(Page page, short[] positionToCombinationIds)
    {
        checkArgument(positionToCombinationIds.length == page.getPositionCount());

        int maxCardinality = 1;
        for (int channel = 0; channel < channels.length; channel++) {
            Block block = page.getBlock(channels[channel]);
            verify(block instanceof DictionaryBlock, "Only dictionary blocks are supported");
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
            int dictionarySize = dictionaryBlock.getDictionary().getPositionCount();
            maxCardinality *= dictionarySize;
            if (channel == 0) {
                for (int position = 0; position < positionToCombinationIds.length; position++) {
                    positionToCombinationIds[position] = (short) dictionaryBlock.getId(position);
                }
            }
            else {
                for (int position = 0; position < positionToCombinationIds.length; position++) {
                    short combinationId = positionToCombinationIds[position];
                    combinationId *= dictionarySize;
                    combinationId += dictionaryBlock.getId(position);
                    positionToCombinationIds[position] = combinationId;
                }
            }
        }
        return maxCardinality;
    }

    private boolean ensureHashTableSize(int batchSize)
    {
        int positionCountUntilRehash = maxFill - nextGroupId;
        while (positionCountUntilRehash < batchSize) {
            if (!tryRehash()) {
                return false;
            }
            positionCountUntilRehash = maxFill - nextGroupId;
        }
        return true;
    }
}
