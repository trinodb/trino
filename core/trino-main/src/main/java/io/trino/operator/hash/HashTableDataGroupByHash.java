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
package io.trino.operator.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.operator.GroupByHash;
import io.trino.operator.GroupByIdBlock;
import io.trino.operator.HashGenerator;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.MultiChannelGroupByHash.DictionaryLookBack;
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.hash.fixedwidth.SingleTableHashTableData;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HashTableDataGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashTableDataGroupByHash.class).instanceSize();

    static final float FILL_RATIO = 0.75f;

    private final Optional<Integer> inputHashChannel;
    private final int[] hashChannelsWithHash;

    // the hash table with values + groupId entries.

    private HashTableData hashTableData;
    private double expectedHashCollisions;
    private final boolean processDictionary;
    private DictionaryLookBack dictionaryLookBack;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;
    private final List<Type> types;

    private final HashTableManager hashTableManager;

    public HashTableDataGroupByHash(
            IsolatedHashTableFactory isolatedHashTableFactory,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            UpdateMemory updateMemory,
            BlockTypeOperators blockTypeOperators)
    {
        this.processDictionary = processDictionary;
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        requireNonNull(inputHashChannel, "inputHashChannel is null");

        if (inputHashChannel.isPresent()) {
            int channel = inputHashChannel.get();
            this.inputHashChannel = Optional.of(hashChannels.length);
            this.hashChannelsWithHash = Arrays.copyOf(hashChannels, hashChannels.length + 1);
            // map inputHashChannel to last column
            this.hashChannelsWithHash[hashChannels.length] = channel;
        }
        else {
            this.inputHashChannel = Optional.empty();
            this.hashChannelsWithHash = hashChannels;
        }

        this.hashTableManager = new HashTableManager(
                this,
                isolatedHashTableFactory,
                hashTypes,
                createHashGenerator(hashTypes, this.inputHashChannel, blockTypeOperators));
        this.hashTableData = new SingleTableHashTableData(hashTypes.size(), arraySize(expectedSize, FILL_RATIO), hashTableManager.valuesLTotalLength());

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : ImmutableList.copyOf(hashTypes);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                hashTableData.getEstimatedSize() +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashTableData.getHashCollisions();
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashTableData.getCapacity());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return hashTableData.getHashTableSize();
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        hashTableManager.valuesAppender().appendValuesTo(hashTableData, groupId, pageBuilder, 0, inputHashChannel.isPresent());
    }

    @Override
    public WorkProcessor<Page> buildResult(IntIterator groupIds, PageBuilder pageBuilder, List<GroupedAggregator> groupedAggregators)
    {
        return hashTableManager.valuesAppender().buildResult(hashTableData, groupIds, pageBuilder, groupedAggregators, types.size(), inputHashChannel.isPresent());
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();

        Page minimalPage = page.getColumns(hashChannelsWithHash);
        if (isRunLengthEncoded(minimalPage)) {
            return new AddRunLengthEncodedPageWork(this, hashTableManager.getHashTable(minimalPage), minimalPage);
        }
        if (canProcessDictionary(minimalPage)) {
            return hashTableManager.getAAddDictionaryPageWork(minimalPage);
        }

        return hashTableManager.getAddPageWork(minimalPage);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();

        Page minimalPage = page.getColumns(hashChannelsWithHash);
        if (isRunLengthEncoded(minimalPage)) {
            return new GetRunLengthEncodedGroupIdsWork(this, hashTableManager.getHashTable(minimalPage), minimalPage);
        }
        if (canProcessDictionary(minimalPage)) {
            return hashTableManager.getGetDictionaryGroupIdsWorkFactory(minimalPage);
        }
        return hashTableManager.getGetGroupIdsWorkFactory(minimalPage);
    }

    @Override
    public boolean contains(int position, Page page)
    {
        return hashTableManager.getForContains(page).contains(hashTableData, position, page);
    }

    @Override
    public boolean contains(int position, Page page, long rawHash)
    {
        return hashTableManager.getForContains(page).contains(hashTableData, position, page, rawHash);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return hashTableData.getRawHash(groupId);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashTableData.getCapacity();
    }

    public boolean tryRehash()
    {
        return tryRehash(hashTableData.getCapacity() * 2L);
    }

    private boolean tryRehash(long newCapacityLong)
    {
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);
        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashTableData.getCapacity()) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - hashTableData.maxFill()) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashTableData.getCapacity());

        hashTableData = hashTableData.resize(newCapacity);
        return true;
    }

    public static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    public HashTableData getHashTableData()
    {
        return hashTableData;
    }

    public static class AddPageWork
            implements Work<Void>
    {
        private final GroupByHashTable hashTable;
        private final HashTableDataGroupByHash groupByHash;
        private final Page page;
        private int lastPosition;

        public AddPageWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = requireNonNull(groupByHash, "groupByHash is null");
            this.page = requireNonNull(page, "page is null");
            this.hashTable = requireNonNull(hashTable, "hashTable is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            while (lastPosition < positionCount) {
                hashTable.putIfAbsent(data, lastPosition, page);
                lastPosition++;
                if (data.needRehash()) {
                    if (!groupByHash.tryRehash()) {
                        // memory not available
                        break;
                    }
                    data = groupByHash.getHashTableData();
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final HashTableDataGroupByHash groupByHash;
        private final GroupByHashTable hashTable;
        private final Page page;

        private boolean finished;

        public AddRunLengthEncodedPageWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = requireNonNull(groupByHash, "groupByHash is null");
            this.page = requireNonNull(page, "page is null");
            this.hashTable = requireNonNull(hashTable, "hashTable is null");
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
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            // Only needs to process the first row since it is Run Length Encoded
            hashTable.putIfAbsent(data, 0, page);
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class GetRunLengthEncodedGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final HashTableDataGroupByHash groupByHash;
        private final GroupByHashTable hashTable;
        private final Page page;

        int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = requireNonNull(groupByHash, "groupByHash is null");
            this.page = requireNonNull(page, "page is null");
            this.hashTable = requireNonNull(hashTable, "hashTable is null");
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
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            // Only needs to process the first row since it is Run Length Encoded
            groupId = hashTable.putIfAbsent(data, 0, page);

            processFinished = true;
            return true;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            return new GroupByIdBlock(
                    groupByHash.getGroupCount(),
                    new RunLengthEncodedBlock(
                            BIGINT.createFixedSizeBlockBuilder(1).writeLong(groupId).build(),
                            page.getPositionCount()));
        }
    }

    public static class AddDictionaryPageWork
            implements Work<Void>
    {
        private final HashTableDataGroupByHash groupByHash;
        private final GroupByHashTable hashTable;
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private int lastPosition;

        public AddDictionaryPageWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = groupByHash;
            this.hashTable = hashTable;
            verify(groupByHash.canProcessDictionary(page), "invalid call to addDictionaryPage");
            this.page = requireNonNull(page, "page is null");
            this.dictionaryBlock = (DictionaryBlock) page.getBlock(0);
            groupByHash.updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = groupByHash.createPageWithExtractedDictionary(page);
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            while (lastPosition < positionCount) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                groupByHash.registerGroupId(hashTable, data, dictionaryPage, positionInDictionary);
                lastPosition++;
                if (data.needRehash()) {
                    if (!groupByHash.tryRehash()) {
                        // memory not available
                        break;
                    }
                    data = groupByHash.getHashTableData();
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class GetDictionaryGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final long[] groupIds;
        private final HashTableDataGroupByHash groupByHash;
        private final GroupByHashTable hashTable;
        private final Page page;
        private final Page dictionaryPage;
        private final DictionaryBlock dictionaryBlock;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = groupByHash;
            this.hashTable = hashTable;
            verify(groupByHash.canProcessDictionary(page), "invalid call to addDictionaryPage");
            this.page = requireNonNull(page, "page is null");
            this.dictionaryBlock = (DictionaryBlock) page.getBlock(0);
            groupByHash.updateDictionaryLookBack(dictionaryBlock.getDictionary());
            this.dictionaryPage = groupByHash.createPageWithExtractedDictionary(page);
            this.groupIds = new long[page.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            while (lastPosition < positionCount) {
                int positionInDictionary = dictionaryBlock.getId(lastPosition);
                int groupId = groupByHash.registerGroupId(hashTable, data, dictionaryPage, positionInDictionary);
                groupIds[lastPosition] = groupId;
                lastPosition++;
                if (data.needRehash()) {
                    if (!groupByHash.tryRehash()) {
                        // memory not available
                        break;
                    }
                    data = groupByHash.getHashTableData();
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(groupByHash.getGroupCount(), new LongArrayBlock(page.getPositionCount(), Optional.empty(), groupIds));
        }
    }

    public static class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final HashTableDataGroupByHash groupByHash;
        private final GroupByHashTable hashTable;
        private final long[] groupIds;
        private final Page page;
        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(HashTableDataGroupByHash groupByHash, GroupByHashTable hashTable, Page page)
        {
            this.groupByHash = requireNonNull(groupByHash, "groupByHash is null");
            this.hashTable = requireNonNull(hashTable, "hashTable is null");
            this.page = requireNonNull(page, "page is null");
            this.groupIds = new long[page.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (groupByHash.getHashTableData().needRehash() && !groupByHash.tryRehash()) {
                return false;
            }

            HashTableData data = groupByHash.getHashTableData();
            while (lastPosition < positionCount && !data.needRehash()) {
                // output the group id for this row
                int groupId = hashTable.putIfAbsent(data, lastPosition, page);
                groupIds[lastPosition] = groupId;
                lastPosition++;
                if (data.needRehash()) {
                    if (!groupByHash.tryRehash()) {
                        // memory not available
                        break;
                    }
                    data = groupByHash.getHashTableData();
                }
            }
            return lastPosition == positionCount;
        }

        @Override
        public GroupByIdBlock getResult()
        {
            checkState(lastPosition == page.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return new GroupByIdBlock(groupByHash.getGroupCount(), new LongArrayBlock(page.getPositionCount(), Optional.empty(), groupIds));
        }
    }

    public boolean canProcessDictionary(Page page)
    {
        if (!this.processDictionary || page.getChannelCount() > 1 || !(page.getBlock(0) instanceof DictionaryBlock)) {
            return false;
        }

        if (inputHashChannel.isPresent()) {
            Block inputHashBlock = page.getBlock(inputHashChannel.get());
            DictionaryBlock inputDataBlock = (DictionaryBlock) page.getBlock(0);

            if (!(inputHashBlock instanceof DictionaryBlock)) {
                // data channel is dictionary encoded but hash channel is not
                return false;
            }
            if (!((DictionaryBlock) inputHashBlock).getDictionarySourceId().equals(inputDataBlock.getDictionarySourceId())) {
                // dictionarySourceIds of data block and hash block do not match
                return false;
            }
        }

        return true;
    }

    private boolean isRunLengthEncoded(Page page)
    {
        for (int i = 0; i < page.getChannelCount(); i++) {
            if (!(page.getBlock(i) instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    // For a page that contains DictionaryBlocks, create a new page in which
    // the dictionaries from the DictionaryBlocks are extracted into the corresponding channels
    // From Page(DictionaryBlock1, DictionaryBlock2) create new page with Page(dictionary1, dictionary2)
    public Page createPageWithExtractedDictionary(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        Block dictionary = ((DictionaryBlock) page.getBlock(0)).getDictionary();

        // extract data dictionary
        blocks[0] = dictionary;

        // extract hash dictionary
        inputHashChannel.ifPresent(channel -> blocks[channel] = ((DictionaryBlock) page.getBlock(channel)).getDictionary());

        return new Page(dictionary.getPositionCount(), blocks);
    }

    public void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    public int registerGroupId(GroupByHashTable hashTable, HashTableData data, Page page, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = hashTable.putIfAbsent(data, positionInDictionary, page);
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    private static HashGenerator createHashGenerator(List<? extends Type> hashTypes, Optional<Integer> inputHashChannel, BlockTypeOperators blockTypeOperators)
    {
        return inputHashChannel.isPresent() ?
                new PrecomputedHashGenerator(inputHashChannel.get()) :
                InterpretedHashGenerator.createPositionalWithTypes(ImmutableList.copyOf(hashTypes), blockTypeOperators);
    }
}
