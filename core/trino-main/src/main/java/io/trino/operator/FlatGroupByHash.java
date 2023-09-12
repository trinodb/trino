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
import com.google.common.primitives.Shorts;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;

// This implementation assumes arrays used in the hash are always a power of 2
public class FlatGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(FlatGroupByHash.class);
    private static final int BATCH_SIZE = 1024;
    // Max (page value count / cumulative dictionary size) to trigger the low cardinality case
    private static final double SMALL_DICTIONARIES_MAX_CARDINALITY_RATIO = 0.25;

    private final FlatHash flatHash;
    private final int groupByChannelCount;
    private final boolean hasPrecomputedHash;

    private final boolean processDictionary;

    private DictionaryLookBack dictionaryLookBack;

    private long currentPageSizeInBytes;

    // reusable arrays for the blocks and block builders
    private final Block[] currentBlocks;
    private final BlockBuilder[] currentBlockBuilders;

    public FlatGroupByHash(
            List<Type> hashTypes,
            boolean hasPrecomputedHash,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory checkMemoryReservation)
    {
        this.flatHash = new FlatHash(joinCompiler.getFlatHashStrategy(hashTypes), hasPrecomputedHash, expectedSize, checkMemoryReservation);
        this.groupByChannelCount = hashTypes.size();
        this.hasPrecomputedHash = hasPrecomputedHash;

        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        int totalChannels = hashTypes.size() + (hasPrecomputedHash ? 1 : 0);
        this.currentBlocks = new Block[totalChannels];
        this.currentBlockBuilders = new BlockBuilder[totalChannels];

        this.processDictionary = processDictionary && hashTypes.size() == 1;
    }

    public int getPhysicalPosition(int groupId)
    {
        return flatHash.getPhysicalPosition(groupId);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return flatHash.hashPosition(groupId);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                flatHash.getEstimatedSize() +
                currentPageSizeInBytes +
                (dictionaryLookBack != null ? dictionaryLookBack.getRetainedSizeInBytes() : 0);
    }

    @Override
    public int getGroupCount()
    {
        return flatHash.size();
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        BlockBuilder[] blockBuilders = currentBlockBuilders;
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = pageBuilder.getBlockBuilder(i);
        }
        flatHash.appendTo(groupId, blockBuilders);
    }

    @Override
    public Work<?> addPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return new CompletedWork<>(new int[0]);
        }

        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block[] blocks = getBlocksFromPage(page);

        if (isRunLengthEncoded(blocks)) {
            return new AddRunLengthEncodedPageWork(blocks);
        }
        if (canProcessDictionary(blocks)) {
            return new AddDictionaryPageWork(blocks);
        }
        if (canProcessLowCardinalityDictionary(blocks)) {
            return new AddLowCardinalityDictionaryPageWork(blocks);
        }

        return new AddNonDictionaryPageWork(blocks);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        if (page.getPositionCount() == 0) {
            return new CompletedWork<>(new int[0]);
        }

        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block[] blocks = getBlocksFromPage(page);

        if (isRunLengthEncoded(blocks)) {
            return new GetRunLengthEncodedGroupIdsWork(blocks);
        }
        if (canProcessDictionary(blocks)) {
            return new GetDictionaryGroupIdsWork(blocks);
        }
        if (canProcessLowCardinalityDictionary(blocks)) {
            return new GetLowCardinalityDictionaryGroupIdsWork(blocks);
        }

        return new GetNonDictionaryGroupIdsWork(blocks);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return flatHash.getCapacity();
    }

    private int putIfAbsent(Block[] blocks, int position)
    {
        return flatHash.putIfAbsent(blocks, position);
    }

    private Block[] getBlocksFromPage(Page page)
    {
        Block[] blocks = currentBlocks;
        checkArgument(page.getChannelCount() == blocks.length);
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
        }
        return blocks;
    }

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    private boolean canProcessDictionary(Block[] blocks)
    {
        if (!processDictionary || !(blocks[0] instanceof DictionaryBlock inputDictionary)) {
            return false;
        }

        if (!hasPrecomputedHash) {
            return true;
        }

        // dictionarySourceIds of data block and hash block must match
        return blocks[1] instanceof DictionaryBlock hashDictionary &&
                hashDictionary.getDictionarySourceId().equals(inputDictionary.getDictionarySourceId());
    }

    private boolean canProcessLowCardinalityDictionary(Block[] blocks)
    {
        // We don't have to rely on 'optimizer.dictionary-aggregations' here since there is little to none chance of regression
        int positionCount = blocks[0].getPositionCount();
        long cardinality = 1;
        for (int channel = 0; channel < groupByChannelCount; channel++) {
            if (!(blocks[channel] instanceof DictionaryBlock dictionaryBlock)) {
                return false;
            }
            cardinality = multiplyExact(cardinality, dictionaryBlock.getDictionary().getPositionCount());
            if (cardinality > positionCount * SMALL_DICTIONARIES_MAX_CARDINALITY_RATIO
                    || cardinality > Short.MAX_VALUE) { // Must into fit into short[]
                return false;
            }
        }
        return true;
    }

    private boolean isRunLengthEncoded(Block[] blocks)
    {
        for (int channel = 0; channel < groupByChannelCount; channel++) {
            if (!(blocks[channel] instanceof RunLengthEncodedBlock)) {
                return false;
            }
        }
        return true;
    }

    private int registerGroupId(Block[] dictionaries, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(dictionaries, positionInDictionary);
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
        private final Block[] blocks;
        private int lastPosition;

        public AddNonDictionaryPageWork(Block[] blocks)
        {
            this.blocks = blocks;
        }

        @Override
        public boolean process()
        {
            int positionCount = blocks[0].getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!flatHash.ensureAvailableCapacity(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    putIfAbsent(blocks, i);
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
        private final DictionaryBlock dictionaryBlock;
        private final Block[] dictionaries;
        private int lastPosition;

        public AddDictionaryPageWork(Block[] blocks)
        {
            verify(canProcessDictionary(blocks), "invalid call to addDictionaryPage");
            this.dictionaryBlock = (DictionaryBlock) blocks[0];

            this.dictionaries = Arrays.stream(blocks)
                    .map(block -> (DictionaryBlock) block)
                    .map(DictionaryBlock::getDictionary)
                    .toArray(Block[]::new);
            updateDictionaryLookBack(dictionaries[0]);
        }

        @Override
        public boolean process()
        {
            int positionCount = dictionaryBlock.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            while (lastPosition < positionCount && flatHash.ensureAvailableCapacity(1)) {
                registerGroupId(dictionaries, dictionaryBlock.getId(lastPosition));
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
        private final Block[] blocks;
        private final int[] combinationIdToPosition;
        private int nextCombinationId;

        public AddLowCardinalityDictionaryPageWork(Block[] blocks)
        {
            this.blocks = blocks;
            this.combinationIdToPosition = calculateCombinationIdToPositionMapping(blocks);
        }

        @Override
        public boolean process()
        {
            for (int combinationId = nextCombinationId; combinationId < combinationIdToPosition.length; combinationId++) {
                int position = combinationIdToPosition[combinationId];
                if (position != -1) {
                    if (!flatHash.ensureAvailableCapacity(1)) {
                        nextCombinationId = combinationId;
                        return false;
                    }
                    putIfAbsent(blocks, position);
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
        private final Block[] blocks;
        private boolean finished;

        public AddRunLengthEncodedPageWork(Block[] blocks)
        {
            for (int i = 0; i < blocks.length; i++) {
                // GroupBy blocks are guaranteed to be RLE, but hash block might not be an RLE due to bugs
                // use getSingleValueBlock here, which for RLE is a no-op, but will still work if hash block is not RLE
                blocks[i] = blocks[i].getSingleValueBlock(0);
            }
            this.blocks = blocks;
        }

        @Override
        public boolean process()
        {
            checkState(!finished);

            if (!flatHash.ensureAvailableCapacity(1)) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(blocks, 0);
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
        private final Block[] blocks;
        private final int[] groupIds;

        private boolean finished;
        private int lastPosition;

        public GetNonDictionaryGroupIdsWork(Block[] blocks)
        {
            this.blocks = blocks;
            this.groupIds = new int[currentBlocks[0].getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = groupIds.length;
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!flatHash.ensureAvailableCapacity(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i++) {
                    groupIds[i] = putIfAbsent(blocks, i);
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
            checkState(lastPosition == currentBlocks[0].getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetLowCardinalityDictionaryGroupIdsWork
            implements Work<int[]>
    {
        private final Block[] blocks;
        private final short[] positionToCombinationId;
        private final int[] combinationIdToGroupId;
        private final int[] groupIds;

        private int nextPosition;
        private boolean finished;

        public GetLowCardinalityDictionaryGroupIdsWork(Block[] blocks)
        {
            this.blocks = blocks;

            int positionCount = blocks[0].getPositionCount();
            positionToCombinationId = new short[positionCount];
            int maxCardinality = calculatePositionToCombinationIdMapping(blocks, positionToCombinationId);

            combinationIdToGroupId = new int[maxCardinality];
            Arrays.fill(combinationIdToGroupId, -1);
            groupIds = new int[positionCount];
        }

        @Override
        public boolean process()
        {
            for (int position = nextPosition; position < positionToCombinationId.length; position++) {
                short combinationId = positionToCombinationId[position];
                int groupId = combinationIdToGroupId[combinationId];
                if (groupId == -1) {
                    if (!flatHash.ensureAvailableCapacity(1)) {
                        nextPosition = position;
                        return false;
                    }
                    groupId = putIfAbsent(blocks, position);
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
        private final DictionaryBlock dictionaryBlock;
        private final Block[] dictionaries;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(Block[] blocks)
        {
            verify(canProcessDictionary(blocks), "invalid call to processDictionary");

            this.dictionaryBlock = (DictionaryBlock) blocks[0];
            this.groupIds = new int[dictionaryBlock.getPositionCount()];

            this.dictionaries = Arrays.stream(blocks)
                    .map(block -> (DictionaryBlock) block)
                    .map(DictionaryBlock::getDictionary)
                    .toArray(Block[]::new);
            updateDictionaryLookBack(dictionaries[0]);
        }

        @Override
        public boolean process()
        {
            checkState(lastPosition <= groupIds.length, "position count out of bound");
            checkState(!finished);

            while (lastPosition < groupIds.length && flatHash.ensureAvailableCapacity(1)) {
                groupIds[lastPosition] = registerGroupId(dictionaries, dictionaryBlock.getId(lastPosition));
                lastPosition++;
            }
            return lastPosition == groupIds.length;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == groupIds.length, "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetRunLengthEncodedGroupIdsWork
            implements Work<int[]>
    {
        private final int positionCount;
        private final Block[] blocks;
        private int groupId = -1;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(Block[] blocks)
        {
            positionCount = blocks[0].getPositionCount();
            for (int i = 0; i < blocks.length; i++) {
                // GroupBy blocks are guaranteed to be RLE, but hash block might not be an RLE due to bugs
                // use getSingleValueBlock here, which for RLE is a no-op, but will still work if hash block is not RLE
                blocks[i] = blocks[i].getSingleValueBlock(0);
            }
            this.blocks = blocks;
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);

            if (!flatHash.ensureAvailableCapacity(1)) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(blocks, 0);
            processFinished = true;
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            int[] groupIds = new int[positionCount];
            Arrays.fill(groupIds, groupId);
            return groupIds;
        }
    }

    /**
     * Returns an array containing a position that corresponds to the low cardinality
     * dictionary combinationId, or a value of -1 if no position exists within the page
     * for that combinationId.
     */
    private int[] calculateCombinationIdToPositionMapping(Block[] blocks)
    {
        short[] positionToCombinationId = new short[blocks[0].getPositionCount()];
        int maxCardinality = calculatePositionToCombinationIdMapping(blocks, positionToCombinationId);

        int[] combinationIdToPosition = new int[maxCardinality];
        Arrays.fill(combinationIdToPosition, -1);
        for (int position = 0; position < positionToCombinationId.length; position++) {
            combinationIdToPosition[positionToCombinationId[position]] = position;
        }
        return combinationIdToPosition;
    }

    /**
     * Returns the number of combinations in all dictionary ids in input page blocks and populates
     * positionToCombinationIds with the combinationId for each position in the input Page
     */
    private int calculatePositionToCombinationIdMapping(Block[] blocks, short[] positionToCombinationIds)
    {
        checkArgument(positionToCombinationIds.length == blocks[0].getPositionCount());

        int maxCardinality = 1;
        for (int channel = 0; channel < groupByChannelCount; channel++) {
            Block block = blocks[channel];
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
                    int combinationId = positionToCombinationIds[position];
                    combinationId *= dictionarySize;
                    combinationId += dictionaryBlock.getId(position);
                    positionToCombinationIds[position] = Shorts.checkedCast(combinationId);
                }
            }
        }
        return maxCardinality;
    }
}
