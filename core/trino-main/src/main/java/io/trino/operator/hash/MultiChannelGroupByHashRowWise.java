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
import io.trino.operator.PrecomputedHashGenerator;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MultiChannelGroupByHashRowWise
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByHashRowWise.class).instanceSize();

    static final float FILL_RATIO = 0.75f;

    private final Optional<Integer> inputHashChannel;
    private final NoRehashHashTableFactory hashTableFactory;
    private final int[] hashChannels;
    private final int[] hashChannelsWithHash;

    // the hash table with values + groupId entries.
    private NoRehashHashTable hashTable;

    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    private final List<Type> types;

    public MultiChannelGroupByHashRowWise(
            IsolatedHashTableRowFormatFactory isolatedHashTableRowFormatFactory,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            UpdateMemory updateMemory,
            BlockTypeOperators blockTypeOperators,
            int maxVarWidthBufferSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        checkArgument(hashChannels.length > 0, "hashChannels.length must be at least 1");
        this.hashChannels = hashChannels;
        requireNonNull(inputHashChannel, "inputHashChannel is null");

        if (inputHashChannel.isPresent()) {
            int channel = inputHashChannel.get();
            int mappedChannel = -1;
            for (int i = 0; i < hashChannels.length; i++) {
                if (hashChannels[i] == channel) {
                    mappedChannel = i;
                    break;
                }
            }
            if (mappedChannel == -1) {
                this.inputHashChannel = Optional.of(hashChannels.length);
                this.hashChannelsWithHash = Arrays.copyOf(hashChannels, hashChannels.length + 1);
                // map inputHashChannel to last column
                this.hashChannelsWithHash[hashChannels.length] = channel;
            }
            else {
                // inputHashChannel is one of the hashChannels
                this.inputHashChannel = inputHashChannel;
                this.hashChannelsWithHash = hashChannels;
            }
        }
        else {
            this.inputHashChannel = Optional.empty();
            this.hashChannelsWithHash = hashChannels;
        }
        this.hashTableFactory = new NoRehashHashTableFactory(
                hashTypes,
                isolatedHashTableRowFormatFactory.create(hashTypes, maxVarWidthBufferSize),
                createHashGenerator(hashTypes, this.inputHashChannel, blockTypeOperators));
        this.hashTable = hashTableFactory.create(expectedSize);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
        this.types = inputHashChannel.isPresent() ? ImmutableList.copyOf(Iterables.concat(hashTypes, ImmutableList.of(BIGINT))) : ImmutableList.copyOf(hashTypes);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                hashTable.getEstimatedSize() +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashTable.getHashCollisions();
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashTable.getCapacity());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return hashTable.getSize();
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        hashTable.appendValuesTo(groupId, pageBuilder, outputChannelOffset, inputHashChannel.isPresent());
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();

        return new AddPageWork(page.getColumns(hashChannelsWithHash));
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(page.getColumns(hashChannelsWithHash));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        // TODO lysy: we are ignoring hashChannels here
        return hashTable.contains(position, page);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        // TODO lysy: we are ignoring hashChannels here
        return hashTable.contains(position, page, rawHash);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return hashTable.getRawHash(groupId);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashTable.getCapacity();
    }

    private boolean tryRehash()
    {
        return tryRehash(hashTable.getCapacity() * 2L);
    }

    private boolean tryRehash(long newCapacityLong)
    {
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);
        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashTable.getCapacity()) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - hashTable.maxFill()) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashTable.getCapacity());

        hashTable = hashTable.resize(newCapacity);
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

    private class AddPageWork
            implements Work<Void>
    {
        private final Page page;
        private int lastPosition;

        private AddPageWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelGroupByHashRowWise.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                hashTable.putIfAbsent(lastPosition, page);
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelGroupByHashRowWise.this.hashTable;
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

    private class GetGroupIdsWork
            implements Work<GroupByIdBlock>
    {
        private final BlockBuilder blockBuilder;
        private final Page page;
        private boolean finished;
        private int lastPosition;

        private GetGroupIdsWork(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            // we know the exact size required for the block
            this.blockBuilder = BIGINT.createFixedSizeBlockBuilder(page.getPositionCount());
        }

        @Override
        public boolean process()
        {
            int positionCount = page.getPositionCount();
            checkState(lastPosition < positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (hashTable.needRehash() && !tryRehash()) {
                return false;
            }

            NoRehashHashTable hashTable = MultiChannelGroupByHashRowWise.this.hashTable;
            while (lastPosition < positionCount && !hashTable.needRehash()) {
                // output the group id for this row
                int groupId = hashTable.putIfAbsent(lastPosition, page);
                BIGINT.writeLong(blockBuilder, groupId);
                lastPosition++;
                if (hashTable.needRehash()) {
                    tryRehash();
                    hashTable = MultiChannelGroupByHashRowWise.this.hashTable;
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
            return new GroupByIdBlock(getGroupCount(), blockBuilder.build());
        }
    }

    private static HashGenerator createHashGenerator(List<? extends Type> hashTypes, Optional<Integer> inputHashChannel, BlockTypeOperators blockTypeOperators)
    {
        return inputHashChannel.isPresent() ?
                new PrecomputedHashGenerator(inputHashChannel.get()) :
                InterpretedHashGenerator.createPositionalWithTypes(ImmutableList.copyOf(hashTypes), blockTypeOperators);
    }
}
