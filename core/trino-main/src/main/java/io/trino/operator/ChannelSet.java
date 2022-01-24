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
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

public class ChannelSet
{
    private final GroupByHash hash;
    private final boolean containsNull;

    public ChannelSet(GroupByHash hash, boolean containsNull)
    {
        this.hash = hash;
        this.containsNull = containsNull;
    }

    public Type getType()
    {
        return hash.getTypes().get(0);
    }

    public long getEstimatedSizeInBytes()
    {
        return hash.getEstimatedSize();
    }

    public int size()
    {
        return hash.getGroupCount();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public boolean contains(int position, Page page)
    {
        return hash.contains(position, page);
    }

    public boolean contains(int position, Page page, long rawHash)
    {
        return hash.contains(position, page, rawHash);
    }

    public static class ChannelSetBuilder
    {
        private static final int[] HASH_CHANNELS = {0};

        private final GroupByHash hash;
        private final Page nullBlockPage;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext localMemoryContext;

        public ChannelSetBuilder(Type type, boolean hashPresent, int expectedPositions, OperatorContext operatorContext, GroupByHashFactory groupByHashFactory)
        {
            // Set builder has a single channel which goes in channel 0, if hash is present, add a hashBlock to channel 1
            Optional<Integer> hashChannel = hashPresent ? Optional.of(1) : Optional.empty();
            List<Type> types = ImmutableList.of(type);
            this.hash = groupByHashFactory.createGroupByHash(
                    operatorContext.getSession(),
                    types,
                    HASH_CHANNELS,
                    hashChannel,
                    expectedPositions,
                    this::updateMemoryReservation);
            this.nullBlockPage = createNullPage(type, hashPresent);
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.localMemoryContext = operatorContext.localUserMemoryContext();
        }

        private static Page createNullPage(Type type, boolean hashPresent)
        {
            Block nullBlock = type.createBlockBuilder(null, 1, UNKNOWN.getFixedSize()).appendNull().build();
            if (hashPresent) {
                Block nullHashCode = BigintType.BIGINT.createBlockBuilder(null, 1).writeLong(NULL_HASH_CODE).build();
                return new Page(nullBlock, nullHashCode);
            }
            else {
                return new Page(nullBlock);
            }
        }

        public ChannelSet build()
        {
            return new ChannelSet(hash, hash.contains(0, nullBlockPage));
        }

        public long getEstimatedSize()
        {
            return hash.getEstimatedSize();
        }

        public int size()
        {
            return hash.getGroupCount();
        }

        public Work<?> addPage(Page page)
        {
            // Just add the page to the pending work, which will be processed later.
            return hash.addPage(page);
        }

        public boolean updateMemoryReservation()
        {
            // If memory is not available, once we return, this operator will be blocked until memory is available.
            localMemoryContext.setBytes(hash.getEstimatedSize());

            // If memory is not available, inform the caller that we cannot proceed for allocation.
            return operatorContext.isWaitingForMemory().isDone();
        }

        @VisibleForTesting
        public int getCapacity()
        {
            return hash.getCapacity();
        }
    }
}
