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
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;

import java.util.Optional;

import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

public class ChannelSet
{
    private final GroupByHash hash;
    private final boolean containsNull;

    private ChannelSet(GroupByHash hash, boolean containsNull)
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

        private final Type type;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext localMemoryContext;
        private final GroupByHash hash;

        public ChannelSetBuilder(Type type, boolean hasHashChannel, int expectedPositions, OperatorContext operatorContext, JoinCompiler joinCompiler, TypeOperators typeOperators)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.localMemoryContext = operatorContext.localUserMemoryContext();
            this.hash = createGroupByHash(
                    operatorContext.getSession(),
                    ImmutableList.of(type),
                    HASH_CHANNELS,
                    hasHashChannel ? Optional.of(1) : Optional.empty(),
                    expectedPositions,
                    joinCompiler,
                    typeOperators,
                    this::updateMemoryReservation);
        }

        public ChannelSet build()
        {
            Page nullBlockPage = new Page(type.createBlockBuilder(null, 1, UNKNOWN.getFixedSize()).appendNull().build());
            boolean containsNull = hash.contains(0, nullBlockPage);
            return new ChannelSet(hash, containsNull);
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
