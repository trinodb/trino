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
package io.trino.operator.index;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHash;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.operator.index.IndexSnapshot.UNLOADED_INDEX_KEY;
import static java.util.Objects.requireNonNull;

public class UnloadedIndexKeyRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final List<PageAndPositions> pageAndPositions;

    public UnloadedIndexKeyRecordSet(
            Session session,
            IndexSnapshot existingSnapshot,
            Set<Integer> channelsForDistinct,
            List<Type> types,
            List<UpdateRequest> requests,
            FlatHashStrategyCompiler hashStrategyCompiler)
    {
        requireNonNull(existingSnapshot, "existingSnapshot is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        requireNonNull(requests, "requests is null");

        int[] distinctChannels = Ints.toArray(channelsForDistinct);
        List<Type> distinctChannelTypes = new ArrayList<>(distinctChannels.length);
        for (int i = 0; i < distinctChannels.length; i++) {
            distinctChannelTypes.add(types.get(distinctChannels[i]));
        }

        ImmutableList.Builder<PageAndPositions> builder = ImmutableList.builder();
        GroupByHash groupByHash = createGroupByHash(session, distinctChannelTypes, false, 10_000, hashStrategyCompiler, NOOP);
        for (UpdateRequest request : requests) {
            Page page = request.getPage();

            // Move through the positions while advancing the cursors in lockstep
            Work<int[]> work = groupByHash.getGroupIds(page.getColumns(distinctChannels));
            boolean done = work.process();
            // TODO: this class does not yield wrt memory limit; enable it
            verify(done);
            int[] groupIds = work.getResult();
            int positionCount = page.getBlock(0).getPositionCount();
            int nextDistinctId = -1;
            int groupCount = groupByHash.getGroupCount();
            IntList positions = new IntArrayList(groupCount);
            for (int position = 0; position < positionCount; position++) {
                // We are reading ahead in the cursors, so we need to filter any nulls since they cannot join
                if (!containsNullValue(position, page)) {
                    // Only include the key if it is not already in the index
                    if (existingSnapshot.getJoinPosition(position, page) == UNLOADED_INDEX_KEY) {
                        // Only add the position if we have not seen this tuple before (based on the distinct channels)
                        int groupId = groupIds[position];
                        if (nextDistinctId < groupId) {
                            nextDistinctId = groupId;
                            positions.add(position);
                        }
                    }
                }
            }

            if (!positions.isEmpty()) {
                builder.add(new PageAndPositions(request, positions));
            }
        }

        pageAndPositions = builder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public UnloadedIndexKeyRecordCursor cursor()
    {
        return new UnloadedIndexKeyRecordCursor(types, pageAndPositions);
    }

    private static boolean containsNullValue(int position, Page page)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    public static class UnloadedIndexKeyRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final Iterator<PageAndPositions> pageAndPositionsIterator;
        private Page page;
        private IntListIterator positionIterator;
        private int position;

        public UnloadedIndexKeyRecordCursor(List<Type> types, List<PageAndPositions> pageAndPositions)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.pageAndPositionsIterator = pageAndPositions.iterator();
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (positionIterator == null || !positionIterator.hasNext()) {
                if (!pageAndPositionsIterator.hasNext()) {
                    return false;
                }
                PageAndPositions pageAndPositions = pageAndPositionsIterator.next();
                page = pageAndPositions.getUpdateRequest().getPage();
                checkState(types.size() == page.getChannelCount());
                positionIterator = pageAndPositions.getPositions().iterator();
            }

            position = positionIterator.nextInt();

            return true;
        }

        public Page getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return types.get(field).getBoolean(page.getBlock(field), position);
        }

        @Override
        public long getLong(int field)
        {
            return types.get(field).getLong(page.getBlock(field), position);
        }

        @Override
        public double getDouble(int field)
        {
            return types.get(field).getDouble(page.getBlock(field), position);
        }

        @Override
        public Slice getSlice(int field)
        {
            return types.get(field).getSlice(page.getBlock(field), position);
        }

        @Override
        public Object getObject(int field)
        {
            return types.get(field).getObject(page.getBlock(field), position);
        }

        @Override
        public boolean isNull(int field)
        {
            return page.getBlock(field).isNull(position);
        }

        @Override
        public void close()
        {
            // Do nothing
        }
    }

    private static class PageAndPositions
    {
        private final UpdateRequest updateRequest;
        private final IntList positions;

        private PageAndPositions(UpdateRequest updateRequest, IntList positions)
        {
            this.updateRequest = requireNonNull(updateRequest, "updateRequest is null");
            this.positions = requireNonNull(positions, "positions is null");
        }

        private UpdateRequest getUpdateRequest()
        {
            return updateRequest;
        }

        private IntList getPositions()
        {
            return positions;
        }
    }
}
