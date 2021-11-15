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
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.operator.GroupByHash.createGroupByHash;

public class MarkDistinctHash
{
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, JoinCompiler joinCompiler, BlockTypeOperators blockTypeOperators, UpdateMemory updateMemory)
    {
        this(session, types, channels, hashChannel, 10_000, joinCompiler, blockTypeOperators, updateMemory);
    }

    public MarkDistinctHash(Session session, List<Type> types, int[] channels, Optional<Integer> hashChannel, int expectedDistinctValues, JoinCompiler joinCompiler, BlockTypeOperators blockTypeOperators, UpdateMemory updateMemory)
    {
        this.groupByHash = createGroupByHash(types, channels, hashChannel, expectedDistinctValues, isDictionaryAggregationEnabled(session), joinCompiler, blockTypeOperators, updateMemory);
    }

    public long getEstimatedSize()
    {
        return groupByHash.getEstimatedSize();
    }

    public Work<Block> markDistinctRows(Page page)
    {
        return new TransformWork<>(groupByHash.getGroupIds(page), this::processNextGroupIds);
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    private Block processNextGroupIds(GroupByIdBlock ids)
    {
        int positions = ids.getPositionCount();
        if (positions > 1) {
            // must have > 1 positions to benefit from using a RunLengthEncoded block
            if (nextDistinctId == ids.getGroupCount()) {
                // no new distinct positions
                return new RunLengthEncodedBlock(BooleanType.createBlockForSingleNonNullValue(false), positions);
            }
            if (nextDistinctId + positions == ids.getGroupCount()) {
                // all positions are distinct
                nextDistinctId = ids.getGroupCount();
                return new RunLengthEncodedBlock(BooleanType.createBlockForSingleNonNullValue(true), positions);
            }
        }
        byte[] distinctMask = new byte[positions];
        for (int position = 0; position < distinctMask.length; position++) {
            if (ids.getGroupId(position) == nextDistinctId) {
                distinctMask[position] = 1;
                nextDistinctId++;
            }
            else {
                distinctMask[position] = 0;
            }
        }
        checkState(nextDistinctId == ids.getGroupCount());
        return BooleanType.wrapByteArrayAsBooleanBlockWithoutNulls(distinctMask);
    }
}
