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
package io.trino.operator.aggregation.listagg;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AccumulatorState;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class SingleListaggAggregationState
        extends AbstractListaggAggregationState
{
    private final DynamicSliceOutput out = new DynamicSliceOutput(0);

    private SqlRow tempSerializedState;

    public SingleListaggAggregationState()
    {
        super(0);
    }

    private SingleListaggAggregationState(SingleListaggAggregationState state)
    {
        super(state);
        checkArgument(state.tempSerializedState == null, "state.tempSerializedState is not null");
        tempSerializedState = null;
    }

    @Override
    public void write(VariableWidthBlockBuilder blockBuilder)
    {
        if (size() == 0) {
            blockBuilder.appendNull();
            return;
        }
        out.reset();
        writeNotGrouped(out);
        blockBuilder.writeEntry(out.slice());
    }

    private void writeNotGrouped(SliceOutput out)
    {
        int entryCount = toIntExact(size());
        int emittedCount = 0;
        for (byte[] records : closedRecordGroups) {
            int recordOffset = 0;
            for (int recordIndex = 0; recordIndex < RECORDS_PER_GROUP; recordIndex++) {
                if (!writeEntry(records, recordOffset, out, entryCount, emittedCount)) {
                    return;
                }
                emittedCount++;
                recordOffset += recordSize;
            }
        }
        int recordsInOpenGroup = entryCount & RECORDS_PER_GROUP_MASK;
        int recordOffset = 0;
        for (int recordIndex = 0; recordIndex < recordsInOpenGroup; recordIndex++) {
            if (!writeEntry(openRecordGroup, recordOffset, out, entryCount, emittedCount)) {
                return;
            }
            emittedCount++;
            recordOffset += recordSize;
        }
    }

    @Override
    public AccumulatorState copy()
    {
        return new SingleListaggAggregationState(this);
    }

    void setTempSerializedState(SqlRow tempSerializedState)
    {
        this.tempSerializedState = tempSerializedState;
    }

    SqlRow removeTempSerializedState()
    {
        SqlRow sqlRow = tempSerializedState;
        checkState(sqlRow != null, "tempDeserializeBlock is null");
        tempSerializedState = null;
        return sqlRow;
    }
}
