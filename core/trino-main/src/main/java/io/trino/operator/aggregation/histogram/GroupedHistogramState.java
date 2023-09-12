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

package io.trino.operator.aggregation.histogram;

import io.trino.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;

public class GroupedHistogramState
        extends AbstractGroupedAccumulatorState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = instanceSize(GroupedHistogramState.class);

    private final TypedHistogram histogram;

    public GroupedHistogramState(
            Type keyType,
            MethodHandle readFlat,
            MethodHandle writeFlat,
            MethodHandle hashFlat,
            MethodHandle distinctFlatBlock,
            MethodHandle hashBlock)
    {
        this.histogram = new TypedHistogram(keyType, readFlat, writeFlat, hashFlat, distinctFlatBlock, hashBlock, true);
    }

    @Override
    public void ensureCapacity(long size)
    {
        histogram.setMaxGroupId(toIntExact(size));
    }

    @Override
    public void add(Block block, int position, long count)
    {
        histogram.add(toIntExact(getGroupId()), block, position, count);
    }

    @Override
    public void writeAll(MapBlockBuilder out)
    {
        histogram.serialize(toIntExact(getGroupId()), out);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + histogram.getEstimatedSize();
    }
}
