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
package io.trino.plugin.iceberg.aggregation;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.CompactSketch;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class DataSketchStateSerializer
        implements AccumulatorStateSerializer<DataSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(DataSketchState state, BlockBuilder out)
    {
        serializeToVarbinary(state, out);
    }

    public static void serializeToVarbinary(DataSketchState state, BlockBuilder out)
    {
        if (state.getUpdateSketch() == null && state.getCompactSketch() == null) {
            out.appendNull();
        }
        else {
            checkArgument(state.getUpdateSketch() == null || state.getCompactSketch() == null, "A state must not have both transient accumulator and combined form set");
            CompactSketch compactSketch = Optional.ofNullable(state.getCompactSketch())
                    .orElseGet(() -> state.getUpdateSketch().compact());
            Slice slice = Slices.wrappedBuffer(compactSketch.toByteArray());
            VARBINARY.writeSlice(out, slice);
        }
    }

    @Override
    public void deserialize(Block block, int index, DataSketchState state)
    {
        if (!block.isNull(index)) {
            state.setCompactSketch(deserialize(block, index));
        }
    }

    public static CompactSketch deserialize(Block block, int index)
    {
        checkArgument(!block.isNull(index), "Value is null");
        Slice slice = VARBINARY.getSlice(block, index);
        return CompactSketch.heapify(WritableMemory.writableWrap(slice.getBytes()));
    }
}
