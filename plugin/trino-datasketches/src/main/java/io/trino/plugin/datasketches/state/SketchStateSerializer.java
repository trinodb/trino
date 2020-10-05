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
package io.trino.plugin.datasketches.state;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class SketchStateSerializer
        implements AccumulatorStateSerializer<SketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(SketchState state, BlockBuilder out)
    {
        if (state.getSketch() == null) {
            out.appendNull();
        }
        else {
            Slice slice = state.getSketch();

            SliceOutput sliceOutput = Slices.allocate(SIZE_OF_LONG + SIZE_OF_INT + SIZE_OF_INT + slice.length()).getOutput();
            sliceOutput.appendInt(state.getNominalEntries());
            sliceOutput.appendLong(state.getSeed());

            sliceOutput.appendInt(slice.length());
            sliceOutput.appendBytes(slice);

            VARBINARY.writeSlice(out, sliceOutput.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, SketchState state)
    {
        Slice slice = VARBINARY.getSlice(block, index);
        SliceInput input = slice.getInput();

        state.setNominalEntries(input.readInt());
        state.setSeed(input.readLong());

        int sketchLength = input.readInt();
        state.setSketch(input.readSlice(sketchLength));
    }
}
