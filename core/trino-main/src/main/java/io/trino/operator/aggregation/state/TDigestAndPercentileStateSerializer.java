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
package io.trino.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.TDigest;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class TDigestAndPercentileStateSerializer
        implements AccumulatorStateSerializer<TDigestAndPercentileState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(TDigestAndPercentileState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            Slice serialized = state.getDigest().serialize();

            SliceOutput output = Slices.allocate(SIZE_OF_DOUBLE + SIZE_OF_INT + serialized.length()).getOutput();
            output.appendDouble(state.getPercentile());
            output.appendInt(serialized.length());
            output.appendBytes(serialized);

            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, TDigestAndPercentileState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read percentile
        state.setPercentile(input.readDouble());

        // read digest
        int length = input.readInt();
        TDigest digest = TDigest.deserialize(input.readSlice(length));
        state.setDigest(digest);
        state.addMemoryUsage(state.getDigest().estimatedInMemorySizeInBytes());
    }
}
