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
package io.prestosql.operator.aggregation;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class StringApproximateMostFrequentStateSerializer
        implements AccumulatorStateSerializer<VarcharApproximateMostFrequent.State>
{
    public static void serializeBucket(Slice key, Long count, DynamicSliceOutput output)
    {
        output.appendInt(key.length());
        output.appendBytes(key);
        output.appendLong(count);
    }

    public static void deserializeBucket(SliceInput input, ApproximateMostFrequentHistogram<Slice> histogram)
    {
        int keySize = input.readInt();
        Slice key = input.readSlice(keySize);
        long count = input.readLong();
        histogram.add(key, count);
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(VarcharApproximateMostFrequent.State state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, state.get().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, VarcharApproximateMostFrequent.State state)
    {
        state.set(new ApproximateMostFrequentHistogram<Slice>(
                VARBINARY.getSlice(block, index),
                StringApproximateMostFrequentStateSerializer::serializeBucket,
                StringApproximateMostFrequentStateSerializer::deserializeBucket));
    }
}
