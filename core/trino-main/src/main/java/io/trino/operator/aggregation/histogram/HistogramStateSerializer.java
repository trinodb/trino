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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import static io.trino.operator.aggregation.histogram.HistogramStateFactory.EXPECTED_SIZE_FOR_HASHING;

public class HistogramStateSerializer
        implements AccumulatorStateSerializer<HistogramState>
{
    private final Type serializedType;

    public HistogramStateSerializer(@TypeParameter("map(T, BIGINT)") Type serializedType)
    {
        this.serializedType = serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(HistogramState state, BlockBuilder out)
    {
        state.get().serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, HistogramState state)
    {
        state.deserialize((Block) serializedType.getObject(block, index), EXPECTED_SIZE_FOR_HASHING);
    }
}
