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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ListaggAggregationStateSerializer
        implements AccumulatorStateSerializer<ListaggAggregationState>
{
    private final Type serializedType;

    public ListaggAggregationStateSerializer()
    {
        this.serializedType = RowType.anonymous(ImmutableList.of(VARCHAR, BOOLEAN, VARCHAR, BOOLEAN, new ArrayType(VARCHAR)));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ListaggAggregationState state, BlockBuilder out)
    {
        state.serialize((RowBlockBuilder) out);
    }

    @Override
    public void deserialize(Block block, int index, ListaggAggregationState state)
    {
        SqlRow sqlRow = (SqlRow) serializedType.getObject(block, index);
        ((SingleListaggAggregationState) state).setTempSerializedState(sqlRow);
    }
}
