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

import io.airlift.slice.Slice;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateFactoryClass = ListaggAggregationStateFactory.class,
        stateSerializerClass = ListaggAggregationStateSerializer.class)
public interface ListaggAggregationState
        extends AccumulatorState
{
    void initialize(Slice separator, boolean overflowError, Slice overflowFiller, boolean showOverflowEntryCount);

    void add(ValueBlock block, int position);

    void serialize(RowBlockBuilder out);

    void merge(ListaggAggregationState otherState);

    void write(VariableWidthBlockBuilder blockBuilder);
}
