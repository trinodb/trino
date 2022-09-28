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
import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateFactoryClass = ListaggAggregationStateFactory.class,
        stateSerializerClass = ListaggAggregationStateSerializer.class)
public interface ListaggAggregationState
        extends AccumulatorState
{
    void setSeparator(Slice separator);

    Slice getSeparator();

    void setOverflowFiller(Slice overflowFiller);

    Slice getOverflowFiller();

    void setOverflowError(boolean overflowError);

    boolean isOverflowError();

    void setShowOverflowEntryCount(boolean showOverflowEntryCount);

    boolean showOverflowEntryCount();

    void add(Block block, int position);

    void forEach(ListaggAggregationStateConsumer consumer);

    boolean isEmpty();

    int getEntryCount();

    default void merge(ListaggAggregationState otherState)
    {
        otherState.forEach((block, position) -> {
            add(block, position);
            return true;
        });
    }

    default void reset()
    {
        throw new UnsupportedOperationException();
    }
}
