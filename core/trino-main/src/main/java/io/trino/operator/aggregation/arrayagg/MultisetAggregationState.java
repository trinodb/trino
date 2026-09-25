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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.Block;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

/// Holds a single running multiset value, stored as the block of its elements (or `null` when
/// no input has been seen). Used by the INTERSECTION aggregate to fold inputs into one multiset.
@AccumulatorStateMetadata(
        stateFactoryClass = MultisetAggregationStateFactory.class,
        stateSerializerClass = MultisetAggregationStateSerializer.class,
        typeParameters = "E",
        serializedType = "multiset(E)")
public interface MultisetAggregationState
        extends AccumulatorState
{
    Block get();

    void set(Block value);
}
