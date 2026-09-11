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
import io.trino.spi.function.AccumulatorState;

/**
 * Common state interface shared by the union and intersection aggregations. Concrete state metadata
 * (serializer and factory) is declared on the {@link UnionState} and {@link IntersectionState}
 * sub-interfaces, because union and intersection accumulate and serialize differently.
 */
public interface SketchState
        extends AccumulatorState
{
    Slice getSketch();

    long getSeed();

    void setSeed(long value);

    void addSketch(Slice value);

    /**
     * Overwrites the accumulated sketch, discarding any prior state. Used by the state deserializers
     * to avoid combining fresh data with stale content left in a scratch state that Trino reuses
     * across aggregation positions.
     */
    void setSketch(Slice value);

    void merge(SketchState other);
}
