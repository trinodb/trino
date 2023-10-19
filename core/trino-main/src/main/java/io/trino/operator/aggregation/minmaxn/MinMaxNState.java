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
package io.trino.operator.aggregation.minmaxn;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;

public interface MinMaxNState
        extends AccumulatorState
{
    /**
     * Initialize the state if not already initialized.  Only the first call is processed and
     * all subsequent calls are ignored.
     */
    void initialize(long n);

    /**
     * Adds the value to this state.
     */
    void add(Block block, int position);

    /**
     * Merge with the specified state.
     * The supplied state should not be used after this method is called, because
     * the internal details of the state may be reused in this state.
     */
    void merge(MinMaxNState other);

    /**
     * Writes all values to the supplied block builder as an array entry.
     */
    void writeAllSorted(BlockBuilder out);

    /**
     * Write this state to the specified block builder.
     */
    void serialize(BlockBuilder out);
}
