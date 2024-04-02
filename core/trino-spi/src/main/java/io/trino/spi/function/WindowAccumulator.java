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
package io.trino.spi.function;

import io.trino.spi.block.BlockBuilder;

public interface WindowAccumulator
{
    /**
     * @return the estimated size of the accumulator in bytes
     */
    long getEstimatedSize();

    /**
     * @return a copy of the accumulator
     */
    WindowAccumulator copy();

    /**
     * Add the specified positions to the accumulator.
     *
     * @param startPosition the first position to add
     * @param endPosition the last position to add (inclusive)
     */
    void addInput(WindowIndex index, int startPosition, int endPosition);

    /**
     * Remove the specified positions from the accumulator.
     *
     * @param startPosition the first position to remove
     * @param endPosition the last position to remove (inclusive)
     * @return false if input could not be removed and accumulator should be recreated
     */
    default boolean removeInput(WindowIndex index, int startPosition, int endPosition)
    {
        return false;
    }

    /**
     * Get the value of the accumulator.
     */
    void output(BlockBuilder blockBuilder);
}
