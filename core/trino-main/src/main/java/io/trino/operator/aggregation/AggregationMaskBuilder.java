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
package io.trino.operator.aggregation;

import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.Optional;

public interface AggregationMaskBuilder
{
    /**
     * Create an AggregationMask that only selects positions that pass the specified
     * mask block, and do not have null for non-null arguments. The returned mask
     * can be further modified if desired, but it should not be used after the next
     * call to this method. Internally implementations are allowed to reuse position
     * arrays across multiple calls.
     */
    AggregationMask buildAggregationMask(Page arguments, Optional<Block> optionalMaskBlock);
}
