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
package io.trino.spi;

import io.trino.spi.block.Block;

/**
 * An efficient append-only map from unique page rows to non-negative sequential integer group IDs.
 * Group IDs are assigned in insertion order and can be used as indices into caller-managed arrays.
 * <p>
 * The column schema is fixed at creation time via {@link BlocksHashFactory}.
 * The {@code Block[]} arrays passed to all operations must match the factory types in count and order.
 * <p>
 * Not thread-safe.
 */
public interface BlocksHash
{
    /**
     * Inserts the row at the given position.
     * Returns the group ID, assigning a new one if the row is not already present.
     */
    int putIfAbsent(Block[] blocks, int position);

    /**
     * Returns the group ID for the row at the given position or -1 if not present.
     */
    int getIfPresent(Block[] blocks, int position);

    long getEstimatedSize();
}
