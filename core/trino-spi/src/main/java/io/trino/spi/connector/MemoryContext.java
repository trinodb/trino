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
package io.trino.spi.connector;

import static io.trino.spi.connector.Preconditions.checkArgument;

/// Facade for reporting memory usage.
///
/// Implementations are not required to be thread-safe.
public interface MemoryContext
{
    MemoryContext NO_LIMIT = currentBytes -> checkArgument(currentBytes >= 0, "currentBytes is negative: %s", currentBytes);

    /// Reports current memory usage, in bytes.
    ///
    /// @param currentBytes current memory usage (non-negative)
    /// @throws IllegalArgumentException when `currentBytes < 0`
    /// @throws RuntimeException if the memory usage exceeds a limit
    void setBytes(long currentBytes);
}
