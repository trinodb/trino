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
package io.trino.filesystem.cache;

import java.util.Optional;

/**
 * Produces a stable affinity key for a split, used by the engine to consistently route
 * splits reading the same content to the same worker(s). A non-empty key opts the split
 * into cache-aware scheduling via the engine's consistent-hash ring; an empty key leaves
 * scheduling to the split's own addresses.
 */
public interface SplitAffinityProvider
{
    Optional<String> getKey(String path, long offset, long length);
}
