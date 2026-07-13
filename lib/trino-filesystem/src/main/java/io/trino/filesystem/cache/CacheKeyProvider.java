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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.cache.CacheKey;

import java.io.IOException;
import java.util.Optional;

public interface CacheKeyProvider
{
    /**
     * Get the cache key of a TrinoInputFile. Returns Optional.empty() if the file is not cacheable.
     */
    Optional<CacheKey> getCacheKey(TrinoInputFile inputFile)
            throws IOException;

    /**
     * Prefix of every key produced by {@link #getCacheKey} for the file at the given location,
     * used to invalidate all cached entries (all versions and variants) of that file without
     * accessing the file system. Returns Optional.empty() if entries for the location cannot be
     * identified by a key prefix.
     */
    default Optional<CacheKey> getCacheKeyPrefix(Location location)
    {
        return Optional.of(CacheKey.of(location.toString()));
    }
}
