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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.util.Collection;

public interface TrinoFileSystemCache
{
    /**
     * Get the TrinoInput of the TrinoInputFile, potentially using or updating the data cached at key.
     */
    TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException;

    /**
     * Get the TrinoInputStream of the TrinoInputFile, potentially using or updating the data cached at key.
     */
    TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException;

    /**
     * Get the length of the TrinoInputFile, potentially using or updating the data cached at key.
     */
    long cacheLength(TrinoInputFile delegate, String key)
            throws IOException;

    /**
     * Give a hint to the cache that the cache entry for location should be expired.
     */
    void expire(Location location)
            throws IOException;

    /**
     * Give a hint to the cache that the cache entry for locations should be expired.
     */
    void expire(Collection<Location> locations)
            throws IOException;
}
