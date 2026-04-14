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
package io.trino.spi.filesystem.cache;

import io.trino.spi.filesystem.Location;
import io.trino.spi.filesystem.TrinoInput;
import io.trino.spi.filesystem.TrinoInputFile;
import io.trino.spi.filesystem.TrinoInputStream;

import java.io.IOException;
import java.util.Collection;

public interface ConnectorFileSystemCache
{
    TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException;

    TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException;

    long cacheLength(TrinoInputFile delegate, String key)
            throws IOException;

    void expire(Location location)
            throws IOException;

    void expire(Collection<Location> locations)
            throws IOException;
}
