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
package io.trino.spi.cache;

import io.trino.spi.catalog.CatalogName;
import io.trino.spi.filesystem.cache.ConnectorFileSystemCache;

import java.time.Duration;
import java.util.Collection;

public interface FileSystemCacheManager
{
    ConnectorFileSystemCache createFileSystemCache(CatalogName catalog, Duration ttl);

    void invalidate(CatalogName catalog);

    void drop(CatalogName catalog);

    Collection<CacheInfo> getCaches();

    void shutdown();
}
