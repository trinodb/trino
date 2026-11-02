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

import java.util.Set;

public interface BlobCacheManager
{
    /**
     * Whether the caches this manager creates provide the given capability. The engine only
     * routes a request to this manager when it has every capability the caller requires; the
     * answer may reflect the manager's actual configuration.
     */
    boolean hasCapability(CacheCapability capability);

    /**
     * Creates the cache used by the given catalog, providing the given capabilities.
     * Implementations offering multiple service levels use the capabilities to configure the
     * returned cache; the engine only passes capabilities this manager
     * {@link #hasCapability has}. Every {@link CacheKey} passed to the returned
     * {@link BlobCache} has the catalog name as its first component, so all of a catalog's
     * entries can be invalidated with the {@code [catalog]} key prefix.
     */
    BlobCache create(CatalogName catalog, Set<CacheCapability> capabilities);

    /**
     * Releases the cache created for the given catalog and requests removal of its cached
     * entries. Removal is best effort: implementations that cannot enumerate a catalog's
     * entries may leave cached data to age out through eviction. Entries left behind are
     * unreachable through the dropped cache and, since every key carries the catalog name,
     * cannot alias entries of other catalogs.
     */
    void drop(CatalogName catalog);

    void shutdown();
}
