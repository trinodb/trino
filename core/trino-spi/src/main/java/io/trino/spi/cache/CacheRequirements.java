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

import java.util.Set;

/**
 * What a consumer requires from a blob cache: the usage name declaring who is caching what,
 * for example {@code "filesystem.data"}, and the capabilities the serving cache manager must
 * provide. The usage name becomes a cache key component, so it must be unique among the cache
 * consumers of a catalog, and entries of one usage can be invalidated together. The engine
 * enforces this: repeating an identical request is idempotent, but reusing a usage name with
 * different requirements fails.
 */
public record CacheRequirements(String usageName, Set<CacheCapability> capabilities)
{
    public CacheRequirements
    {
        if (usageName == null || usageName.isEmpty()) {
            throw new IllegalArgumentException("usageName is null or empty");
        }
        capabilities = Set.copyOf(capabilities);
    }
}
