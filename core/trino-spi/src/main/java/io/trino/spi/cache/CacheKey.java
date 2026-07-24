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

import java.util.ArrayList;
import java.util.List;

/**
 * Hierarchical cache entry identifier composed of opaque components ordered from most general
 * to most specific (for example {@code [catalog, file identity, version]}). Components carry no
 * semantics for the cache: producers choose them so that entries that must be invalidated
 * together share a leading run of components, and {@link BlobCache#tryInvalidate(CacheKey)} removes
 * every entry whose key {@link #startsWith starts with} the given components.
 */
public record CacheKey(List<String> components)
{
    public CacheKey
    {
        components = List.copyOf(components);
        if (components.isEmpty()) {
            throw new IllegalArgumentException("components is empty");
        }
    }

    public static CacheKey of(String... components)
    {
        return new CacheKey(List.of(components));
    }

    /**
     * Returns a new key with the given component appended at the tail, identifying a variant
     * of this entry that is invalidated together with it by any prefix covering this key.
     */
    public CacheKey append(String component)
    {
        List<String> extended = new ArrayList<>(components.size() + 1);
        extended.addAll(components);
        extended.add(component);
        return new CacheKey(extended);
    }

    /**
     * Returns a new key with the given key's components appended at the tail, scoping it
     * under this key.
     */
    public CacheKey append(CacheKey key)
    {
        List<String> extended = new ArrayList<>(components.size() + key.components.size());
        extended.addAll(components);
        extended.addAll(key.components);
        return new CacheKey(extended);
    }

    public boolean startsWith(CacheKey prefix)
    {
        if (prefix.components.size() > components.size()) {
            return false;
        }
        return components.subList(0, prefix.components.size()).equals(prefix.components);
    }

    @Override
    public String toString()
    {
        // Escape the separator so the representation is injective: ["a|b", "c"] and
        // ["a", "b|c"] must not render identically, since implementations may use this
        // string as a storage identifier
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < components.size(); i++) {
            if (i > 0) {
                builder.append('|');
            }
            builder.append(components.get(i)
                    .replace("\\", "\\\\")
                    .replace("|", "\\|"));
        }
        return builder.toString();
    }
}
