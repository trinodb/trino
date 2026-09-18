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

import com.google.common.base.Splitter;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.cache.CacheKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
     * used to invalidate all cached entries (all versions and variants) of that file, and of
     * every file beneath it when the location is a directory, without accessing the file system.
     * Returns Optional.empty() if entries for the location cannot be identified by a key prefix.
     */
    default Optional<CacheKey> getCacheKeyPrefix(Location location)
    {
        return Optional.of(locationKey(location));
    }

    /**
     * Splits a location into cache key components: a root component holding the scheme and
     * authority, followed by one component per path segment. A file's key therefore
     * {@link CacheKey#startsWith starts with} the key of any directory containing it, so a
     * directory's entries can be invalidated with the directory's key as a prefix.
     * <p>
     * Interior empty segments are kept, since {@code a//b} and {@code a/b} are distinct objects
     * on object stores and must not collide. Only trailing empty segments are dropped, so a
     * directory produces the same key whether or not the location carries a trailing slash.
     */
    static CacheKey locationKey(Location location)
    {
        StringBuilder root = new StringBuilder();
        location.scheme().ifPresent(scheme -> root.append(scheme).append("://"));
        location.userInfo().ifPresent(userInfo -> root.append(userInfo).append('@'));
        location.host().ifPresent(root::append);
        location.port().ifPresent(port -> root.append(':').append(port));
        List<String> segments = Splitter.on('/').splitToList(location.path());
        int end = segments.size();
        while (end > 0 && segments.get(end - 1).isEmpty()) {
            end--;
        }
        List<String> components = new ArrayList<>(end + 1);
        components.add(root.toString());
        components.addAll(segments.subList(0, end));
        return new CacheKey(components);
    }
}
