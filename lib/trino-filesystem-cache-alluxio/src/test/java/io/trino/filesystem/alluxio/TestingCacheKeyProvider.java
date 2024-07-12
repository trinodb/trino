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
package io.trino.filesystem.alluxio;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheKeyProvider;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class TestingCacheKeyProvider
        implements CacheKeyProvider
{
    private final AtomicInteger cacheVersion = new AtomicInteger(0);

    @Override
    public Optional<String> getCacheKey(TrinoInputFile inputFile)
    {
        return Optional.of(testingCacheKeyForLocation(inputFile.location(), cacheVersion.get()));
    }

    public static String testingCacheKeyForLocation(Location location, int generation)
    {
        return location.toString() + "-v" + generation;
    }

    public void increaseCacheVersion()
    {
        cacheVersion.incrementAndGet();
    }

    public int currentCacheVersion()
    {
        return cacheVersion.get();
    }
}
