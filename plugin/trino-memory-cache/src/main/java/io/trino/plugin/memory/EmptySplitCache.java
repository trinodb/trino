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
package io.trino.plugin.memory;

import io.trino.spi.cache.CacheManager.SplitCache;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.Optional;

public class EmptySplitCache
        implements SplitCache
{
    public static final EmptySplitCache EMPTY_SPLIT_CACHE = new EmptySplitCache();

    @Override
    public Optional<ConnectorPageSource> loadPages(CacheSplitId splitId)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorPageSink> storePages(CacheSplitId splitId)
    {
        return Optional.empty();
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
