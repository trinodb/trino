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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheManagerFactory;
import io.trino.spi.cache.MemoryAllocator;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestMemoryCachePlugin
{
    @Test
    public void testCreateCacheManager()
    {
        Plugin plugin = new MemoryCachePlugin();
        CacheManagerFactory factory = getOnlyElement(plugin.getCacheManagerFactories());
        factory.create(
                ImmutableMap.of(),
                new CacheManagerContext()
                {
                    @Override
                    public MemoryAllocator revocableMemoryAllocator()
                    {
                        return null;
                    }

                    @Override
                    public BlockEncodingSerde blockEncodingSerde()
                    {
                        return new TestingBlockEncodingSerde();
                    }
                });
    }
}
