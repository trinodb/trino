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
package io.trino.filesystem.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.memory.MemoryFileSystemCacheConfig.DEFAULT_CACHE_SIZE;
import static java.util.concurrent.TimeUnit.HOURS;

public class TestMemoryFileSystemCacheConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MemoryFileSystemCacheConfig.class)
                .setMaxSize(DEFAULT_CACHE_SIZE)
                .setCacheTtl(new Duration(1, HOURS))
                .setMaxContentLength(DataSize.of(8, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.memory-cache.max-size", "10MB")
                .put("fs.memory-cache.max-content-length", "1MB")
                .put("fs.memory-cache.ttl", "8h")
                .buildOrThrow();

        MemoryFileSystemCacheConfig expected = new MemoryFileSystemCacheConfig()
                .setMaxSize(DataSize.of(10, MEGABYTE))
                .setCacheTtl(new Duration(8, HOURS))
                .setMaxContentLength(DataSize.of(1, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
