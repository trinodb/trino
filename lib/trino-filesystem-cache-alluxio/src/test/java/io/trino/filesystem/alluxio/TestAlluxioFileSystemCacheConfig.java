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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.filesystem.alluxio.AlluxioConfigurationFactory.totalSpace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAlluxioFileSystemCacheConfig
{
    @Test
    public void testInvalidConfiguration()
    {
        assertThatThrownBy(() ->
                AlluxioConfigurationFactory.create(
                        new AlluxioFileSystemCacheConfig()
                                .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                                .setMaxCacheDiskUsagePercentages(ImmutableList.of(0))
                                .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either fs.cache.max-sizes or fs.cache.max-disk-usage-percentages must be specified");
        assertThatThrownBy(() ->
                AlluxioConfigurationFactory.create(
                        new AlluxioFileSystemCacheConfig()
                                .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                                .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fs.cache.directories and fs.cache.max-sizes must have the same size");
        assertThatThrownBy(() ->
                AlluxioConfigurationFactory.create(
                        new AlluxioFileSystemCacheConfig()
                                .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                                .setMaxCacheDiskUsagePercentages(ImmutableList.of(0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fs.cache.directories and fs.cache.max-disk-usage-percentages must have the same size");
    }

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AlluxioFileSystemCacheConfig.class)
                .setCacheDirectories(ImmutableList.of())
                .setCachePageSize(DataSize.valueOf("1MB"))
                .setMaxCacheSizes(ImmutableList.of())
                .setMaxCacheDiskUsagePercentages(ImmutableList.of())
                .setCacheTTL(Duration.valueOf("7d")));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path cacheDirectory = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.cache.directories", cacheDirectory.toString())
                .put("fs.cache.page-size", "7MB")
                .put("fs.cache.max-sizes", "1GB")
                .put("fs.cache.max-disk-usage-percentages", "50")
                .put("fs.cache.ttl", "1d")
                .buildOrThrow();

        AlluxioFileSystemCacheConfig expected = new AlluxioFileSystemCacheConfig()
                .setCacheDirectories(ImmutableList.of(cacheDirectory.toString()))
                .setCachePageSize(DataSize.valueOf("7MB"))
                .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1GB")))
                .setMaxCacheDiskUsagePercentages(ImmutableList.of(50))
                .setCacheTTL(Duration.valueOf("1d"));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testTotalSpaceCalculation()
            throws IOException
    {
        Path cacheDirectory = Files.createTempFile(null, null);

        assertThat(totalSpace(cacheDirectory)).isEqualTo(cacheDirectory.toFile().getTotalSpace());
        assertThat(totalSpace(cacheDirectory.resolve(Path.of("does-not-exist")))).isEqualTo(cacheDirectory.toFile().getTotalSpace());
    }
}
