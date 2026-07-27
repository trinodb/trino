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
package io.trino.filesystem.cache.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

final class TestNativeFileSystemCacheConfig
{
    @Test
    void testInvalidConfiguration()
    {
        assertFailsValidation(
                new NativeFileSystemCacheConfig()
                        .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B"))),
                "cacheDirectoriesConfigured",
                "fs.cache.directories must be specified",
                AssertTrue.class);
        assertFailsValidation(
                new NativeFileSystemCacheConfig()
                        .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                        .setMaxCacheDiskUsagePercentages(ImmutableList.of(0))
                        .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B"))),
                "cacheMaxSizeConfigured",
                "Either fs.cache.max-sizes or fs.cache.max-disk-usage-percentages must be specified",
                AssertTrue.class);
        assertFailsValidation(
                new NativeFileSystemCacheConfig()
                        .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                        .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B"))),
                "cacheMaxSizeCountValid",
                "fs.cache.directories and configured cache size limits must have the same size",
                AssertTrue.class);
        assertFailsValidation(
                new NativeFileSystemCacheConfig()
                        .setCacheDirectories(ImmutableList.of("/cache1", "/cache2"))
                        .setMaxCacheDiskUsagePercentages(ImmutableList.of(0)),
                "cacheMaxSizeCountValid",
                "fs.cache.directories and configured cache size limits must have the same size",
                AssertTrue.class);
        assertFailsValidation(
                new NativeFileSystemCacheConfig()
                        .setCacheDirectories(ImmutableList.of("/cache"))
                        .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1B")))
                        .setAccessHistoryDuration(Duration.valueOf("1m"))
                        .setAccessBucketDuration(Duration.valueOf("2m")),
                "accessDurationValid",
                "fs.cache.access-history-duration must be greater than or equal to fs.cache.access-bucket-duration",
                AssertTrue.class);
    }

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NativeFileSystemCacheConfig.class)
                .setCacheDirectories(ImmutableList.of())
                .setCachePageSize(DataSize.valueOf("1MB"))
                .setMaxCacheSizes(ImmutableList.of())
                .setMaxCacheDiskUsagePercentages(ImmutableList.of())
                .setCacheTTL(Duration.valueOf("7d"))
                .setMaxCacheFilesPerDirectory(10_000_000)
                .setAccessHistoryDuration(Duration.valueOf("24h"))
                .setAccessBucketDuration(Duration.valueOf("10m"))
                .setAccessTrackingMemory(DataSize.valueOf("256MB")));
    }

    @Test
    void testExplicitPropertyMappings(@TempDir Path cacheDirectory)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.cache.directories", cacheDirectory.toString())
                .put("fs.cache.page-size", "7MB")
                .put("fs.cache.max-sizes", "1GB")
                .put("fs.cache.ttl", "1d")
                .put("fs.cache.max-files-per-directory", "1000")
                .put("fs.cache.access-history-duration", "1h")
                .put("fs.cache.access-bucket-duration", "5m")
                .put("fs.cache.access-tracking-memory", "16MB")
                .buildOrThrow();

        NativeFileSystemCacheConfig expected = new NativeFileSystemCacheConfig()
                .setCacheDirectories(ImmutableList.of(cacheDirectory.toString()))
                .setCachePageSize(DataSize.valueOf("7MB"))
                .setMaxCacheSizes(ImmutableList.of(DataSize.valueOf("1GB")))
                .setCacheTTL(Duration.valueOf("1d"))
                .setMaxCacheFilesPerDirectory(1000)
                .setAccessHistoryDuration(Duration.valueOf("1h"))
                .setAccessBucketDuration(Duration.valueOf("5m"))
                .setAccessTrackingMemory(DataSize.valueOf("16MB"));

        assertFullMapping(properties, expected, ImmutableSet.of("fs.cache.max-disk-usage-percentages"));
    }

    @Test
    void testTotalSpaceCalculation()
            throws IOException
    {
        Path cacheDirectory = Files.createTempDirectory(null);

        assertThat(NativeFileSystemCache.totalSpace(cacheDirectory)).isEqualTo(Files.getFileStore(cacheDirectory).getTotalSpace());
        assertThat(NativeFileSystemCache.totalSpace(cacheDirectory.resolve(Path.of("does-not-exist")))).isEqualTo(Files.getFileStore(cacheDirectory).getTotalSpace());
    }
}
