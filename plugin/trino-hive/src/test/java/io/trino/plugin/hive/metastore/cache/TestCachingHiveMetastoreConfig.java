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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreConfig.DEFAULT_STATS_CACHE_TTL;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CachingHiveMetastoreConfig.class)
                .setMetastoreCacheTtl(new Duration(0, SECONDS))
                .setStatsCacheTtl(new Duration(5, MINUTES))
                .setMetastoreRefreshInterval(null)
                .setMetastoreCacheMaximumSize(20000)
                .setMaxMetastoreRefreshThreads(10)
                .setPartitionCacheEnabled(true)
                .setCacheMissing(true)
                .setCacheMissingPartitions(true)
                .setCacheMissingStats(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore-stats-cache-ttl", "10m")
                .put("hive.metastore-refresh-interval", "30m")
                .put("hive.metastore-cache-maximum-size", "5000")
                .put("hive.metastore-refresh-max-threads", "2500")
                .put("hive.metastore-cache.cache-partitions", "false")
                .put("hive.metastore-cache.cache-missing", "false")
                .put("hive.metastore-cache.cache-missing-partitions", "false")
                .put("hive.metastore-cache.cache-missing-stats", "false")
                .buildOrThrow();

        CachingHiveMetastoreConfig expected = new CachingHiveMetastoreConfig()
                .setMetastoreCacheTtl(new Duration(2, HOURS))
                .setStatsCacheTtl(new Duration(10, MINUTES))
                .setMetastoreRefreshInterval(new Duration(30, MINUTES))
                .setMetastoreCacheMaximumSize(5000)
                .setMaxMetastoreRefreshThreads(2500)
                .setPartitionCacheEnabled(false)
                .setCacheMissing(false)
                .setCacheMissingPartitions(false)
                .setCacheMissingStats(false);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testStatsCacheTtl()
    {
        // enabled by default
        assertThat(new CachingHiveMetastoreConfig().getStatsCacheTtl()).isEqualTo(DEFAULT_STATS_CACHE_TTL);

        // takes higher of the DEFAULT_STATS_CACHE_TTL or metastoreTtl if not set explicitly
        assertThat(new CachingHiveMetastoreConfig()
                .setMetastoreCacheTtl(new Duration(1, SECONDS))
                .getStatsCacheTtl()).isEqualTo(DEFAULT_STATS_CACHE_TTL);
        assertThat(new CachingHiveMetastoreConfig()
                .setMetastoreCacheTtl(new Duration(1111, DAYS))
                .getStatsCacheTtl()).isEqualTo(new Duration(1111, DAYS));

        // explicit configuration is honored
        assertThat(new CachingHiveMetastoreConfig()
                .setStatsCacheTtl(new Duration(135, MILLISECONDS))
                .setMetastoreCacheTtl(new Duration(1111, DAYS))
                .getStatsCacheTtl()).isEqualTo(new Duration(135, MILLISECONDS));
    }
}
