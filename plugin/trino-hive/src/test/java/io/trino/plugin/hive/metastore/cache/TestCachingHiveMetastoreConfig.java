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
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCachingHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CachingHiveMetastoreConfig.class)
                .setMetastoreCacheTtl(new Duration(0, TimeUnit.SECONDS))
                .setStatsCacheTtl(new Duration(5, TimeUnit.MINUTES))
                .setMetastoreRefreshInterval(null)
                .setMetastoreCacheMaximumSize(10000)
                .setMaxMetastoreRefreshThreads(10)
                .setPartitionCacheEnabled(true));
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
                .buildOrThrow();

        CachingHiveMetastoreConfig expected = new CachingHiveMetastoreConfig()
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setStatsCacheTtl(new Duration(10, TimeUnit.MINUTES))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMetastoreCacheMaximumSize(5000)
                .setMaxMetastoreRefreshThreads(2500)
                .setPartitionCacheEnabled(false);

        assertFullMapping(properties, expected);
    }
}
