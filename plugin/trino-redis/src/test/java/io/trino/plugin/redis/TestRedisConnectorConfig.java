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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestRedisConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RedisConnectorConfig.class)
                .setNodes("")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/redis/"))
                .setTableDescriptionCacheDuration(new Duration(5, MINUTES))
                .setKeyPrefixSchemaTable(false)
                .setRedisKeyDelimiter(":")
                .setRedisConnectTimeout("2000ms")
                .setRedisDataBaseIndex(0)
                .setRedisUser(null)
                .setRedisPassword(null)
                .setRedisScanCount(100)
                .setRedisMaxKeysPerFetch(100)
                .setHideInternalColumns(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redis.table-description-dir", "/var/lib/redis")
                .put("redis.table-description-cache-ttl", "30s")
                .put("redis.table-names", "table1, table2, table3")
                .put("redis.default-schema", "redis")
                .put("redis.nodes", "localhost:12345,localhost:23456")
                .put("redis.key-delimiter", ",")
                .put("redis.key-prefix-schema-table", "true")
                .put("redis.scan-count", "20")
                .put("redis.max-keys-per-fetch", "10")
                .put("redis.hide-internal-columns", "false")
                .put("redis.connect-timeout", "10s")
                .put("redis.database-index", "5")
                .put("redis.user", "test")
                .put("redis.password", "secret")
                .buildOrThrow();

        RedisConnectorConfig expected = new RedisConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/redis"))
                .setTableDescriptionCacheDuration(new Duration(30, SECONDS))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("redis")
                .setNodes("localhost:12345, localhost:23456")
                .setHideInternalColumns(false)
                .setRedisScanCount(20)
                .setRedisMaxKeysPerFetch(10)
                .setRedisConnectTimeout("10s")
                .setRedisDataBaseIndex(5)
                .setRedisUser("test")
                .setRedisPassword("secret")
                .setRedisKeyDelimiter(",")
                .setKeyPrefixSchemaTable(true);

        assertFullMapping(properties, expected);
    }
}
