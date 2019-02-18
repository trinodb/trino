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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestThriftHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftHiveMetastoreConfig.class)
                .setMaxRetries(9)
                .setBackoffScaleFactor(2.0)
                .setMinBackoffDelay(new Duration(1, SECONDS))
                .setMaxBackoffDelay(new Duration(1, SECONDS))
                .setMaxRetryTime(new Duration(30, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.thrift.client.max-retries", "15")
                .put("hive.metastore.thrift.client.backoff-scale-factor", "3.0")
                .put("hive.metastore.thrift.client.min-backoff-delay", "2s")
                .put("hive.metastore.thrift.client.max-backoff-delay", "4s")
                .put("hive.metastore.thrift.client.max-retry-time", "60s")
                .build();

        ThriftHiveMetastoreConfig expected = new ThriftHiveMetastoreConfig()
                .setMaxRetries(15)
                .setBackoffScaleFactor(3.0)
                .setMinBackoffDelay(new Duration(2, SECONDS))
                .setMaxBackoffDelay(new Duration(4, SECONDS))
                .setMaxRetryTime(new Duration(60, SECONDS));

        assertFullMapping(properties, expected);
    }
}
