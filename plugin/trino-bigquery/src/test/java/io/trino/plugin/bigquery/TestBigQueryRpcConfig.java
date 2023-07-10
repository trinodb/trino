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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestBigQueryRpcConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BigQueryRpcConfig.class)
                .setRpcInitialChannelCount(1)
                .setMinRpcPerChannel(0)
                .setMaxRpcPerChannel(Integer.MAX_VALUE)
                .setRpcMinChannelCount(1)
                .setRpcMaxChannelCount(1)
                .setRetries(0)
                .setTimeout(Duration.valueOf("0s"))
                .setRetryDelay(Duration.valueOf("0s"))
                .setRetryMultiplier(1.0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.channel-pool.initial-size", "11")
                .put("bigquery.channel-pool.min-size", "12")
                .put("bigquery.channel-pool.max-size", "13")
                .put("bigquery.channel-pool.min-rpc-per-channel", "14")
                .put("bigquery.channel-pool.max-rpc-per-channel", "15")
                .put("bigquery.rpc-retries", "5")
                .put("bigquery.rpc-timeout", "17s")
                .put("bigquery.rpc-retry-delay", "10s")
                .put("bigquery.rpc-retry-delay-multiplier", "1.2")
                .buildOrThrow();

        BigQueryRpcConfig expected = new BigQueryRpcConfig()
                .setRpcInitialChannelCount(11)
                .setRpcMinChannelCount(12)
                .setRpcMaxChannelCount(13)
                .setMinRpcPerChannel(14)
                .setMaxRpcPerChannel(15)
                .setRetries(5)
                .setTimeout(Duration.valueOf("17s"))
                .setRetryDelay(Duration.valueOf("10s"))
                .setRetryMultiplier(1.2);

        assertFullMapping(properties, expected);
    }
}
