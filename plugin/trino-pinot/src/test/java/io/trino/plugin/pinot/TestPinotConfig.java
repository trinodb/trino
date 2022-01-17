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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotConfig.class)
                        .setControllerUrls("")
                        .setIdleTimeout(new Duration(5, TimeUnit.MINUTES))
                        .setMaxBacklogPerServer(30)
                        .setMaxConnectionsPerServer(30)
                        .setMinConnectionsPerServer(10)
                        .setThreadPoolSize(30)
                        .setEstimatedSizeInBytesForNonNumericColumn(20)
                        .setConnectionTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setMetadataCacheExpiry(new Duration(2, TimeUnit.MINUTES))
                        .setPreferBrokerQueries(false)
                        .setSegmentsPerSplit(1)
                        .setFetchRetryCount(2)
                        .setForbidSegmentQueries(false)
                        .setNonAggregateLimitForBrokerQueries(25_000)
                        .setRequestTimeout(new Duration(30, TimeUnit.SECONDS))
                        .setMaxRowsPerSplitForSegmentQueries(50_000)
                        .setMaxRowsForBrokerQueries(50_000)
                        .setAggregationPushdownEnabled(true)
                        .setCountDistinctPushdownEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("pinot.controller-urls", "host1:1111,host2:1111")
                .put("pinot.idle-timeout", "1h")
                .put("pinot.max-backlog-per-server", "15")
                .put("pinot.max-connections-per-server", "10")
                .put("pinot.min-connections-per-server", "1")
                .put("pinot.thread-pool-size", "100")
                .put("pinot.estimated-size-in-bytes-for-non-numeric-column", "30")
                .put("pinot.connection-timeout", "8m")
                .put("pinot.metadata-expiry", "1m")
                .put("pinot.prefer-broker-queries", "true")
                .put("pinot.segments-per-split", "2")
                .put("pinot.fetch-retry-count", "3")
                .put("pinot.non-aggregate-limit-for-broker-queries", "10")
                .put("pinot.forbid-segment-queries", "true")
                .put("pinot.request-timeout", "1m")
                .put("pinot.max-rows-per-split-for-segment-queries", "10")
                .put("pinot.max-rows-for-broker-queries", "5000")
                .put("pinot.aggregation-pushdown.enabled", "false")
                .put("pinot.count-distinct-pushdown.enabled", "false")
                .buildOrThrow();

        PinotConfig expected = new PinotConfig()
                .setControllerUrls("host1:1111,host2:1111")
                .setIdleTimeout(new Duration(1, TimeUnit.HOURS))
                .setMaxBacklogPerServer(15)
                .setMaxConnectionsPerServer(10)
                .setMinConnectionsPerServer(1)
                .setThreadPoolSize(100)
                .setEstimatedSizeInBytesForNonNumericColumn(30)
                .setConnectionTimeout(new Duration(8, TimeUnit.MINUTES))
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MINUTES))
                .setPreferBrokerQueries(true)
                .setSegmentsPerSplit(2)
                .setFetchRetryCount(3)
                .setNonAggregateLimitForBrokerQueries(10)
                .setForbidSegmentQueries(true)
                .setRequestTimeout(new Duration(1, TimeUnit.MINUTES))
                .setMaxRowsPerSplitForSegmentQueries(10)
                .setMaxRowsForBrokerQueries(5000)
                .setAggregationPushdownEnabled(false)
                .setCountDistinctPushdownEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidCountDistinctPushdown()
    {
        assertThatThrownBy(() -> new PinotConfig()
                .setAggregationPushdownEnabled(false)
                .setCountDistinctPushdownEnabled(true)
                .validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Invalid configuration: pinot.aggregation-pushdown.enabled must be enabled if pinot.count-distinct-pushdown.enabled");
    }
}
