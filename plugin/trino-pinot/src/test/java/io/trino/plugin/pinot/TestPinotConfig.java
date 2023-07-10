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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPinotConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotConfig.class)
                        .setControllerUrls("")
                        .setEstimatedSizeInBytesForNonNumericColumn(20)
                        .setConnectionTimeout(new Duration(1, TimeUnit.MINUTES))
                        .setMetadataCacheExpiry(new Duration(2, TimeUnit.MINUTES))
                        .setPreferBrokerQueries(false)
                        .setSegmentsPerSplit(1)
                        .setFetchRetryCount(2)
                        .setForbidSegmentQueries(false)
                        .setNonAggregateLimitForBrokerQueries(25_000)
                        .setMaxRowsForBrokerQueries(50_000)
                        .setAggregationPushdownEnabled(true)
                        .setCountDistinctPushdownEnabled(true)
                        .setGrpcEnabled(true)
                        .setProxyEnabled(false)
                        .setTargetSegmentPageSize(DataSize.of(1, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", "https://host1:1111,https://host2:1111")
                .put("pinot.estimated-size-in-bytes-for-non-numeric-column", "30")
                .put("pinot.connection-timeout", "8m")
                .put("pinot.metadata-expiry", "1m")
                .put("pinot.prefer-broker-queries", "true")
                .put("pinot.segments-per-split", "2")
                .put("pinot.fetch-retry-count", "3")
                .put("pinot.non-aggregate-limit-for-broker-queries", "10")
                .put("pinot.forbid-segment-queries", "true")
                .put("pinot.max-rows-for-broker-queries", "5000")
                .put("pinot.aggregation-pushdown.enabled", "false")
                .put("pinot.count-distinct-pushdown.enabled", "false")
                .put("pinot.grpc.enabled", "false")
                .put("pinot.proxy.enabled", "true")
                .put("pinot.target-segment-page-size", "2MB")
                .buildOrThrow();

        PinotConfig expected = new PinotConfig()
                .setControllerUrls("https://host1:1111,https://host2:1111")
                .setEstimatedSizeInBytesForNonNumericColumn(30)
                .setConnectionTimeout(new Duration(8, TimeUnit.MINUTES))
                .setMetadataCacheExpiry(new Duration(1, TimeUnit.MINUTES))
                .setPreferBrokerQueries(true)
                .setSegmentsPerSplit(2)
                .setFetchRetryCount(3)
                .setNonAggregateLimitForBrokerQueries(10)
                .setForbidSegmentQueries(true)
                .setMaxRowsForBrokerQueries(5000)
                .setAggregationPushdownEnabled(false)
                .setCountDistinctPushdownEnabled(false)
                .setGrpcEnabled(false)
                .setProxyEnabled(true)
                .setTargetSegmentPageSize(DataSize.of(2, MEGABYTE));

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

    @Test
    public void testControllerUrls()
    {
        PinotConfig config = new PinotConfig();
        config.setControllerUrls("my-controller-1:8443,my-controller-2:8443");
        assertTrue(config.allUrlSchemesEqual());
        assertFalse(config.isTlsEnabled());
        config.setControllerUrls("http://my-controller-1:9000,http://my-controller-2:9000");
        assertTrue(config.allUrlSchemesEqual());
        assertFalse(config.isTlsEnabled());
        config.setControllerUrls("https://my-controller-1:8443,https://my-controller-2:8443");
        assertTrue(config.allUrlSchemesEqual());
        assertTrue(config.isTlsEnabled());
        config.setControllerUrls("my-controller-1:8443,http://my-controller-2:8443");
        assertTrue(config.allUrlSchemesEqual());
        assertFalse(config.isTlsEnabled());
        config.setControllerUrls("http://my-controller-1:8443,https://my-controller-2:8443");
        assertFalse(config.allUrlSchemesEqual());
        config.setControllerUrls("my-controller-1:8443,https://my-controller-2:8443");
        assertFalse(config.allUrlSchemesEqual());
    }
}
