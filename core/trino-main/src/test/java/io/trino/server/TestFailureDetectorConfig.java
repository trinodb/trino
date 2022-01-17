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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.failuredetector.FailureDetectorConfig;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFailureDetectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FailureDetectorConfig.class)
                .setExpirationGraceInterval(new Duration(10, TimeUnit.MINUTES))
                .setFailureRatioThreshold(0.1)
                .setHeartbeatInterval(new Duration(500, TimeUnit.MILLISECONDS))
                .setWarmupInterval(new Duration(5, TimeUnit.SECONDS))
                .setEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("failure-detector.expiration-grace-interval", "5m")
                .put("failure-detector.warmup-interval", "60s")
                .put("failure-detector.heartbeat-interval", "10s")
                .put("failure-detector.threshold", "0.5")
                .put("failure-detector.enabled", "false")
                .buildOrThrow();

        FailureDetectorConfig expected = new FailureDetectorConfig()
                .setExpirationGraceInterval(new Duration(5, TimeUnit.MINUTES))
                .setWarmupInterval(new Duration(60, TimeUnit.SECONDS))
                .setHeartbeatInterval(new Duration(10, TimeUnit.SECONDS))
                .setFailureRatioThreshold(0.5)
                .setEnabled(false);

        assertFullMapping(properties, expected);
    }
}
