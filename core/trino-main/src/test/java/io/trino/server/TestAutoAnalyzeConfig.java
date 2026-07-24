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
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestAutoAnalyzeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AutoAnalyzeConfig.class)
                .setEnabled(false)
                .setCheckInterval(new Duration(10, TimeUnit.MINUTES))
                .setSmallTableThreshold(100_000)
                .setSmallTableInterval(new Duration(1, TimeUnit.HOURS))
                .setLargeTableInterval(new Duration(24, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("auto-analyze.enabled", "true")
                .put("auto-analyze.check-interval", "5m")
                .put("auto-analyze.small-table-threshold", "50000")
                .put("auto-analyze.small-table-interval", "30m")
                .put("auto-analyze.large-table-interval", "12h")
                .buildOrThrow();

        AutoAnalyzeConfig expected = new AutoAnalyzeConfig()
                .setEnabled(true)
                .setCheckInterval(new Duration(5, TimeUnit.MINUTES))
                .setSmallTableThreshold(50_000)
                .setSmallTableInterval(new Duration(30, TimeUnit.MINUTES))
                .setLargeTableInterval(new Duration(12, TimeUnit.HOURS));

        assertFullMapping(properties, expected);
    }
}
