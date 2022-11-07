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
import io.trino.plugin.pinot.client.PinotLegacyServerQueryClientConfig;
import org.testng.annotations.Test;

import java.util.Map;

public class TestPinotLegacyServerQueryClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotLegacyServerQueryClientConfig.class)
                        .setMaxRowsPerSplitForSegmentQueries(50_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.of("pinot.max-rows-per-split-for-segment-queries", "10");
        PinotLegacyServerQueryClientConfig expected = new PinotLegacyServerQueryClientConfig()
                .setMaxRowsPerSplitForSegmentQueries(10);
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
