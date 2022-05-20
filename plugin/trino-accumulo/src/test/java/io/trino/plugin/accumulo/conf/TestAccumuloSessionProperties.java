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
package io.trino.plugin.accumulo.conf;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.SessionPropertiesUtil.assertDefaultProperties;
import static io.trino.testing.SessionPropertiesUtil.assertExplicitProperties;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestAccumuloSessionProperties
{
    @Test
    public void testDefaults()
    {
        AccumuloSessionProperties provider = new AccumuloSessionProperties();

        Map<String, Object> properties = new HashMap<>();
        properties.put("optimize_locality_enabled", true);
        properties.put("optimize_split_ranges_enabled", true);
        properties.put("optimize_index_enabled", true);
        properties.put("index_rows_per_split", 10000);
        properties.put("index_threshold", 0.2);
        properties.put("index_lowest_cardinality_threshold", 0.01);
        properties.put("index_metrics_enabled", true);
        properties.put("scan_username", null);
        properties.put("index_short_circuit_cardinality_fetch", true);
        properties.put("index_cardinality_cache_polling_duration", new Duration(10, MILLISECONDS));

        assertDefaultProperties(provider.getSessionProperties(), properties);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        AccumuloSessionProperties provider = new AccumuloSessionProperties();

        Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put("optimize_locality_enabled", false)
                .put("optimize_split_ranges_enabled", false)
                .put("optimize_index_enabled", false)
                .put("index_rows_per_split", 999)
                .put("index_threshold", 0.5)
                .put("index_lowest_cardinality_threshold", 0.09)
                .put("index_metrics_enabled", false)
                .put("scan_username", "test")
                .put("index_short_circuit_cardinality_fetch", false)
                .put("index_cardinality_cache_polling_duration", new Duration(1, SECONDS))
                .buildOrThrow();

        assertExplicitProperties(provider.getSessionProperties(), properties);
    }
}
