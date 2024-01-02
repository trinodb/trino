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
package io.trino.cache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCacheConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CacheConfig.class)
                .setEnabled(false)
                .setRevokingThreshold(0.9)
                .setRevokingTarget(0.7)
                .setCacheCommonSubqueriesEnabled(true)
                .setCacheAggregationsEnabled(true)
                .setCacheProjectionsEnabled(true)
                .setMaxSplitSize(DataSize.of(256, DataSize.Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cache.enabled", "true")
                .put("cache.revoking-threshold", "0.6")
                .put("cache.revoking-target", "0.5")
                .put("cache.common-subqueries.enabled", "false")
                .put("cache.aggregations.enabled", "false")
                .put("cache.projections.enabled", "false")
                .put("cache.max-split-size", "64MB")
                .buildOrThrow();

        CacheConfig expected = new CacheConfig()
                .setEnabled(true)
                .setRevokingThreshold(0.6)
                .setRevokingTarget(0.5)
                .setCacheAggregationsEnabled(false)
                .setCacheProjectionsEnabled(false)
                .setCacheCommonSubqueriesEnabled(false)
                .setMaxSplitSize(DataSize.of(64, DataSize.Unit.MEGABYTE));
        assertFullMapping(properties, expected);
    }
}
