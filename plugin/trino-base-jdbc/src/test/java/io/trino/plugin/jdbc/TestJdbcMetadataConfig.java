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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestJdbcMetadataConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcMetadataConfig.class)
                .setComplexExpressionPushdownEnabled(true)
                .setJoinPushdownEnabled(false)
                .setAggregationPushdownEnabled(true)
                .setTopNPushdownEnabled(true)
                .setDomainCompactionThreshold(32));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("complex-expression-pushdown.enabled", "false")
                .put("join-pushdown.enabled", "true")
                .put("aggregation-pushdown.enabled", "false")
                .put("domain-compaction-threshold", "42")
                .put("topn-pushdown.enabled", "false")
                .buildOrThrow();

        JdbcMetadataConfig expected = new JdbcMetadataConfig()
                .setComplexExpressionPushdownEnabled(false)
                .setJoinPushdownEnabled(true)
                .setAggregationPushdownEnabled(false)
                .setTopNPushdownEnabled(false)
                .setDomainCompactionThreshold(42);

        assertFullMapping(properties, expected);
    }
}
