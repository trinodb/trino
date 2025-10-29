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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestDruidConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DruidConfig.class)
                .setExecutionTimeout(0)
                .setSqlTimeZone(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("druid.execution-timeout", "10000")
                .put("druid.sql-timezone", "UTC")
                .buildOrThrow();

        DruidConfig expected = new DruidConfig()
                .setExecutionTimeout(10000)
                .setSqlTimeZone("UTC");

        assertFullMapping(properties, expected);
    }
}
