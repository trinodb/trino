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
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestJdbcDynamicFilteringConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JdbcDynamicFilteringConfig.class)
                .setDynamicFilteringEnabled(true)
                .setDynamicFilteringWaitTimeout(new Duration(20, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("dynamic-filtering.enabled", "false")
                .put("dynamic-filtering.wait-timeout", "10m")
                .buildOrThrow();

        JdbcDynamicFilteringConfig expected = new JdbcDynamicFilteringConfig()
                .setDynamicFilteringEnabled(false)
                .setDynamicFilteringWaitTimeout(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }
}
