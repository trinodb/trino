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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestJmxConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JmxConnectorConfig.class)
                .setDumpTables("")
                .setDumpPeriod(new Duration(10, TimeUnit.SECONDS))
                .setMaxEntries(24 * 60 * 60));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("jmx.dump-tables", "table1,table\\,with\\,commas")
                .put("jmx.dump-period", "1s")
                .put("jmx.max-entries", "100")
                .buildOrThrow();

        JmxConnectorConfig expected = new JmxConnectorConfig()
                .setDumpTables(ImmutableSet.of("table1", "table,with,commas"))
                .setDumpPeriod(new Duration(1, TimeUnit.SECONDS))
                .setMaxEntries(100);

        assertFullMapping(properties, expected);
        assertDeprecatedEquivalence(JmxConnectorConfig.class, properties);
    }
}
