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
package io.trino.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestDbResourceGroupConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DbResourceGroupConfig.class)
                .setConfigDbUrl(null)
                .setConfigDbUser(null)
                .setConfigDbPassword(null)
                .setMaxRefreshInterval(new Duration(1, HOURS))
                .setExactMatchSelectorEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("resource-groups.config-db-url", "jdbc:mysql://localhost:3306/config")
                .put("resource-groups.config-db-user", "trino_admin")
                .put("resource-groups.config-db-password", "trino_admin_pass")
                .put("resource-groups.max-refresh-interval", "1m")
                .put("resource-groups.exact-match-selector-enabled", "true")
                .buildOrThrow();
        DbResourceGroupConfig expected = new DbResourceGroupConfig()
                .setConfigDbUrl("jdbc:mysql://localhost:3306/config")
                .setConfigDbUser("trino_admin")
                .setConfigDbPassword("trino_admin_pass")
                .setMaxRefreshInterval(new Duration(1, MINUTES))
                .setExactMatchSelectorEnabled(true);

        assertFullMapping(properties, expected);
    }
}
