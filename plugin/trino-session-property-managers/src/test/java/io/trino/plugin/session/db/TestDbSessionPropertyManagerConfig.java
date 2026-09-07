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
package io.trino.plugin.session.db;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDbSessionPropertyManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DbSessionPropertyManagerConfig.class)
                .setConfigDbUrl(null)
                .setConfigDbUser(null)
                .setConfigDbPassword(null)
                .setMaxRefreshInterval(new Duration(1, HOURS))
                .setRefreshInterval(new Duration(1, SECONDS))
                .setRunMigrationsEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("session-property-manager.config-db-url", "foo")
                .put("session-property-manager.config-db-user", "bar")
                .put("session-property-manager.config-db-password", "pass")
                .put("session-property-manager.max-refresh-interval", "1m")
                .put("session-property-manager.refresh-interval", "2s")
                .put("session-property-manager.db-migrations-enabled", "false")
                .buildOrThrow();

        DbSessionPropertyManagerConfig expected = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl("foo")
                .setConfigDbUser("bar")
                .setConfigDbPassword("pass")
                .setMaxRefreshInterval(new Duration(1, MINUTES))
                .setRefreshInterval(new Duration(2, SECONDS))
                .setRunMigrationsEnabled(false);

        assertFullMapping(properties, expected);
    }
}
