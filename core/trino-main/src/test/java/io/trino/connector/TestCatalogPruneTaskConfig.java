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
package io.trino.connector;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestCatalogPruneTaskConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CatalogPruneTaskConfig.class)
                .setEnabled(true)
                .setUpdateInterval(new Duration(5, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("catalog.prune.enabled", "false")
                .put("catalog.prune.update-interval", "37s")
                .buildOrThrow();

        CatalogPruneTaskConfig expected = new CatalogPruneTaskConfig()
                .setEnabled(false)
                .setUpdateInterval(new Duration(37, SECONDS));

        assertFullMapping(properties, expected);
    }
}
