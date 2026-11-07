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
package io.trino.metastore.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestLakehouseMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LakehouseMetastoreConfig.class)
                .setProjectId(null)
                .setCatalogId(null)
                .setRequestTimeout(new Duration(1, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.lakehouse.project-id", "example-project")
                .put("hive.metastore.lakehouse.catalog-id", "example-catalog")
                .put("hive.metastore.lakehouse.request-timeout", "30s")
                .buildOrThrow();

        LakehouseMetastoreConfig expected = new LakehouseMetastoreConfig()
                .setProjectId("example-project")
                .setCatalogId("example-catalog")
                .setRequestTimeout(new Duration(30, SECONDS));

        assertFullMapping(properties, expected);
    }
}
