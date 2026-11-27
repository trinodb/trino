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
package io.trino.plugin.hive.metastore.lakehouse;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
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
                .setJsonKey(null)
                .setJsonKeyFilePath(null)
                .setRequestTimeout(new Duration(1, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings(@TempDir Path jsonKeyFilePath)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.lakehouse.project-id", "example-project")
                .put("hive.metastore.lakehouse.catalog-id", "example-catalog")
                .put("hive.metastore.lakehouse.google-json-key-file-path", jsonKeyFilePath.toString())
                .put("hive.metastore.lakehouse.request-timeout", "30s")
                .buildOrThrow();

        LakehouseMetastoreConfig expected = new LakehouseMetastoreConfig()
                .setProjectId("example-project")
                .setCatalogId("example-catalog")
                .setJsonKeyFilePath(jsonKeyFilePath.toString())
                .setRequestTimeout(new Duration(30, SECONDS));

        assertFullMapping(properties, expected, ImmutableSet.of("hive.metastore.lakehouse.google-json-key"));
    }

    @Test
    public void testJsonKeyExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.lakehouse.project-id", "example-project")
                .put("hive.metastore.lakehouse.catalog-id", "example-catalog")
                .put("hive.metastore.lakehouse.google-json-key", "{}")
                .put("hive.metastore.lakehouse.request-timeout", "30s")
                .buildOrThrow();

        LakehouseMetastoreConfig expected = new LakehouseMetastoreConfig()
                .setProjectId("example-project")
                .setCatalogId("example-catalog")
                .setJsonKey("{}")
                .setRequestTimeout(new Duration(30, SECONDS));

        assertFullMapping(properties, expected, ImmutableSet.of("hive.metastore.lakehouse.google-json-key-file-path"));
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new LakehouseMetastoreConfig()
                        .setProjectId("example-project")
                        .setCatalogId("example-catalog")
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("file.json"),
                "authMethodValid",
                "hive.metastore.lakehouse.google-json-key and hive.metastore.lakehouse.google-json-key-file-path cannot be set at the same time",
                AssertTrue.class);
    }
}
