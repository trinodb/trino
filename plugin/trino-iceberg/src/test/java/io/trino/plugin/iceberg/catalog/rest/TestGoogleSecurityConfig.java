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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestGoogleSecurityConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GoogleSecurityConfig.class)
                .setProjectId(null)
                .setJsonKey(null)
                .setJsonKeyFilePath(null));
    }

    @Test
    void testExplicitPropertyMappings(@TempDir Path config)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest-catalog.google-project-id", "gcp")
                .put("gcs.json-key-file-path", config.toString())
                .buildOrThrow();

        GoogleSecurityConfig expected = new GoogleSecurityConfig()
                .setProjectId("gcp")
                .setJsonKeyFilePath(config.toString());

        assertFullMapping(properties, expected, ImmutableSet.of("gcs.json-key"));
    }

    @Test
    void testJsonKeyExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest-catalog.google-project-id", "gcp")
                .put("gcs.json-key", "{}")
                .buildOrThrow();

        GoogleSecurityConfig expected = new GoogleSecurityConfig()
                .setProjectId("gcp")
                .setJsonKey("{}");

        assertFullMapping(properties, expected, ImmutableSet.of("gcs.json-key-file-path"));
    }

    @Test
    void testValidation()
    {
        assertFailsValidation(
                new GoogleSecurityConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("file.json"),
                "authMethodValid",
                "gcs.json-key and gcs.json-key-file-path cannot be set at the same time",
                AssertTrue.class);
    }
}
