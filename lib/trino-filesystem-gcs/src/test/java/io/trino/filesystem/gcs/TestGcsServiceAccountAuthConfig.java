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
package io.trino.filesystem.gcs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestGcsServiceAccountAuthConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsServiceAccountAuthConfig.class)
                .setJsonKey(null)
                .setJsonKeyFilePath(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        assertFullMapping(ImmutableMap.of("gcs.json-key", "{}"), new GcsServiceAccountAuthConfig().setJsonKey("{}"), ImmutableSet.of("gcs.json-key-file-path"));
        assertFullMapping(ImmutableMap.of("gcs.json-key-file-path", "/dev/null"), new GcsServiceAccountAuthConfig().setJsonKeyFilePath("/dev/null"), ImmutableSet.of("gcs.json-key"));
    }

    @Test
    void testValidation()
    {
        assertFailsValidation(
                new GcsServiceAccountAuthConfig(),
                "authMethodValid",
                "Either gcs.json-key or gcs.json-key-file-path must be set",
                AssertTrue.class);

        assertFailsValidation(
                new GcsServiceAccountAuthConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("file.json"),
                "authMethodValid",
                "Either gcs.json-key or gcs.json-key-file-path must be set",
                AssertTrue.class);

        assertFailsValidation(
                new GcsServiceAccountAuthConfig()
                        .setJsonKey("{}")
                        .setJsonKeyFilePath("/dev/null"),
                "authMethodValid",
                "Either gcs.json-key or gcs.json-key-file-path must be set",
                AssertTrue.class);
    }
}
