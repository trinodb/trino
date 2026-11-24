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
package io.trino.plugin.inference;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.ConfigurationFactory;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

final class TestInferenceConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(InferenceConfig.class)
                .setEndpoint(null)
                .setMaxBatchSize(1000)
                .setMaxRetries(3)
                .setAuthenticationToken(null)
                .setAuthenticationTokenFile(null)
                .setAuthenticationHeaderName("Authorization"));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("inference.endpoint", "http://example.com:8080")
                .put("inference.max-batch-size", "64")
                .put("inference.max-retries", "5")
                .put("inference.authentication.token", "secret")
                .put("inference.authentication.header-name", "infer-key")
                .buildOrThrow();

        InferenceConfig expected = new InferenceConfig()
                .setEndpoint(URI.create("http://example.com:8080"))
                .setMaxBatchSize(64)
                .setMaxRetries(5)
                .setAuthenticationToken("secret")
                .setAuthenticationHeaderName("infer-key");

        // the token and the token file cannot be set together, so the token file is tested separately
        assertFullMapping(properties, expected, ImmutableSet.of("inference.authentication.token-file"));
    }

    @Test
    void testTokenFileMapping()
            throws IOException
    {
        Path tokenFile = Files.createTempFile(null, null);
        Map<String, String> properties = ImmutableMap.of(
                "inference.endpoint", "http://example.com:8080",
                "inference.authentication.token-file", tokenFile.toString());

        InferenceConfig config = new ConfigurationFactory(properties).build(InferenceConfig.class);

        assertThat(config.getAuthenticationTokenFile()).contains(tokenFile.toFile());
        assertThat(config.getAuthenticationToken()).isEmpty();
    }

    @Test
    void testTokenAndTokenFile()
            throws IOException
    {
        File tokenFile = Files.createTempFile(null, null).toFile();
        assertFailsValidation(
                new InferenceConfig()
                        .setEndpoint(URI.create("http://example.com:8080"))
                        .setAuthenticationToken("secret")
                        .setAuthenticationTokenFile(tokenFile),
                "authenticationTokenValid",
                "Only one of inference.authentication.token and inference.authentication.token-file can be set",
                AssertTrue.class);
    }
}
