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
package io.trino.plugin.redis.tls;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestRedisTlsConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RedisTlsConfig.class)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null));
    }

    @Test
    void testExplicitPropertyMappings(@TempDir Path tempDir)
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(tempDir, null, null);
        Path truststoreFile = Files.createTempFile(tempDir, null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redis.tls.keystore-path", keystoreFile.toString())
                .put("redis.tls.keystore-password", "keystore-password")
                .put("redis.tls.truststore-path", truststoreFile.toString())
                .put("redis.tls.truststore-password", "truststore-password")
                .buildOrThrow();

        RedisTlsConfig expected = new RedisTlsConfig()
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password");

        assertFullMapping(properties, expected);
    }
}
