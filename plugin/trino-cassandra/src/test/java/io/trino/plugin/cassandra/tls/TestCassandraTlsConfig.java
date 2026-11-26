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
package io.trino.plugin.cassandra.tls;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

final class TestCassandraTlsConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CassandraTlsConfig.class)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setVerifyHostnames(true)
                .setUseReverseDns(false));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cassandra.tls.keystore-path", keystoreFile.toString())
                .put("cassandra.tls.keystore-password", "keystore-password")
                .put("cassandra.tls.truststore-path", truststoreFile.toString())
                .put("cassandra.tls.truststore-password", "truststore-password")
                .put("cassandra.tls.verify-hostnames", "false")
                .put("cassandra.tls.use-reverse-dns", "true")
                .buildOrThrow();

        CassandraTlsConfig expected = new CassandraTlsConfig()
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setVerifyHostnames(false)
                .setUseReverseDns(true);

        assertFullMapping(properties, expected);
    }

    @Test
    void testKeystoreOrTruststoreRequired(@TempDir Path storeFile)
    {
        // the default trust anchors of the JVM are used only when certificates are verified against the address used to connect
        assertValidates(new CassandraTlsConfig());
        assertFailsValidation(
                new CassandraTlsConfig().setVerifyHostnames(false),
                "keystoreOrTruststoreConfigured",
                "cassandra.tls.keystore-path or cassandra.tls.truststore-path must be set when cassandra.tls.verify-hostnames is false or cassandra.tls.use-reverse-dns is true",
                AssertTrue.class);
        assertFailsValidation(
                new CassandraTlsConfig().setUseReverseDns(true),
                "keystoreOrTruststoreConfigured",
                "cassandra.tls.keystore-path or cassandra.tls.truststore-path must be set when cassandra.tls.verify-hostnames is false or cassandra.tls.use-reverse-dns is true",
                AssertTrue.class);

        assertValidates(new CassandraTlsConfig().setVerifyHostnames(false).setKeystorePath(storeFile.toFile()));
        assertValidates(new CassandraTlsConfig().setVerifyHostnames(false).setTruststorePath(storeFile.toFile()));
        assertValidates(new CassandraTlsConfig().setUseReverseDns(true).setTruststorePath(storeFile.toFile()));
    }
}
