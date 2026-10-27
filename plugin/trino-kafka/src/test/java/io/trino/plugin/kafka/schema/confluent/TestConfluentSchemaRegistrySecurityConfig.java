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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.validation.FileExists;
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
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.DISABLED;
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.HTTPS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.JKS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.PKCS12;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConfluentSchemaRegistrySecurityConfig
{
    @TempDir
    Path tempDir;

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfluentSchemaRegistrySecurityConfig.class)
                .setBasicAuthUser(null)
                .setBasicAuthPassword(null)
                .setKeystoreLocation(null)
                .setKeystorePassword(null)
                .setKeystoreType(JKS)
                .setTruststoreLocation(null)
                .setTruststorePassword(null)
                .setTruststoreType(JKS)
                .setKeyPassword(null)
                .setEndpointIdentificationAlgorithm(HTTPS));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystorePath = Files.createTempFile(tempDir, "keystore", ".p12");
        Path truststorePath = Files.createTempFile(tempDir, "truststore", ".p12");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.confluent-schema-registry.basic-auth.user", "trino")
                .put("kafka.confluent-schema-registry.basic-auth.password", "secret")
                .put("kafka.confluent-schema-registry.ssl.keystore.location", keystorePath.toString())
                .put("kafka.confluent-schema-registry.ssl.keystore.password", "keystore-password")
                .put("kafka.confluent-schema-registry.ssl.keystore.type", "PKCS12")
                .put("kafka.confluent-schema-registry.ssl.truststore.location", truststorePath.toString())
                .put("kafka.confluent-schema-registry.ssl.truststore.password", "truststore-password")
                .put("kafka.confluent-schema-registry.ssl.truststore.type", "PKCS12")
                .put("kafka.confluent-schema-registry.ssl.key.password", "key-password")
                .put("kafka.confluent-schema-registry.ssl.endpoint-identification-algorithm", "disabled")
                .buildOrThrow();

        ConfluentSchemaRegistrySecurityConfig expected = new ConfluentSchemaRegistrySecurityConfig()
                .setBasicAuthUser("trino")
                .setBasicAuthPassword("secret")
                .setKeystoreLocation(keystorePath.toString())
                .setKeystorePassword("keystore-password")
                .setKeystoreType(PKCS12)
                .setTruststoreLocation(truststorePath.toString())
                .setTruststorePassword("truststore-password")
                .setTruststoreType(PKCS12)
                .setKeyPassword("key-password")
                .setEndpointIdentificationAlgorithm(DISABLED);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testClientProperties()
    {
        ConfluentSchemaRegistrySecurityConfig config = new ConfluentSchemaRegistrySecurityConfig()
                .setBasicAuthUser("trino")
                .setBasicAuthPassword("secret")
                .setKeystoreLocation("/some/path/to/keystore")
                .setKeystorePassword("keystore-password")
                .setKeystoreType(PKCS12)
                .setTruststoreLocation("/some/path/to/truststore")
                .setTruststorePassword("truststore-password")
                .setTruststoreType(JKS)
                .setKeyPassword("key-password")
                .setEndpointIdentificationAlgorithm(HTTPS);

        // The property names must match those expected by the schema registry client
        assertThat(config.getSchemaRegistryClientProperties())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.<String, Object>builder()
                        .put("basic.auth.credentials.source", "USER_INFO")
                        .put("basic.auth.user.info", "trino:secret")
                        .put("schema.registry.ssl.keystore.location", "/some/path/to/keystore")
                        .put("schema.registry.ssl.keystore.password", "keystore-password")
                        .put("schema.registry.ssl.keystore.type", "PKCS12")
                        .put("schema.registry.ssl.truststore.location", "/some/path/to/truststore")
                        .put("schema.registry.ssl.truststore.password", "truststore-password")
                        .put("schema.registry.ssl.truststore.type", "JKS")
                        .put("schema.registry.ssl.key.password", "key-password")
                        .put("schema.registry.ssl.endpoint.identification.algorithm", "https")
                        .buildOrThrow());
    }

    @Test
    public void testDefaultClientPropertiesVerifyHostName()
    {
        // The client only verifies the registry host name when the algorithm is passed explicitly
        assertThat(new ConfluentSchemaRegistrySecurityConfig().getSchemaRegistryClientProperties())
                .containsExactly(Map.entry("schema.registry.ssl.endpoint.identification.algorithm", "https"));
    }

    @Test
    public void testDisabledEndpointIdentificationAlgorithm()
    {
        ConfluentSchemaRegistrySecurityConfig config = new ConfluentSchemaRegistrySecurityConfig()
                .setEndpointIdentificationAlgorithm(DISABLED);
        assertThat(config.getSchemaRegistryClientProperties())
                .containsEntry("schema.registry.ssl.endpoint.identification.algorithm", "");
    }

    @Test
    public void testBasicAuthRequiresUserAndPassword()
    {
        String message = "kafka.confluent-schema-registry.basic-auth.user and kafka.confluent-schema-registry.basic-auth.password must be set together";
        assertFailsValidation(new ConfluentSchemaRegistrySecurityConfig().setBasicAuthUser("trino"), "basicAuthComplete", message, AssertTrue.class);
        assertFailsValidation(new ConfluentSchemaRegistrySecurityConfig().setBasicAuthPassword("secret"), "basicAuthComplete", message, AssertTrue.class);
        assertValidates(new ConfluentSchemaRegistrySecurityConfig().setBasicAuthUser("trino").setBasicAuthPassword("secret"));
    }

    @Test
    public void testBasicAuthUserMustNotContainColon()
    {
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setBasicAuthUser("tri:no").setBasicAuthPassword("secret"),
                "basicAuthUserValid",
                "kafka.confluent-schema-registry.basic-auth.user must not contain ':'",
                AssertTrue.class);
    }

    @Test
    public void testKeystorePasswordRequiredWithKeystoreLocation()
            throws IOException
    {
        Path keystorePath = Files.createTempFile(tempDir, "keystore", ".p12");
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setKeystoreLocation(keystorePath.toString()),
                "keystorePasswordValid",
                "kafka.confluent-schema-registry.ssl.keystore.password must be set when kafka.confluent-schema-registry.ssl.keystore.location is given",
                AssertTrue.class);
    }

    @Test
    public void testKeystoreLocationRequiredWithKeystorePasswords()
            throws IOException
    {
        String message = "kafka.confluent-schema-registry.ssl.keystore.location must be set when kafka.confluent-schema-registry.ssl.keystore.password or kafka.confluent-schema-registry.ssl.key.password is given";
        assertFailsValidation(new ConfluentSchemaRegistrySecurityConfig().setKeystorePassword("keystore-password"), "keystoreLocationValid", message, AssertTrue.class);
        assertFailsValidation(new ConfluentSchemaRegistrySecurityConfig().setKeyPassword("key-password"), "keystoreLocationValid", message, AssertTrue.class);
        assertValidates(new ConfluentSchemaRegistrySecurityConfig()
                .setKeystoreLocation(Files.createTempFile(tempDir, "keystore", ".p12").toString())
                .setKeystorePassword("keystore-password")
                .setKeyPassword("key-password"));
    }

    @Test
    public void testTruststoreLocationRequiredWithTruststorePassword()
            throws IOException
    {
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setTruststorePassword("truststore-password"),
                "truststoreLocationValid",
                "kafka.confluent-schema-registry.ssl.truststore.location must be set when kafka.confluent-schema-registry.ssl.truststore.password is given",
                AssertTrue.class);
        assertValidates(new ConfluentSchemaRegistrySecurityConfig()
                .setTruststoreLocation(Files.createTempFile(tempDir, "truststore", ".p12").toString())
                .setTruststorePassword("truststore-password"));
    }

    @Test
    public void testMissingStoreFile()
    {
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setTruststoreLocation("/not/a/real/path").setTruststorePassword("truststore-password"),
                "truststoreLocation",
                "file does not exist: /not/a/real/path",
                FileExists.class);
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setKeystoreLocation("/not/a/real/path").setKeystorePassword("keystore-password"),
                "keystoreLocation",
                "file does not exist: /not/a/real/path",
                FileExists.class);
    }

    @Test
    public void testBasicAuthPasswordMayContainColon()
    {
        // the client splits the user info at the first colon
        ConfluentSchemaRegistrySecurityConfig config = new ConfluentSchemaRegistrySecurityConfig()
                .setBasicAuthUser("trino")
                .setBasicAuthPassword("se:cr:et");
        assertValidates(config);
        assertThat(config.getSchemaRegistryClientProperties()).containsEntry("basic.auth.user.info", "trino:se:cr:et");
    }

    @Test
    public void testTruststorePasswordRequiredWithTruststoreLocation()
            throws IOException
    {
        Path truststorePath = Files.createTempFile(tempDir, "truststore", ".p12");
        assertFailsValidation(
                new ConfluentSchemaRegistrySecurityConfig().setTruststoreLocation(truststorePath.toString()),
                "truststorePasswordValid",
                "kafka.confluent-schema-registry.ssl.truststore.password must be set when kafka.confluent-schema-registry.ssl.truststore.location is given",
                AssertTrue.class);
    }
}
