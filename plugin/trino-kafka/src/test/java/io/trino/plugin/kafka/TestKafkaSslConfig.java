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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.DISABLED;
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.HTTPS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.JKS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.PKCS12;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaSslConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaSslConfig.class)
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
        String secret = "confluent";
        Path keystorePath = Files.createTempFile("keystore", ".p12");
        Path truststorePath = Files.createTempFile("truststore", ".p12");

        writeToFile(keystorePath, secret);
        writeToFile(truststorePath, secret);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.ssl.keystore.location", keystorePath.toString())
                .put("kafka.ssl.keystore.password", "keystore-password")
                .put("kafka.ssl.keystore.type", "PKCS12")
                .put("kafka.ssl.truststore.location", truststorePath.toString())
                .put("kafka.ssl.truststore.password", "truststore-password")
                .put("kafka.ssl.truststore.type", "PKCS12")
                .put("kafka.ssl.key.password", "key-password")
                .put("kafka.ssl.endpoint-identification-algorithm", "disabled")
                .buildOrThrow();
        KafkaSslConfig expected = new KafkaSslConfig()
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
    public void testAllConfigPropertiesAreContained()
    {
        KafkaSslConfig config = new KafkaSslConfig()
                .setKeystoreLocation("/some/path/to/keystore")
                .setKeystorePassword("superSavePasswordForKeystore")
                .setKeystoreType(JKS)
                .setTruststoreLocation("/some/path/to/truststore")
                .setTruststorePassword("superSavePasswordForTruststore")
                .setTruststoreType(JKS)
                .setKeyPassword("aSslKeyPassword")
                .setEndpointIdentificationAlgorithm(HTTPS);

        Map<String, Object> securityProperties = config.getKafkaClientProperties();
        // Since security related properties are all passed to the underlying kafka-clients library,
        // the property names must match those expected by kafka-clients
        assertThat(securityProperties)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.copyOf(Map.of(
                        SSL_KEYSTORE_LOCATION_CONFIG, "/some/path/to/keystore",
                        SSL_KEYSTORE_PASSWORD_CONFIG, "superSavePasswordForKeystore",
                        SSL_KEYSTORE_TYPE_CONFIG, JKS.name(),
                        SSL_TRUSTSTORE_LOCATION_CONFIG, "/some/path/to/truststore",
                        SSL_TRUSTSTORE_PASSWORD_CONFIG, "superSavePasswordForTruststore",
                        SSL_TRUSTSTORE_TYPE_CONFIG, JKS.name(),
                        SSL_KEY_PASSWORD_CONFIG, "aSslKeyPassword",
                        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, HTTPS.getValue())));
    }

    @Test
    public void testDisabledEndpointIdentificationAlgorithm()
    {
        KafkaSslConfig config = new KafkaSslConfig();
        if (KafkaEndpointIdentificationAlgorithm.fromString("disabled").isPresent()) {
            config.setEndpointIdentificationAlgorithm(KafkaEndpointIdentificationAlgorithm.fromString("disabled").get());
        }
        Map<String, Object> securityProperties = config.getKafkaClientProperties();
        assertThat(securityProperties).containsKey(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        assertThat(securityProperties.get(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)).isEqualTo("");
    }

    @Test
    public void testFailOnMissingKeystorePasswordWithKeystoreLocationSet()
            throws Exception
    {
        String secret = "confluent";
        Path keystorePath = Files.createTempFile("keystore", ".p12");

        writeToFile(keystorePath, secret);

        KafkaSslConfig config = new KafkaSslConfig();
        config.setKeystoreLocation(keystorePath.toString());
        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("kafka.ssl.keystore.password must set when kafka.ssl.keystore.location is given");
    }

    @Test
    public void testFailOnMissingTruststorePasswordWithTruststoreLocationSet()
            throws Exception
    {
        String secret = "confluent";
        Path truststorePath = Files.createTempFile("truststore", ".p12");

        writeToFile(truststorePath, secret);

        KafkaSslConfig config = new KafkaSslConfig();
        config.setTruststoreLocation(truststorePath.toString());
        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("kafka.ssl.truststore.password must set when kafka.ssl.truststore.location is given");
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (FileWriter writer = new FileWriter(filepath.toFile(), UTF_8)) {
            writer.write(content);
        }
    }
}
