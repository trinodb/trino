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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaSecurityConfig
{
    @Test
    public void testDefaults()
    {
        // No defaults, should be empty if nothing is set specifically
        assertThat(recordDefaults(KafkaSecurityConfig.class).getKafkaClientProperties()).isEmpty();
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        String keystoreFilepath = getClass().getClassLoader().getResource("keystore.jks").getPath();
        String truststoreFilepath = getClass().getClassLoader().getResource("truststore.jks").getPath();
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.keystore-file", keystoreFilepath)
                .put("kafka.keystore-password", "keystore-password")
                .put("kafka.key-password", "key-password")
                .put("kafka.truststore-file", truststoreFilepath)
                .put("kafka.truststore-password", "truststore-password")
                .put("kafka.endpoint-identification-algorithm", "https")
                .build();
        KafkaSecurityConfig expected = new KafkaSecurityConfig()
                .setSslKeystoreFile(keystoreFilepath)
                .setSslKeystorePassword("keystore-password")
                .setSslKeyPassword("key-password")
                .setSslTruststoreFile(truststoreFilepath)
                .setSslTruststorePassword("truststore-password")
                .setSslEndpointIdentificationAlgorithm("https");

        assertFullMapping(properties, expected);
    }

    @Test
    public void verifyEmptyPropertiesAreReturned()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        Assert.assertEquals(sut.getKafkaClientProperties().isEmpty(), true);
    }

    @Test
    public void verifyAllConfigPropertiesAreContained()
    {
        KafkaSecurityConfig config = new KafkaSecurityConfig();
        config.setSslKeystoreFile("/some/path/to/keystore");
        config.setSslKeystorePassword("superSavePasswordForKeystore");
        config.setSslKeyPassword("aSslKeyPassword");
        config.setSslTruststoreFile("/some/path/to/truststore");
        config.setSslTruststorePassword("superSavePasswordForTruststore");
        config.setSslEndpointIdentificationAlgorithm(KafkaEndpointIdentificationAlgorithm.HTTPS.toString());
        Map<String, Object> securityProperties = config.getKafkaClientProperties();
        Assert.assertEquals(securityProperties.isEmpty(), false);
        Assert.assertEquals(securityProperties.keySet().size(), 6);
        // Since security related properties are all passed to the underlying kafka-clients library,
        // the property names must match those expected by kafka-clients
        assertThat(securityProperties).containsKey("ssl.keystore.location");
        assertThat(securityProperties).containsKey("ssl.keystore.password");
        assertThat(securityProperties).containsKey("ssl.truststore.location");
        assertThat(securityProperties).containsKey("ssl.truststore.password");
        assertThat(securityProperties).containsKey("ssl.key.password");
        assertThat(securityProperties).containsKey("ssl.endpoint.identification.algorithm");
    }

    @Test
    public void verifyDisabledIsTranslatedToKafkaConformValue()
    {
        KafkaSecurityConfig config = new KafkaSecurityConfig();
        config.setSslEndpointIdentificationAlgorithm("disabled");
        Map<String, Object> securityProperties = config.getKafkaClientProperties();
        assertThat(securityProperties).containsKey("ssl.endpoint.identification.algorithm");
        assertThat(securityProperties.get("ssl.endpoint.identification.algorithm")).isEqualTo("");
    }
}
