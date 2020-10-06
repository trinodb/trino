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

import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertTrue;

public class TestKafkaSecurityConfig
{
    @Test
    public void verifyEmptyPropertiesAreReturned()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        Assert.assertEquals(sut.getKafkaClientProperties().isEmpty(), true);
    }

    @Test
    public void verifyAllConfigPropertiesAreContained()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        sut.setSslKeystoreFile("/some/path/to/keystore");
        sut.setSslKeystorePassword("superSavePasswordForKeystore");
        sut.setSslKeyPassword("aSslKeyPassword");
        sut.setSslTruststoreFile("/some/path/to/truststore");
        sut.setSslTruststorePassword("superSavePasswordForTruststore");
        sut.setSslEndpointIdentificationAlgorithm("https");
        Properties securityProperties = sut.getKafkaClientProperties();
        Assert.assertEquals(securityProperties.isEmpty(), false);
        Assert.assertEquals(securityProperties.keySet().size(), 6);
        // Since security related properties are all passed to the underlying kafka-clients library,
        // the property names must match those expected by kafka-clients
        assertTrue(securityProperties.containsKey("ssl.keystore.location"));
        assertTrue(securityProperties.containsKey("ssl.keystore.password"));
        assertTrue(securityProperties.containsKey("ssl.truststore.location"));
        assertTrue(securityProperties.containsKey("ssl.truststore.password"));
        assertTrue(securityProperties.containsKey("ssl.key.password"));
        assertTrue(securityProperties.containsKey("ssl.endpoint.identification.algorithm"));
    }
}
