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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import java.util.Properties;

public class KafkaSecurityConfigTest
{
    @Test
    public void verifyEmptyPropertiesAreReturned()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        Assert.assertEquals(sut.getKafkaSecurityConfig().isEmpty(), true);
    }

    @Test
    public void verifyEmptyPropertiesWhenPlaintextSecurityProtocol()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        sut.setSecurityProtocol(SecurityProtocol.PLAINTEXT);
        sut.setSslTruststoreLocation("/some/path/to/truststore");
        sut.setSslTruststorePassword("superSavePassword");
        Assert.assertEquals(sut.getKafkaSecurityConfig().isEmpty(), true);
    }

    @Test
    public void verifyEmptyPropertiesWhenDefaultSecurityProtocol()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        sut.setSecurityProtocol(SecurityProtocol.PLAINTEXT);
        sut.setSslTruststoreLocation("/some/path/to/truststore");
        sut.setSslTruststorePassword("superSavePassword");
        sut.setSslTruststoreType("JKS");
        sut.setSslKeystoreLocation("/some/path/to/keystore");
        sut.setSslKeystorePassword("savePassword");
        sut.setSslKeystoreType("JKS");
        sut.setSslEndpointIdentificationAlgorithm("HTTPS");
        sut.setSslKeyPassword("keyPassword");
        sut.setSslProvider("provider");
        Assert.assertEquals(sut.getKafkaSecurityConfig().isEmpty(), true);
    }

    @Test
    public void verifyAllConfigPropertiesAreContained()
    {
        KafkaSecurityConfig sut = new KafkaSecurityConfig();
        sut.setSecurityProtocol(SecurityProtocol.SSL);
        sut.setSslTruststoreLocation("/some/path/to/truststore");
        sut.setSslTruststorePassword("superSavePasswordForTruststore");
        sut.setSslKeystoreLocation("/some/path/to/keystore");
        sut.setSslKeystorePassword("superSavePasswordForKeystore");
        sut.setSslKeyPassword("aSslKeyPassword");
        Properties securityProperties = sut.getKafkaSecurityConfig();
        Assert.assertEquals(securityProperties.isEmpty(), false);
        Assert.assertEquals(securityProperties.keySet().size(), 6);
        Assert.assertEquals(securityProperties.get("security.protocol"), "SSL");
    }
}
