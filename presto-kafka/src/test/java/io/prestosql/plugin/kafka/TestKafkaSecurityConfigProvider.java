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
import org.junit.Test;

import java.util.Properties;

public class TestKafkaSecurityConfigProvider
{
    @Test
    public void verifyEmptyPropertiesAreReturned()
    {
        KafkaSecurityConfigProvider sut = new KafkaSecurityConfigProvider();
        Assert.assertEquals(sut.getSecurityProperties().isEmpty(), true);
    }

    @Test
    public void verifyAllConfigPropertiesAreContained()
    {
        KafkaSecurityConfigProvider sut = new KafkaSecurityConfigProvider();
        sut.setSslProtocol("TLS");
        sut.setSslProvider("sslProvider");
        sut.setSslCipherSuites("cipherSuitesList");
        sut.setSslEnabledProtocols("TLSv1;TLSv3");
        sut.setSslKeystoreType("JKS");
        sut.setSslKeystoreLocation("/some/path/to/keystore");
        sut.setSslKeystorePassword("superSavePasswordForKeystore");
        sut.setSslKeyPassword("aSslKeyPassword");
        sut.setSslTruststoreType("JKS");
        sut.setSslTruststoreLocation("/some/path/to/truststore");
        sut.setSslTruststorePassword("superSavePasswordForTruststore");
        sut.setSslKeymanagerAlgorithm("key manager algorithm");
        sut.setSslTrustmanagerAlgorithm("trust manager algorithm");
        sut.setSslEndpointIdentificationAlgorithm("https");
        sut.setSslSecureRandomImplementation("SecureRandom PRNG implementation");
        Properties securityProperties = sut.getSecurityProperties();
        Assert.assertEquals(securityProperties.isEmpty(), false);
        Assert.assertEquals(securityProperties.keySet().size(), 15);
    }
}
