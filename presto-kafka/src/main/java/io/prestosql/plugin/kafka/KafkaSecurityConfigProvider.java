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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 *  {@KafkaSecurityConfig} manages Kafka configuration related to connection security. I.e. if the security protocol
 *  'SSL' is being used. If so, additional properties might be supplied, i.e. location and password of the truststore
 *  and keystore to be used.
 */
public class KafkaSecurityConfigProvider
{
    private Properties props;

    private String sslProtocol;
    private String sslProvider;
    private String sslCipherSuites;
    private String sslEnabledProtocols;
    private String sslKeystoreType;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslKeyPassword;
    private String sslTruststoreType;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeymanagerAlgorithm;
    private String sslTrustmanagerAlgorithm;
    private String sslEndpointIdentificationAlgorithm;
    private String sslSecureRandomImplementation;

    public KafkaSecurityConfigProvider()
    {
        // no-args constructor
    }

    @Config("kafka." + SslConfigs.SSL_PROTOCOL_CONFIG)
    @ConfigDescription(SslConfigs.SSL_PROTOCOL_DOC)
    public void setSslProtocol(String sslProtocol)
    {
        this.sslProtocol = sslProtocol;
    }

    public String getSslProtocol()
    {
        return sslProtocol;
    }

    @Config("kafka." + SslConfigs.SSL_PROVIDER_CONFIG)
    @ConfigDescription(SslConfigs.SSL_PROVIDER_DOC)
    public void setSslProvider(String sslProvider)
    {
        this.sslProvider = sslProvider;
    }

    public String getSslProvider()
    {
        return sslProvider;
    }

    @Config("kafka." + SslConfigs.SSL_CIPHER_SUITES_CONFIG)
    @ConfigDescription(SslConfigs.SSL_CIPHER_SUITES_DOC)
    public void setSslCipherSuites(String sslCipherSuites)
    {
        this.sslCipherSuites = sslCipherSuites;
    }

    public String getSslCipherSuites()
    {
        return sslCipherSuites;
    }

    @Config("kafka." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG)
    @ConfigDescription(SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
    public void setSslEnabledProtocols(String sslEnabledProtocols)
    {
        this.sslEnabledProtocols = sslEnabledProtocols;
    }

    public String getSslEnabledProtocols()
    {
        return sslEnabledProtocols;
    }

    @Config("kafka." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_TYPE_DOC)
    public void setSslKeystoreType(String sslKeystoreType)
    {
        this.sslKeystoreType = sslKeystoreType;
    }

    public String getSslKeystoreType()
    {
        return sslKeystoreType;
    }

    @Config("kafka." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
    public void setSslKeystoreLocation(String sslKeystoreLocation)
    {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystoreLocation()
    {
        return sslKeystoreLocation;
    }

    @Config("kafka." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public void setSslKeystorePassword(String sslKeystorePassword)
    {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeystorePassword()
    {
        return sslKeystorePassword;
    }

    @Config("kafka." + SslConfigs.SSL_KEY_PASSWORD_CONFIG)
    @ConfigDescription(SslConfigs.SSL_KEY_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public void setSslKeyPassword(String sslKeyPassword)
    {
        this.sslKeyPassword = sslKeyPassword;
    }

    public String getSslKeyPassword()
    {
        return sslKeyPassword;
    }

    @Config("kafka." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
    public void setSslTruststoreType(String sslTruststoreType)
    {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslTruststoreType()
    {
        return sslTruststoreType;
    }

    @Config("kafka." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
    public void setSslTruststoreLocation(String sslTruststoreLocation)
    {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststoreLocation()
    {
        return sslTruststoreLocation;
    }

    @Config("kafka." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public void setSslTruststorePassword(String sslTruststorePassword)
    {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststorePassword()
    {
        return sslTruststorePassword;
    }

    @Config("kafka." + SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG)
    @ConfigDescription(SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
    public void setSslKeymanagerAlgorithm(String sslKeymanagerAlgorithm)
    {
        this.sslKeymanagerAlgorithm = sslKeymanagerAlgorithm;
    }

    public String getSslKeymanagerAlgorithm()
    {
        return sslKeymanagerAlgorithm;
    }

    @Config("kafka." + SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)
    @ConfigDescription(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
    public void setSslTrustmanagerAlgorithm(String sslTrustmanagerAlgorithm)
    {
        this.sslTrustmanagerAlgorithm = sslTrustmanagerAlgorithm;
    }

    public String getSslTrustmanagerAlgorithm()
    {
        return sslTrustmanagerAlgorithm;
    }

    @Config("kafka." + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)
    @ConfigDescription(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm)
    {
        this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
    }

    public String getSslEndpointIdentificationAlgorithm()
    {
        return sslEndpointIdentificationAlgorithm;
    }

    @Config("kafka." + SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG)
    @ConfigDescription(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
    public void setSslSecureRandomImplementation(String sslSecureRandomImplementation)
    {
        this.sslSecureRandomImplementation = sslSecureRandomImplementation;
    }

    public String getSslSecureRandomImplementation()
    {
        return sslSecureRandomImplementation;
    }

    public Properties getSecurityProperties()
    {
        props = new Properties();
        addNotNull(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
        addNotNull(SslConfigs.SSL_PROVIDER_CONFIG, sslProvider);
        addNotNull(SslConfigs.SSL_CIPHER_SUITES_CONFIG, sslCipherSuites);
        addNotNull(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, sslEnabledProtocols);
        addNotNull(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
        addNotNull(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
        addNotNull(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        addNotNull(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        addNotNull(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreType);
        addNotNull(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
        addNotNull(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        addNotNull(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, sslKeymanagerAlgorithm);
        addNotNull(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, sslTrustmanagerAlgorithm);
        addNotNull(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
        addNotNull(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, sslSecureRandomImplementation);
        return props;
    }

    private void addNotNull(Object key, Object value)
    {
        if (value != null) {
            props.put(key, value);
        }
    }
}
