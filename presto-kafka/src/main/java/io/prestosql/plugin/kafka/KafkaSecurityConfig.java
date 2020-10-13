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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;

/**
 *  {@KafkaSecurityConfig} manages Kafka configuration related to connection security. I.e. if the security protocol
 *  'SSL' is being used. If so, additional properties might be supplied, i.e. location and password of the truststore
 *  and keystore to be used.
 */
public class KafkaSecurityConfig
{
    private String sslKeystoreFile;
    private String sslKeystorePassword;
    private String sslKeyPassword;
    private String sslTruststoreFile;
    private String sslTruststorePassword;
    private KafkaEndpointIdentificationAlgorithm sslEndpointIdentificationAlgorithm;

    @Config("kafka.keystore-file")
    @LegacyConfig("kafka.ssl.keystore.location")
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
    public KafkaSecurityConfig setSslKeystoreFile(String sslKeystoreFile)
    {
        this.sslKeystoreFile = sslKeystoreFile;
        return this;
    }

    @FileExists
    public String getSslKeystoreFile()
    {
        return sslKeystoreFile;
    }

    @Config("kafka.keystore-password")
    @LegacyConfig("kafka.ssl.keystore.password")
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public KafkaSecurityConfig setSslKeystorePassword(String sslKeystorePassword)
    {
        this.sslKeystorePassword = sslKeystorePassword;
        return this;
    }

    public String getSslKeystorePassword()
    {
        return sslKeystorePassword;
    }

    @Config("kafka.key-password")
    @LegacyConfig("kafka.ssl.key.password")
    @ConfigDescription(SslConfigs.SSL_KEY_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public KafkaSecurityConfig setSslKeyPassword(String sslKeyPassword)
    {
        this.sslKeyPassword = sslKeyPassword;
        return this;
    }

    public String getSslKeyPassword()
    {
        return sslKeyPassword;
    }

    @Config("kafka.truststore-file")
    @LegacyConfig("kafka.ssl.truststore.location")
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
    public KafkaSecurityConfig setSslTruststoreFile(String sslTruststoreFile)
    {
        this.sslTruststoreFile = sslTruststoreFile;
        return this;
    }

    @FileExists
    public String getSslTruststoreFile()
    {
        return sslTruststoreFile;
    }

    @Config("kafka.truststore-password")
    @LegacyConfig("kafka.ssl.truststore.password")
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
    @ConfigSecuritySensitive
    public KafkaSecurityConfig setSslTruststorePassword(String sslTruststorePassword)
    {
        this.sslTruststorePassword = sslTruststorePassword;
        return this;
    }

    public String getSslTruststorePassword()
    {
        return sslTruststorePassword;
    }

    @Config("kafka.endpoint-identification-algorithm")
    @ConfigDescription(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
    public KafkaSecurityConfig setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm)
    {
        this.sslEndpointIdentificationAlgorithm = KafkaEndpointIdentificationAlgorithm.fromString(sslEndpointIdentificationAlgorithm);
        return this;
    }

    public KafkaEndpointIdentificationAlgorithm getSslEndpointIdentificationAlgorithm()
    {
        return sslEndpointIdentificationAlgorithm;
    }

    public Map<String, Object> getKafkaClientProperties()
    {
        ImmutableMap.Builder<String, Object> props = ImmutableMap.builder();
        addNotNull(props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreFile);
        addNotNull(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        addNotNull(props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        addNotNull(props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreFile);
        addNotNull(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);

        addNotNull(props, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
        return props.build();
    }

    private void addNotNull(ImmutableMap.Builder<String, Object> props, String key, Object value)
    {
        if (value != null) {
            props.put(key, value.toString());
        }
    }
}
