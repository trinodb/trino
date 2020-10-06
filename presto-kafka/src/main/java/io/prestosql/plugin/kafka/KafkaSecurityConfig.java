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
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 *  {@KafkaSecurityConfig} manages Kafka configuration related to connection security. I.e. if the security protocol
 *  'SSL' is being used. If so, additional properties might be supplied, i.e. location and password of the truststore
 *  and keystore to be used.
 */
public class KafkaSecurityConfig
{
    private Properties props;

    private String sslKeystoreFile;
    private String sslKeystorePassword;
    private String sslKeyPassword;
    private String sslTruststoreFile;
    private String sslTruststorePassword;
    private KafkaEndpointIdentificationAlgorithm sslEndpointIdentificationAlgorithm;

    @Config("kafka.keystore-file")
    @LegacyConfig("kafka.ssl.keystore.location")
    @ConfigDescription(SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
    public void setSslKeystoreFile(String sslKeystoreFile)
    {
        this.sslKeystoreFile = sslKeystoreFile;
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
    public void setSslKeystorePassword(String sslKeystorePassword)
    {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeystorePassword()
    {
        return sslKeystorePassword;
    }

    @Config("kafka.key-password")
    @LegacyConfig("kafka.ssl.key.password")
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

    @Config("kafka.truststore-file")
    @LegacyConfig("kafka.ssl.truststore.location")
    @ConfigDescription(SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
    public void setSslTruststoreFile(String sslTruststoreFile)
    {
        this.sslTruststoreFile = sslTruststoreFile;
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
    public void setSslTruststorePassword(String sslTruststorePassword)
    {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststorePassword()
    {
        return sslTruststorePassword;
    }

    @Config("kafka.endpoint-identification-algorithm")
    @ConfigDescription(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm)
    {
        if (sslEndpointIdentificationAlgorithm.equals("https")) {
            this.sslEndpointIdentificationAlgorithm = KafkaEndpointIdentificationAlgorithm.HTTPS;
        }
        else if (sslEndpointIdentificationAlgorithm.equals("")) {
            this.sslEndpointIdentificationAlgorithm = KafkaEndpointIdentificationAlgorithm.DEACTIVATED;
        }
    }

    public KafkaEndpointIdentificationAlgorithm getSslEndpointIdentificationAlgorithm()
    {
        return sslEndpointIdentificationAlgorithm;
    }

    public Properties getKafkaClientProperties()
    {
        props = new Properties();
        addNotNull(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreFile);
        addNotNull(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        addNotNull(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        addNotNull(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreFile);
        addNotNull(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        addNotNull(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
        return props;
    }

    private void addNotNull(Object key, Object value)
    {
        if (value != null) {
            props.put(key, value.toString());
        }
    }
}
