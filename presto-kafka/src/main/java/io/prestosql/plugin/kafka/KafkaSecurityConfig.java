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

import io.airlift.log.Logger;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Properties;

/**
 *
 */
public class KafkaSecurityConfig
{
    protected static final String MESSAGE_TEMPLATE_CONFIG_NOT_USED = "Config '%s' won't be used when 'security.protocol' is set to PLAINTEXT (default value)!";
    Logger logger = Logger.get(KafkaSecurityConfig.class);

    private Properties props;

    private SecurityProtocol securityProtocol;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslEndpointIdentificationAlgorithm;
    private String sslKeyPassword;
    private String sslProvider;
    private String sslKeystoreType;
    private String sslTruststoreType;

    public void setSecurityProtocol(SecurityProtocol securityProtocol)
    {
        this.securityProtocol = securityProtocol;
    }

    public SecurityProtocol getSecurityProtocol()
    {
        return securityProtocol;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation)
    {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststoreLocation()
    {
        return sslTruststoreLocation;
    }

    public void setSslTruststorePassword(String sslTruststorePassword)
    {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststorePassword()
    {
        return sslTruststorePassword;
    }

    public void setSslTruststoreType(String sslTruststoreType)
    {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslTruststoreType()
    {
        return sslTruststoreType;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation)
    {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystoreLocation()
    {
        return sslKeystoreLocation;
    }

    public void setSslKeystorePassword(String sslKeystorePassword)
    {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeystorePassword()
    {
        return sslKeystorePassword;
    }

    public void setSslKeystoreType(String sslKeystoreType)
    {
        this.sslKeystoreType = sslKeystoreType;
    }

    public String getSslKeystoreType()
    {
        return sslKeystoreType;
    }

    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm)
    {
        this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
    }

    public String getSslEndpointIdentificationAlgorithm()
    {
        return sslEndpointIdentificationAlgorithm;
    }

    public void setSslKeyPassword(String sslKeyPassword)
    {
        this.sslKeyPassword = sslKeyPassword;
    }

    public String getSslKeyPassword()
    {
        return sslKeyPassword;
    }

    public void setSslProvider(String sslProvider)
    {
        this.sslProvider = sslProvider;
    }

    public String getSslProvider()
    {
        return sslProvider;
    }

    public Properties getKafkaSecurityConfig()
    {
        props = new Properties();

        if (isNotNullOrPlaintext(securityProtocol)) {
            addNotNull("security.protocol", securityProtocol.name);
            addNotNull(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
            addNotNull(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
            addNotNull(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreType);
            addNotNull(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
            addNotNull(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
            addNotNull(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
            addNotNull(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
            addNotNull(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
            addNotNull(SslConfigs.SSL_PROVIDER_CONFIG, sslProvider);
        }
        checkUnusedConfigsWhenPlaintext();
        return props;
    }

    private boolean isNotNullOrPlaintext(SecurityProtocol inp)
    {
        if (inp == null) {
            return false;
        }
        if (inp.equals(SecurityProtocol.PLAINTEXT)) {
            return false;
        }
        return true;
    }

    private void checkUnusedConfigsWhenPlaintext()
    {
        // Additional configs only apply when security protocol is not PLAINTEXT
        if (securityProtocol == null || securityProtocol.equals(SecurityProtocol.PLAINTEXT)) {
            if (isSet(sslTruststoreLocation)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.truststore.location"));
            }
            if (isSet(sslTruststorePassword)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.truststore.password"));
            }
            if (isSet(sslTruststoreType)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.truststore.type"));
            }
            if (isSet(sslKeystoreLocation)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.keystore.location"));
            }
            if (isSet(sslKeystorePassword)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.keystore.password"));
            }
            if (isSet(sslKeystoreType)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "kafka.ssl.keystore.type"));
            }
            if (isSet(sslEndpointIdentificationAlgorithm)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "ssl.endpoint.identification.algorithm"));
            }
            if (isSet(sslKeyPassword)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "ssl.key.password"));
            }
            if (isSet(sslProvider)) {
                logger.warn(String.format(MESSAGE_TEMPLATE_CONFIG_NOT_USED, "ssl.provider"));
            }
        }
    }

    private boolean isSet(Object property)
    {
        if (property == null) {
            return false;
        }
        return true;
    }

    private void addNotNull(Object key, Object value)
    {
        if (value != null) {
            props.put(key, value);
        }
    }
}
