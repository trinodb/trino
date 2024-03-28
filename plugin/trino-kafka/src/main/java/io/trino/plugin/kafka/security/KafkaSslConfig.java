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
package io.trino.plugin.kafka.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.trino.plugin.base.ssl.SslTrustConfig;
import io.trino.plugin.base.ssl.TruststoreType;
import jakarta.annotation.PostConstruct;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.ssl.TruststoreType.JKS;
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.HTTPS;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

/**
 *  Manages Kafka SSL authentication and encryption between clients and brokers.
 */
public class KafkaSslConfig
        extends SslTrustConfig
{
    private String keyPassword;
    private TruststoreType truststoreType = JKS;
    private TruststoreType keystoreType = JKS;
    private KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm = HTTPS;

    public Optional<String> getKeyPassword()
    {
        return Optional.ofNullable(keyPassword);
    }

    @Config("key-password")
    @LegacyConfig("key.password")
    @ConfigDescription("The password of the private key in the key store file")
    @ConfigSecuritySensitive
    public KafkaSslConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public Optional<TruststoreType> getKeystoreType()
    {
        return Optional.ofNullable(keystoreType);
    }

    @Config("keystore-type")
    @LegacyConfig("keystore.type")
    public KafkaSslConfig setKeystoreType(TruststoreType keystoreType)
    {
        this.keystoreType = keystoreType;
        return this;
    }

    public Optional<TruststoreType> getTruststoreType()
    {
        return Optional.ofNullable(truststoreType);
    }

    @Config("truststore-type")
    @LegacyConfig("truststore.type")
    @ConfigDescription("The file format of the trust store file")
    public KafkaSslConfig setTruststoreType(TruststoreType truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public Optional<KafkaEndpointIdentificationAlgorithm> getEndpointIdentificationAlgorithm()
    {
        return Optional.ofNullable(endpointIdentificationAlgorithm);
    }

    @Config("endpoint-identification-algorithm")
    @ConfigDescription("The endpoint identification algorithm to validate server hostname using server certificate")
    public KafkaSslConfig setEndpointIdentificationAlgorithm(KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm)
    {
        this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        return this;
    }

    public Map<String, Object> getKafkaClientProperties()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        getKeystorePassword().ifPresent(v -> properties.put(SSL_KEYSTORE_PASSWORD_CONFIG, v));
        getKeystoreType().ifPresent(v -> properties.put(SSL_KEYSTORE_TYPE_CONFIG, v.name()));
        getKeystorePath().ifPresent(v -> properties.put(SSL_KEYSTORE_LOCATION_CONFIG, v.getPath()));
        getTruststorePath().ifPresent(v -> properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, v.getPath()));
        getTruststorePassword().ifPresent(v -> properties.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, v));
        getTruststoreType().ifPresent(v -> properties.put(SSL_TRUSTSTORE_TYPE_CONFIG, v.name()));
        getKeyPassword().ifPresent(v -> properties.put(SSL_KEY_PASSWORD_CONFIG, v));
        getEndpointIdentificationAlgorithm().ifPresent(v -> properties.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, v.getValue()));
        properties.put(SECURITY_PROTOCOL_CONFIG, SSL.name());

        return properties.buildOrThrow();
    }

    @PostConstruct
    public void validate()
    {
        if (getKeystorePath().isPresent() && getKeystorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("kafka.ssl.keystore.password must set when kafka.ssl.keystore.location is given")));
        }
        if (getTruststorePath().isPresent() && getTruststorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("kafka.ssl.truststore.password must set when kafka.ssl.truststore.location is given")));
        }
    }
}
