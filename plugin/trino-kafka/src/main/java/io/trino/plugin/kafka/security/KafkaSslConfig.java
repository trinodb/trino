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
import io.airlift.configuration.validation.FileExists;

import javax.annotation.PostConstruct;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.HTTPS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.JKS;
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
{
    private String keystoreLocation;
    private String keystorePassword;
    private KafkaKeystoreTruststoreType keystoreType = JKS;
    private String truststoreLocation;
    private String truststorePassword;
    private KafkaKeystoreTruststoreType truststoreType = JKS;
    private String keyPassword;
    private KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm = HTTPS;

    public Optional<@FileExists String> getKeystoreLocation()
    {
        return Optional.ofNullable(keystoreLocation);
    }

    @Config("kafka.ssl.keystore.location")
    @ConfigDescription("The location of the key store file. This can be used for two-way authentication for client")
    public KafkaSslConfig setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("kafka.ssl.keystore.password")
    @ConfigDescription("The store password for the key store file")
    @ConfigSecuritySensitive
    public KafkaSslConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public Optional<KafkaKeystoreTruststoreType> getKeystoreType()
    {
        return Optional.ofNullable(keystoreType);
    }

    @Config("kafka.ssl.keystore.type")
    @ConfigDescription("The file format of the key store file")
    public KafkaSslConfig setKeystoreType(KafkaKeystoreTruststoreType keystoreType)
    {
        this.keystoreType = keystoreType;
        return this;
    }

    public Optional<@FileExists String> getTruststoreLocation()
    {
        return Optional.ofNullable(truststoreLocation);
    }

    @Config("kafka.ssl.truststore.location")
    @ConfigDescription("The location of the trust store file")
    public KafkaSslConfig setTruststoreLocation(String truststoreLocation)
    {
        this.truststoreLocation = truststoreLocation;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("kafka.ssl.truststore.password")
    @ConfigDescription("The password for the trust store file")
    @ConfigSecuritySensitive
    public KafkaSslConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public Optional<KafkaKeystoreTruststoreType> getTruststoreType()
    {
        return Optional.ofNullable(truststoreType);
    }

    @Config("kafka.ssl.truststore.type")
    @ConfigDescription("The file format of the trust store file")
    public KafkaSslConfig setTruststoreType(KafkaKeystoreTruststoreType truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public Optional<String> getKeyPassword()
    {
        return Optional.ofNullable(keyPassword);
    }

    @Config("kafka.ssl.key.password")
    @ConfigDescription("The password of the private key in the key store file")
    @ConfigSecuritySensitive
    public KafkaSslConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public Optional<KafkaEndpointIdentificationAlgorithm> getEndpointIdentificationAlgorithm()
    {
        return Optional.ofNullable(endpointIdentificationAlgorithm);
    }

    @Config("kafka.ssl.endpoint-identification-algorithm")
    @ConfigDescription("The endpoint identification algorithm to validate server hostname using server certificate")
    public KafkaSslConfig setEndpointIdentificationAlgorithm(KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm)
    {
        this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        return this;
    }

    public Map<String, Object> getKafkaClientProperties()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        getKeystoreLocation().ifPresent(v -> properties.put(SSL_KEYSTORE_LOCATION_CONFIG, v));
        getKeystorePassword().ifPresent(v -> properties.put(SSL_KEYSTORE_PASSWORD_CONFIG, v));
        getKeystoreType().ifPresent(v -> properties.put(SSL_KEYSTORE_TYPE_CONFIG, v.name()));
        getTruststoreLocation().ifPresent(v -> properties.put(SSL_TRUSTSTORE_LOCATION_CONFIG, v));
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
        if (getKeystoreLocation().isPresent() && getKeystorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("kafka.ssl.keystore.password must set when kafka.ssl.keystore.location is given")));
        }
        if (getTruststoreLocation().isPresent() && getTruststorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("kafka.ssl.truststore.password must set when kafka.ssl.truststore.location is given")));
        }
    }
}
