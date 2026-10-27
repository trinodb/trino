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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm;
import io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType;
import jakarta.validation.constraints.AssertTrue;

import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.HTTPS;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.JKS;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

/**
 * Credentials and TLS settings for the connection to the Confluent schema registry.
 */
public class ConfluentSchemaRegistrySecurityConfig
{
    private static final String USER_INFO_CREDENTIALS_SOURCE = "USER_INFO";

    private String basicAuthUser;
    private String basicAuthPassword;
    private String keystoreLocation;
    private String keystorePassword;
    private KafkaKeystoreTruststoreType keystoreType = JKS;
    private String truststoreLocation;
    private String truststorePassword;
    private KafkaKeystoreTruststoreType truststoreType = JKS;
    private String keyPassword;
    private KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm = HTTPS;

    public Optional<String> getBasicAuthUser()
    {
        return Optional.ofNullable(basicAuthUser);
    }

    @Config("kafka.confluent-schema-registry.basic-auth.user")
    @ConfigDescription("User name for HTTP basic authentication to the schema registry")
    public ConfluentSchemaRegistrySecurityConfig setBasicAuthUser(String basicAuthUser)
    {
        this.basicAuthUser = basicAuthUser;
        return this;
    }

    public Optional<String> getBasicAuthPassword()
    {
        return Optional.ofNullable(basicAuthPassword);
    }

    @Config("kafka.confluent-schema-registry.basic-auth.password")
    @ConfigDescription("Password for HTTP basic authentication to the schema registry")
    @ConfigSecuritySensitive
    public ConfluentSchemaRegistrySecurityConfig setBasicAuthPassword(String basicAuthPassword)
    {
        this.basicAuthPassword = basicAuthPassword;
        return this;
    }

    public Optional<@FileExists String> getKeystoreLocation()
    {
        return Optional.ofNullable(keystoreLocation);
    }

    @Config("kafka.confluent-schema-registry.ssl.keystore.location")
    @ConfigDescription("The location of the key store file used to authenticate to the schema registry")
    public ConfluentSchemaRegistrySecurityConfig setKeystoreLocation(String keystoreLocation)
    {
        this.keystoreLocation = keystoreLocation;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("kafka.confluent-schema-registry.ssl.keystore.password")
    @ConfigDescription("The store password for the key store file")
    @ConfigSecuritySensitive
    public ConfluentSchemaRegistrySecurityConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public KafkaKeystoreTruststoreType getKeystoreType()
    {
        return keystoreType;
    }

    @Config("kafka.confluent-schema-registry.ssl.keystore.type")
    @ConfigDescription("The file format of the key store file")
    public ConfluentSchemaRegistrySecurityConfig setKeystoreType(KafkaKeystoreTruststoreType keystoreType)
    {
        this.keystoreType = keystoreType;
        return this;
    }

    public Optional<@FileExists String> getTruststoreLocation()
    {
        return Optional.ofNullable(truststoreLocation);
    }

    @Config("kafka.confluent-schema-registry.ssl.truststore.location")
    @ConfigDescription("The location of the trust store file used to verify the schema registry certificate")
    public ConfluentSchemaRegistrySecurityConfig setTruststoreLocation(String truststoreLocation)
    {
        this.truststoreLocation = truststoreLocation;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("kafka.confluent-schema-registry.ssl.truststore.password")
    @ConfigDescription("The password for the trust store file")
    @ConfigSecuritySensitive
    public ConfluentSchemaRegistrySecurityConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public KafkaKeystoreTruststoreType getTruststoreType()
    {
        return truststoreType;
    }

    @Config("kafka.confluent-schema-registry.ssl.truststore.type")
    @ConfigDescription("The file format of the trust store file")
    public ConfluentSchemaRegistrySecurityConfig setTruststoreType(KafkaKeystoreTruststoreType truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public Optional<String> getKeyPassword()
    {
        return Optional.ofNullable(keyPassword);
    }

    @Config("kafka.confluent-schema-registry.ssl.key.password")
    @ConfigDescription("The password of the private key in the key store file")
    @ConfigSecuritySensitive
    public ConfluentSchemaRegistrySecurityConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public KafkaEndpointIdentificationAlgorithm getEndpointIdentificationAlgorithm()
    {
        return endpointIdentificationAlgorithm;
    }

    @Config("kafka.confluent-schema-registry.ssl.endpoint-identification-algorithm")
    @ConfigDescription("The endpoint identification algorithm used to validate the schema registry host name against its certificate")
    public ConfluentSchemaRegistrySecurityConfig setEndpointIdentificationAlgorithm(KafkaEndpointIdentificationAlgorithm endpointIdentificationAlgorithm)
    {
        this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        return this;
    }

    /**
     * The properties, in the schema registry client's own vocabulary, that apply this configuration.
     */
    public Map<String, Object> getSchemaRegistryClientProperties()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        getBasicAuthUser().ifPresent(user -> {
            properties.put(BASIC_AUTH_CREDENTIALS_SOURCE, USER_INFO_CREDENTIALS_SOURCE);
            properties.put(USER_INFO_CONFIG, user + ":" + getBasicAuthPassword().orElse(""));
        });
        getKeystoreLocation().ifPresent(location -> {
            properties.put(CLIENT_NAMESPACE + SSL_KEYSTORE_LOCATION_CONFIG, location);
            properties.put(CLIENT_NAMESPACE + SSL_KEYSTORE_TYPE_CONFIG, keystoreType.name());
        });
        getKeystorePassword().ifPresent(password -> properties.put(CLIENT_NAMESPACE + SSL_KEYSTORE_PASSWORD_CONFIG, password));
        getKeyPassword().ifPresent(password -> properties.put(CLIENT_NAMESPACE + SSL_KEY_PASSWORD_CONFIG, password));
        getTruststoreLocation().ifPresent(location -> {
            properties.put(CLIENT_NAMESPACE + SSL_TRUSTSTORE_LOCATION_CONFIG, location);
            properties.put(CLIENT_NAMESPACE + SSL_TRUSTSTORE_TYPE_CONFIG, truststoreType.name());
        });
        getTruststorePassword().ifPresent(password -> properties.put(CLIENT_NAMESPACE + SSL_TRUSTSTORE_PASSWORD_CONFIG, password));
        // The client skips host name verification unless the algorithm is set explicitly, so it is always passed
        properties.put(CLIENT_NAMESPACE + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, endpointIdentificationAlgorithm.getValue());
        return properties.buildOrThrow();
    }

    @AssertTrue(message = "kafka.confluent-schema-registry.basic-auth.user and kafka.confluent-schema-registry.basic-auth.password must be set together")
    public boolean isBasicAuthComplete()
    {
        return getBasicAuthUser().isPresent() == getBasicAuthPassword().isPresent();
    }

    @AssertTrue(message = "kafka.confluent-schema-registry.basic-auth.user must not contain ':'")
    public boolean isBasicAuthUserValid()
    {
        return getBasicAuthUser().map(user -> !user.contains(":")).orElse(true);
    }

    @AssertTrue(message = "kafka.confluent-schema-registry.ssl.keystore.password must be set when kafka.confluent-schema-registry.ssl.keystore.location is given")
    public boolean isKeystorePasswordValid()
    {
        return getKeystoreLocation().isEmpty() || getKeystorePassword().isPresent();
    }

    @AssertTrue(message = "kafka.confluent-schema-registry.ssl.truststore.password must be set when kafka.confluent-schema-registry.ssl.truststore.location is given")
    public boolean isTruststorePasswordValid()
    {
        return getTruststoreLocation().isEmpty() || getTruststorePassword().isPresent();
    }

    // the client fails to start on a store password without the store, and ignores a key password without a key store
    @AssertTrue(message = "kafka.confluent-schema-registry.ssl.keystore.location must be set when kafka.confluent-schema-registry.ssl.keystore.password or kafka.confluent-schema-registry.ssl.key.password is given")
    public boolean isKeystoreLocationValid()
    {
        return getKeystoreLocation().isPresent() || (getKeystorePassword().isEmpty() && getKeyPassword().isEmpty());
    }

    @AssertTrue(message = "kafka.confluent-schema-registry.ssl.truststore.location must be set when kafka.confluent-schema-registry.ssl.truststore.password is given")
    public boolean isTruststoreLocationValid()
    {
        return getTruststoreLocation().isPresent() || getTruststorePassword().isEmpty();
    }
}
