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

package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.Environment;
import org.testcontainers.containers.BindMode;

import javax.inject.Inject;

import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaSsl
        implements EnvironmentExtender
{
    private final Kafka kafka;

    @Inject
    public KafkaSsl(Kafka kafka)
    {
        this.kafka = requireNonNull(kafka, "kafka is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(Kafka.KAFKA, container -> container
                .withStartupAttempts(3)
                .withStartupTimeout(Duration.ofMinutes(5))
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "SSL://kafka:9092")
                .withEnv("KAFKA_SSL_KEYSTORE_FILENAME", "kafka.broker1.keystore")
                .withEnv("KAFKA_SSL_KEYSTORE_CREDENTIALS", "broker1_keystore_creds")
                .withEnv("KAFKA_SSL_KEYSTORE_TYPE", "JKS")
                .withEnv("KAFKA_SSL_KEY_CREDENTIALS", "broker1_sslkey_creds")
                .withEnv("KAFKA_SSL_TRUSTSTORE_FILENAME", "kafka.broker1.truststore")
                .withEnv("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "broker1_truststore_creds")
                .withEnv("KAFKA_SSL_TRUSTSTORE_TYPE", "JKS")
                .withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https")
                .withEnv("KAFKA_SSL_CLIENT_AUTH", "required")
                .withEnv("KAFKA_SECURITY_INTER_BROKER_PROTOCOL", "SSL")
                .withEnv("KAFKA_SECURITY_PROTOCOL", "SSL")
                .withClasspathResourceMapping("docker/presto-product-tests/conf/environment/multinode-kafka-ssl/secrets", "/etc/kafka/secrets", BindMode.READ_ONLY));
        builder.configureContainer(Kafka.SCHEMA_REGISTRY, container -> container
                .withStartupAttempts(3)
                .withStartupTimeout(Duration.ofMinutes(5))
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "SSL://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SSL")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_LOCATION", "/var/private/ssl/kafka.client.keystore")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_PASSWORD", "confluent")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_TYPE", "JKS")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION", "/var/private/ssl/kafka.client.truststore")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD", "confluent")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE", "JKS")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEY_PASSWORD", "confluent")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SSL_CLIENT_AUTH", "requested")
                .withEnv("SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION", "/var/private/ssl/kafka.client.keystore")
                .withEnv("SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD", "confluent")
                .withEnv("SCHEMA_REGISTRY_SSL_KEYSTORE_TYPE", "JKS")
                .withEnv("SCHEMA_REGISTRY_SSL_KEY_PASSWORD", "confluent")
                .withEnv("SCHEMA_REGISTRY_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https")
                .withClasspathResourceMapping("docker/presto-product-tests/conf/environment/multinode-kafka-ssl/secrets", "/var/private/ssl", BindMode.READ_ONLY));
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(kafka);
    }
}
