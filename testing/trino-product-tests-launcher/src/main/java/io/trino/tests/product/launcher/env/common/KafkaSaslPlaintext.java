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

public class KafkaSaslPlaintext
        implements EnvironmentExtender
{
    private final Kafka kafka;

    @Inject
    public KafkaSaslPlaintext(Kafka kafka)
    {
        this.kafka = requireNonNull(kafka, "kafka is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(Kafka.KAFKA, container -> container
                .withStartupAttempts(3)
                .withStartupTimeout(Duration.ofMinutes(5))
                .withEnv("KAFKA_LISTENERS", "SASL_PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "SASL_PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_SECURITY_INTER_BROKER_PROTOCOL", "SASL_PLAINTEXT")
                .withEnv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_server_jaas.conf")
                .withEnv("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"admin\" " +
                        "password=\"admin-secret\" " +
                        "user_admin=\"admin-secret\";")
                .withEnv("ZOOKEEPER_SASL_ENABLED", "false")
                .withClasspathResourceMapping("docker/presto-product-tests/conf/environment/multinode-kafka-sasl-plaintext/kafka_server_jaas.conf", "/tmp/kafka_server_jaas.conf", BindMode.READ_ONLY));
        builder.configureContainer(Kafka.SCHEMA_REGISTRY, container -> container
                .withStartupAttempts(3)
                .withStartupTimeout(Duration.ofMinutes(5))
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "SASL_PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "PLAIN")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"admin\" " +
                        "password=\"admin-secret\";")
                .withClasspathResourceMapping("docker/presto-product-tests/conf/environment/multinode-kafka-sasl-plaintext/kafka_server_jaas.conf", "/tmp/kafka_server_jaas.conf", BindMode.READ_ONLY));
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(kafka);
    }
}
