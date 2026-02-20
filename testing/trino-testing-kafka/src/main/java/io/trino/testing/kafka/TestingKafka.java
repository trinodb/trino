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
package io.trino.testing.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class TestingKafka
        implements Closeable
{
    private static final Logger log = Logger.get(TestingKafka.class);

    private static final String DEFAULT_CONFLUENT_PLATFORM_VERSION = "8.1.1";
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private static final int KAFKA_PORT = 9092;
    private static final int KAFKA_INTERNAL_BROKER_PORT = 9093;
    private static final int KAFKA_CONTROLLER_PORT = 9094;
    private static final String KAFKA_STARTER_SCRIPT = "/tmp/testcontainers_start.sh";

    private static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-schema-registry");

    private enum SecurityMode
    {
        PLAINTEXT,
        SSL,
        SASL_PLAINTEXT
    }

    private final Network network;
    private final GenericContainer<?> kafka;
    private final GenericContainer<?> schemaRegistry;
    private final boolean withSchemaRegistry;
    private final SecurityMode securityMode;
    private final Closer closer = Closer.create();
    private boolean stopped;
    private Path clientKeystorePath;
    private Path clientTruststorePath;

    public static TestingKafka create()
    {
        return create(DEFAULT_CONFLUENT_PLATFORM_VERSION);
    }

    public static TestingKafka create(String confluentPlatformVersions)
    {
        return new TestingKafka(confluentPlatformVersions, false, SecurityMode.PLAINTEXT);
    }

    public static TestingKafka createWithSchemaRegistry()
    {
        return new TestingKafka(DEFAULT_CONFLUENT_PLATFORM_VERSION, true, SecurityMode.PLAINTEXT);
    }

    public static TestingKafka createSsl()
    {
        return new TestingKafka(DEFAULT_CONFLUENT_PLATFORM_VERSION, false, SecurityMode.SSL);
    }

    public static TestingKafka createSaslPlaintext()
    {
        return new TestingKafka(DEFAULT_CONFLUENT_PLATFORM_VERSION, false, SecurityMode.SASL_PLAINTEXT);
    }

    private TestingKafka(String confluentPlatformVersion, boolean withSchemaRegistry, SecurityMode securityMode)
    {
        this.withSchemaRegistry = withSchemaRegistry;
        this.securityMode = securityMode;
        network = Network.newNetwork();
        closer.register(network::close);

        // Confluent Docker entry point script overwrites /etc/kafka/log4j.properties
        // Modify the template directly instead.
        MountableFile kafkaLogTemplate = forClasspathResource("log4j-kafka.properties.template");
        MountableFile schemaRegistryLogTemplate = forClasspathResource("log4j-schema-registry.properties.template");
        if (securityMode == SecurityMode.PLAINTEXT) {
            kafka = new ConfluentKafkaContainer(KAFKA_IMAGE_NAME.withTag(confluentPlatformVersion))
                    .withStartupAttempts(3)
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withEnv("CLUSTER_ID", "test-cluster-" + UUID.randomUUID().toString().replaceAll("-", ""))
                    .withCopyFileToContainer(
                            kafkaLogTemplate,
                            "/etc/confluent/docker/log4j.properties.template");
        }
        else {
            kafka = new SecureKafkaContainer(KAFKA_IMAGE_NAME.withTag(confluentPlatformVersion), securityMode)
                    .withStartupAttempts(3)
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withEnv("CLUSTER_ID", "test-cluster-" + UUID.randomUUID().toString().replaceAll("-", ""))
                    .withCopyFileToContainer(
                            kafkaLogTemplate,
                            "/etc/confluent/docker/log4j.properties.template");
        }
        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE_NAME.withTag(confluentPlatformVersion))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_HEAP_OPTS", "-Xmx1G")
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .withCopyFileToContainer(
                        schemaRegistryLogTemplate,
                        "/etc/confluent/docker/log4j.properties.template")
                .dependsOn(kafka);

        configureSecurity();

        closer.register(kafka::stop);
        closer.register(schemaRegistry::stop);

        try {
            // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
            //noinspection resource
            PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            kafka.withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", "kafka")));
            schemaRegistry.withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", "schema-registry")));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void start()
    {
        checkState(!stopped, "Cannot start again");
        kafka.start();
        initializeBrokerSecurityConfig();
        if (withSchemaRegistry) {
            schemaRegistry.start();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
        deleteIfExists(clientKeystorePath);
        deleteIfExists(clientTruststorePath);
        stopped = true;
    }

    public void createTopic(String topic)
    {
        createTopic(2, 1, topic);
    }

    private void createTopic(int partitions, int replication, String topic)
    {
        List<String> args = new ArrayList<>(List.of(
                "--create",
                "--if-not-exists",
                "--bootstrap-server", "localhost:9093",
                "--topic", topic,
                "--partitions", Integer.toString(partitions),
                "--replication-factor", Integer.toString(replication)));
        if (securityMode != SecurityMode.PLAINTEXT) {
            args.add("--command-config");
            args.add("/tmp/admin-client.properties");
        }
        runKafkaTopicsCommand(args.toArray(String[]::new));
    }

    public void createTopicWithConfig(int partitions, int replication, String topic, boolean enableLogAppendTime)
    {
        List<String> args = new ArrayList<>(List.of(
                "--create",
                "--if-not-exists",
                "--bootstrap-server", "localhost:9093",
                "--topic", topic,
                "--partitions", Integer.toString(partitions),
                "--replication-factor", Integer.toString(replication)));
        if (securityMode != SecurityMode.PLAINTEXT) {
            args.add("--command-config");
            args.add("/tmp/admin-client.properties");
        }
        if (enableLogAppendTime) {
            args.add("--config");
            args.add("message.timestamp.type=LogAppendTime");
        }
        runKafkaTopicsCommand(args.toArray(String[]::new));
    }

    private void runKafkaTopicsCommand(String... args)
    {
        try {
            List<String> command = new ArrayList<>(List.of("kafka-topics"));
            command.addAll(List.of(args));
            ExecResult result = kafka.execInContainer(command.toArray(String[]::new));
            checkState(
                    result.getExitCode() == 0,
                    "kafka-topics command failed (exit=%s): %s%nstdout:%n%s%nstderr:%n%s",
                    result.getExitCode(),
                    join(" ", command),
                    result.getStdout(),
                    result.getStderr());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to run kafka-topics command", e);
        }
    }

    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream)
    {
        return sendMessages(recordStream, ImmutableMap.of());
    }

    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream, Map<String, String> extraProducerProperties)
    {
        try (KafkaProducer<K, V> producer = createProducer(extraProducerProperties)) {
            Future<RecordMetadata> future = recordStream.map(record -> send(producer, record))
                    .reduce((first, second) -> second)
                    .orElseGet(() -> Futures.immediateFuture(null));
            producer.flush();
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private <K, V> Future<RecordMetadata> send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record)
    {
        return Failsafe.with(
                RetryPolicy.builder()
                        .onRetry(event -> log.warn(event.getLastException(), "Retrying message send"))
                        .withMaxAttempts(10)
                        .withBackoff(1, 10_000, MILLIS)
                        .build())
                .get(() -> producer.send(record));
    }

    public String getConnectString()
    {
        return kafka.getHost() + ":" + kafka.getMappedPort(KAFKA_PORT);
    }

    private <K, V> KafkaProducer<K, V> createProducer(Map<String, String> extraProperties)
    {
        Map<String, String> properties = new HashMap<>(extraProperties);

        properties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, NumberPartitioner.class.getName());
        properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        if (securityMode == SecurityMode.SSL) {
            properties.putIfAbsent("security.protocol", "SSL");
            properties.putIfAbsent("ssl.keystore.type", "PKCS12");
            properties.putIfAbsent("ssl.keystore.location", clientKeystorePath.toString());
            properties.putIfAbsent("ssl.keystore.password", "confluent");
            properties.putIfAbsent("ssl.truststore.type", "PKCS12");
            properties.putIfAbsent("ssl.truststore.location", clientTruststorePath.toString());
            properties.putIfAbsent("ssl.truststore.password", "confluent");
            properties.putIfAbsent("ssl.key.password", "confluent");
            properties.putIfAbsent("ssl.endpoint.identification.algorithm", "");
        }
        if (securityMode == SecurityMode.SASL_PLAINTEXT) {
            properties.putIfAbsent("security.protocol", "SASL_PLAINTEXT");
            properties.putIfAbsent("sasl.mechanism", "PLAIN");
            properties.putIfAbsent("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");
        }

        return new KafkaProducer<>(toProperties(properties));
    }

    private void configureSecurity()
    {
        switch (securityMode) {
            case PLAINTEXT -> {
                return;
            }
            case SSL -> {
                kafka.withEnv("KAFKA_SSL_KEYSTORE_FILENAME", "kafka.broker1.keystore")
                        .withEnv("KAFKA_SSL_KEYSTORE_CREDENTIALS", "broker1_keystore_creds")
                        .withEnv("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12")
                        .withEnv("KAFKA_SSL_KEY_CREDENTIALS", "broker1_sslkey_creds")
                        .withEnv("KAFKA_SSL_TRUSTSTORE_FILENAME", "kafka.broker1.truststore")
                        .withEnv("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "broker1_truststore_creds")
                        .withEnv("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12")
                        .withEnv("KAFKA_SSL_CLIENT_AUTH", "requested")
                        .withEnv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")
                        .withCopyFileToContainer(forClasspathResource("kafka/ssl/secrets/broker1_keystore_creds"), "/etc/kafka/secrets/broker1_keystore_creds")
                        .withCopyFileToContainer(forClasspathResource("kafka/ssl/secrets/broker1_sslkey_creds"), "/etc/kafka/secrets/broker1_sslkey_creds")
                        .withCopyFileToContainer(forClasspathResource("kafka/ssl/secrets/broker1_truststore_creds"), "/etc/kafka/secrets/broker1_truststore_creds")
                        .withCopyFileToContainer(forClasspathResource("kafka/ssl/secrets/kafka.broker1.keystore"), "/etc/kafka/secrets/kafka.broker1.keystore")
                        .withCopyFileToContainer(forClasspathResource("kafka/ssl/secrets/kafka.broker1.truststore"), "/etc/kafka/secrets/kafka.broker1.truststore");
                clientKeystorePath = extractResourceToTempFile("kafka/ssl/secrets/kafka.client.keystore", ".keystore");
                clientTruststorePath = extractResourceToTempFile("kafka/ssl/secrets/kafka.client.truststore", ".truststore");
            }
            case SASL_PLAINTEXT -> {
                kafka.withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
                        .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                        .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_server_jaas.conf")
                        .withEnv("KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" serviceName=\"kafka\";")
                        .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" serviceName=\"kafka\";")
                        .withEnv("ZOOKEEPER_SASL_ENABLED", "false")
                        .withCopyToContainer(Transferable.of("""
                                KafkaServer {
                                   org.apache.kafka.common.security.plain.PlainLoginModule required
                                   username="admin"
                                   password="admin-secret"
                                   user_admin="admin-secret";
                                };
                                """), "/tmp/kafka_server_jaas.conf");
            }
        }
    }

    private void initializeBrokerSecurityConfig()
    {
        if (securityMode == SecurityMode.SSL) {
            execInKafka("bash", "-lc", """
                    cat >/tmp/admin-client.properties <<'EOF'
                    security.protocol=SSL
                    ssl.truststore.location=/etc/kafka/secrets/kafka.broker1.truststore
                    ssl.truststore.password=confluent
                    ssl.truststore.type=PKCS12
                    ssl.keystore.location=/etc/kafka/secrets/kafka.broker1.keystore
                    ssl.keystore.password=confluent
                    ssl.keystore.type=PKCS12
                    ssl.key.password=confluent
                    ssl.endpoint.identification.algorithm=
                    EOF
                    """);
        }
        if (securityMode == SecurityMode.SASL_PLAINTEXT) {
            execInKafka("bash", "-lc", """
                    cat >/tmp/admin-client.properties <<'EOF'
                    security.protocol=SASL_PLAINTEXT
                    sasl.mechanism=PLAIN
                    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
                    EOF
                    """);
        }
    }

    private void execInKafka(String... commandAndArgs)
    {
        try {
            ExecResult result = kafka.execInContainer(commandAndArgs);
            checkState(result.getExitCode() == 0, "Kafka exec failed: %s%nstdout:%n%s%nstderr:%n%s", join(" ", commandAndArgs), result.getStdout(), result.getStderr());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute command in Kafka container", e);
        }
    }

    private static Path extractResourceToTempFile(String resourcePath, String suffix)
    {
        try (InputStream inputStream = TestingKafka.class.getClassLoader().getResourceAsStream(resourcePath)) {
            checkState(inputStream != null, "Resource not found: %s", resourcePath);
            Path tempFile = Files.createTempFile("testing-kafka-", suffix);
            Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to extract resource " + resourcePath, e);
        }
    }

    private static void deleteIfExists(Path path)
    {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete temporary file: %s", path);
        }
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public String getSchemaRegistryConnectString()
    {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);
    }

    public Network getNetwork()
    {
        return network;
    }

    private static final class SecureKafkaContainer
            extends GenericContainer<SecureKafkaContainer>
    {
        private final SecurityMode securityMode;

        private SecureKafkaContainer(DockerImageName dockerImageName, SecurityMode securityMode)
        {
            super(dockerImageName);
            this.securityMode = securityMode;
            withExposedPorts(KAFKA_PORT);
            withCommand("sh", "-c", "while [ ! -f " + KAFKA_STARTER_SCRIPT + " ]; do sleep 0.1; done; " + KAFKA_STARTER_SCRIPT);
            waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));

            withEnv("KAFKA_NODE_ID", "1");
            withEnv("KAFKA_PROCESS_ROLES", "broker,controller");
            withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
            withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:" + KAFKA_CONTROLLER_PORT);
            withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");
            withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
            withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
            withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
            withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.toString(Long.MAX_VALUE));
            withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

            switch (securityMode) {
                case SSL -> {
                    withEnv("KAFKA_LISTENERS", "SSL://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:" + KAFKA_INTERNAL_BROKER_PORT + ",CONTROLLER://0.0.0.0:" + KAFKA_CONTROLLER_PORT);
                    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SSL:SSL,BROKER:SSL,CONTROLLER:PLAINTEXT");
                }
                case SASL_PLAINTEXT -> {
                    withEnv("KAFKA_LISTENERS", "SASL_PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:" + KAFKA_INTERNAL_BROKER_PORT + ",CONTROLLER://0.0.0.0:" + KAFKA_CONTROLLER_PORT);
                    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "SASL_PLAINTEXT:SASL_PLAINTEXT,BROKER:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
                }
                case PLAINTEXT -> throw new IllegalStateException("Unexpected plaintext secure Kafka container");
            }
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo)
        {
            super.containerIsStarting(containerInfo);
            String externalListener = switch (securityMode) {
                case SSL -> format("SSL://%s:%s", getHost(), getMappedPort(KAFKA_PORT));
                case SASL_PLAINTEXT -> format("SASL_PLAINTEXT://%s:%s", getHost(), getMappedPort(KAFKA_PORT));
                case PLAINTEXT -> throw new IllegalStateException("Unexpected plaintext secure Kafka container");
            };
            String brokerAdvertisedListener = format("BROKER://kafka:%s", KAFKA_INTERNAL_BROKER_PORT);
            String starterScript = "#!/bin/bash\n"
                    + "export KAFKA_ADVERTISED_LISTENERS=" + externalListener + "," + brokerAdvertisedListener + "\n"
                    + "/etc/confluent/docker/run\n";
            copyFileToContainer(Transferable.of(starterScript, 0777), KAFKA_STARTER_SCRIPT);
        }
    }
}
