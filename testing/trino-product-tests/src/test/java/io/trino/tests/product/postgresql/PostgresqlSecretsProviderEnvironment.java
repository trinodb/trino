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
package io.trino.tests.product.postgresql;

import io.trino.testing.containers.TrinoTestImages;
import io.trino.tests.product.utils.HostMappingDnsResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Multinode PostgreSQL environment with keystore-backed secrets, password authentication, and TLS.
 */
public class PostgresqlSecretsProviderEnvironment
        extends PostgresqlEnvironment
{
    private static final int HTTPS_PORT = 7778;
    private static final String COORDINATOR_ALIAS = "trino";
    private static final String COORDINATOR_FQDN = "trino.docker.cluster";
    private static final String WORKER_ALIAS = "trino-worker";
    private static final String WORKER_FQDN = "trino-worker.docker.cluster";
    private static final String KEYSTORE_PASSWORD = "123456";
    private static final String TRINO_KEYSTORE_PATH = "/etc/trino/docker.cluster.jks";
    private static final String KEYSTORE_RESOURCE_PATH = "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/trino/etc/docker.cluster.jks";

    private Network network;
    private PostgreSQLContainer<?> postgresql;
    private GenericContainer<?> coordinator;
    private GenericContainer<?> worker;
    private Path tlsKeyStore;
    private Path credentialStore;

    @Override
    public void start()
    {
        if (coordinator != null && coordinator.isRunning()) {
            return;
        }

        network = Network.newNetwork();
        postgresql = new PostgreSQLContainer<>("postgres:11")
                .withNetwork(network)
                .withNetworkAliases("postgresql")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        postgresql.start();

        tlsKeyStore = createTlsKeyStore();
        credentialStore = createCredentialStore();
        coordinator = createTrinoContainer(COORDINATOR_ALIAS, COORDINATOR_FQDN, coordinatorConfig());
        coordinator.withExposedPorts(HTTPS_PORT);
        coordinator.withCopyToContainer(
                Transferable.of(passwordAuthenticatorProperties()),
                "/etc/trino/authenticator.properties");
        coordinator.withCopyToContainer(
                Transferable.of(passwordDatabase()),
                "/etc/trino/password.db");

        worker = createTrinoContainer(WORKER_ALIAS, WORKER_FQDN, workerConfig());

        coordinator.start();
        worker.start();
        waitForClusterReady();
        PostgresqlBasicEnvironment.createTestTables(postgresql);
    }

    private GenericContainer<?> createTrinoContainer(String alias, String fullyQualifiedName, String configProperties)
    {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(TrinoTestImages.getDefaultTrinoImage()))
                .withNetwork(network)
                .withNetworkAliases(alias, fullyQualifiedName)
                .withCreateContainerCmdModifier(command -> command.withHostName(alias))
                .withCreateContainerCmdModifier(command -> command.withDomainName("docker.cluster"))
                .waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        container.withCopyToContainer(Transferable.of(configProperties), "/etc/trino/config.properties");
        container.withCopyToContainer(Transferable.of(postgresqlCatalogProperties()), "/etc/trino/catalog/postgresql.properties");
        container.withCopyToContainer(Transferable.of("connector.name=tpch\n"), "/etc/trino/catalog/tpch.properties");
        container.withCopyToContainer(Transferable.of(secretsConfiguration()), "/etc/trino/secrets.toml");
        container.withCopyFileToContainer(MountableFile.forHostPath(tlsKeyStore), TRINO_KEYSTORE_PATH);
        container.withCopyFileToContainer(MountableFile.forHostPath(credentialStore), "/etc/trino/credential.jckes");
        return container;
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return createTrinoConnection("test");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", "password");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", tlsKeyStore.toString());
        properties.setProperty("SSLTrustStorePassword", KEYSTORE_PASSWORD);
        properties.setProperty("dnsResolver", HostMappingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", "%s=%s;%s=%s".formatted(
                COORDINATOR_FQDN,
                coordinator.getHost(),
                COORDINATOR_ALIAS,
                coordinator.getHost()));
        return DriverManager.getConnection(getTrinoJdbcUrl(), properties);
    }

    @Override
    public Connection createPostgresqlConnection()
            throws SQLException
    {
        return postgresql.createConnection("");
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        if (coordinator == null) {
            return null;
        }
        return "jdbc:trino://%s:%d/postgresql/public".formatted(
                COORDINATOR_FQDN,
                coordinator.getMappedPort(HTTPS_PORT));
    }

    @Override
    public boolean isRunning()
    {
        return coordinator != null && coordinator.isRunning() && worker != null && worker.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (worker != null) {
            worker.close();
            worker = null;
        }
        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
        if (tlsKeyStore != null) {
            deleteTemporaryFile(tlsKeyStore, "TLS key store");
            tlsKeyStore = null;
        }
        if (credentialStore != null) {
            deleteTemporaryFile(credentialStore, "credential store");
            credentialStore = null;
        }
    }

    private void waitForClusterReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = createTrinoConnection();
                    Statement statement = connection.createStatement();
                    ResultSet nodes = statement.executeQuery("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")) {
                if (nodes.next() && nodes.getInt(1) == 2) {
                    try (ResultSet result = statement.executeQuery("SELECT count(*) FROM tpch.tiny.nation")) {
                        if (result.next()) {
                            return;
                        }
                    }
                }
            }
            catch (SQLException ignored) {
                // The cluster is still starting.
            }

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for the Trino cluster", e);
            }
        }
        throw new IllegalStateException("Secrets-provider Trino cluster was not ready within timeout");
    }

    private static Path createTlsKeyStore()
    {
        try {
            Path keyStore = Files.createTempFile("postgresql-secrets-", ".jks");
            Files.copy(locateKeyStore(), keyStore, REPLACE_EXISTING);
            runKeytool(
                    List.of(
                            "-genkeypair",
                            "-alias",
                            WORKER_ALIAS,
                            "-dname",
                            "CN=" + WORKER_FQDN,
                            "-ext",
                            "SAN=dns:" + WORKER_FQDN + ",dns:" + WORKER_ALIAS,
                            "-keyalg",
                            "RSA",
                            "-keysize",
                            "2048",
                            "-validity",
                            "3650",
                            "-keystore",
                            keyStore.toString(),
                            "-storetype",
                            "JKS",
                            "-storepass",
                            KEYSTORE_PASSWORD,
                            "-keypass",
                            KEYSTORE_PASSWORD,
                            "-noprompt"),
                    null);
            return keyStore;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare TLS key store", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while preparing TLS key store", e);
        }
    }

    private static Path createCredentialStore()
    {
        try {
            Path keyStore = Files.createTempFile("postgresql-secrets-", ".p12");
            Files.delete(keyStore);
            importSecret(keyStore, "keystore_password", KEYSTORE_PASSWORD);
            importSecret(keyStore, "shared_secret", "shared_secret");
            importSecret(keyStore, "password_db_file", "etc/password.db");
            importSecret(keyStore, "postgres_password", "test");
            return keyStore;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare credential store", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while preparing credential store", e);
        }
    }

    private static void importSecret(Path keyStore, String alias, String value)
            throws IOException, InterruptedException
    {
        runKeytool(
                List.of(
                        "-importpass",
                        "-storetype",
                        "pkcs12",
                        "-alias",
                        alias,
                        "-keystore",
                        keyStore.toString(),
                        "-storepass",
                        "password"),
                value + "\n" + value + "\n");
    }

    private static void runKeytool(List<String> arguments, String input)
            throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(arguments);

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        try (var standardInput = process.getOutputStream()) {
            if (input != null) {
                standardInput.write(input.getBytes(StandardCharsets.UTF_8));
            }
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }

    private static void deleteTemporaryFile(Path path, String description)
    {
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete temporary " + description, e);
        }
    }

    private static Path locateKeyStore()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(KEYSTORE_RESOURCE_PATH);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate TLS key store at " + KEYSTORE_RESOURCE_PATH);
    }

    private static String coordinatorConfig()
    {
        return commonConfig("trino-coordinator") +
                """
                coordinator=true
                node-scheduler.include-coordinator=false
                http-server.authentication.type=PASSWORD
                password-authenticator.config-files=etc/authenticator.properties
                """;
    }

    private static String workerConfig()
    {
        return commonConfig("trino-worker") +
                """
                coordinator=false
                """;
    }

    private static String commonConfig(String nodeId)
    {
        return """
               node.id=%s
               node.environment=test
               node.internal-address-source=FQDN
               discovery.uri=https://%s:%d
               http-server.http.enabled=false
               http-server.https.enabled=true
               http-server.https.port=%d
               http-server.https.keystore.path=%s
               http-server.https.keystore.key=${keystore:keystore_password}
               internal-communication.https.required=true
               internal-communication.shared-secret=${keystore:shared_secret}
               internal-communication.https.keystore.path=%s
               internal-communication.https.keystore.key=${keystore:keystore_password}
               catalog.management=dynamic
               query.min-expire-age=1m
               task.info.max-age=1m
               """.formatted(
                nodeId,
                COORDINATOR_FQDN,
                HTTPS_PORT,
                HTTPS_PORT,
                TRINO_KEYSTORE_PATH,
                TRINO_KEYSTORE_PATH);
    }

    private static String postgresqlCatalogProperties()
    {
        return """
               connector.name=postgresql
               connection-url=jdbc:postgresql://postgresql:5432/test
               connection-user=test
               connection-password=${keystore:postgres_password}
               """;
    }

    private static String passwordAuthenticatorProperties()
    {
        return """
               password-authenticator.name=file
               file.password-file=${keystore:password_db_file}
               """;
    }

    private static String passwordDatabase()
    {
        return "test:$2y$10$3zhdw4rVy5f5VTjE1YEm5uhdEP3YmKjlsplBrVPJz6SsddMYi4nHa\n";
    }

    private static String secretsConfiguration()
    {
        return """
               [keystore]
               secrets-provider.name = "keystore"
               keystore-file-path = "/etc/trino/credential.jckes"
               keystore-type = "pkcs12"
               keystore-password = "password"
               """;
    }
}
