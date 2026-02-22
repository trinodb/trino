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
package io.trino.tests.product.tls;

import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.utils.HostMappingDnsResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/**
 * Single-node TLS environment used by SuiteTls.
 */
public class TlsEnvironment
        extends ProductTestEnvironment
{
    private static final int HTTP_PORT = 8080;
    private static final int HTTPS_PORT = 7778;
    private static final String SHARED_SECRET = "internal-shared-secret";
    private static final String KEYSTORE_CONTAINER_PATH = "/etc/trino/docker.cluster.jks";
    private static final String KEYSTORE_RESOURCE_PATH = "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/trino/etc/docker.cluster.jks";
    private static final String TRINO_FQDN = "trino.docker.cluster";
    private static final String TRINO_ALIAS = "trino";

    private Network network;
    private GenericContainer<?> trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        trino = new GenericContainer<>(TrinoTestImages.getDefaultTrinoImage())
                .withNetwork(network)
                .withNetworkAliases(TRINO_ALIAS, TRINO_FQDN)
                .withCreateContainerCmdModifier(cmd -> cmd.withHostName(TRINO_ALIAS))
                .withCreateContainerCmdModifier(cmd -> cmd.withDomainName("docker.cluster"));

        // Use log-based wait; HTTP is disabled in this environment.
        trino.waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        trino.addExposedPort(HTTP_PORT);
        trino.addExposedPort(HTTPS_PORT);

        trino.withCopyToContainer(Transferable.of(tlsConfigProperties()), "/etc/trino/config.properties");
        trino.withCopyFileToContainer(MountableFile.forHostPath(locateKeystore().toString()), KEYSTORE_CONTAINER_PATH);
        trino.withCopyToContainer(Transferable.of("connector.name=tpch\n"), "/etc/trino/catalog/tpch.properties");
        trino.start();

        waitUntilJdbcReady();
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return createStrictTrinoConnection("hive");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return createStrictTrinoConnection(user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino == null ? null : jdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    public String getHost()
    {
        return trino.getHost();
    }

    public int getHttpPort()
    {
        return trino.getMappedPort(HTTP_PORT);
    }

    public int getHttpsPort()
    {
        return trino.getMappedPort(HTTPS_PORT);
    }

    private Connection createStrictTrinoConnection(String user)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", locateKeystore().toString());
        properties.setProperty("SSLTrustStorePassword", "123456");
        properties.setProperty("dnsResolver", HostMappingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", "%s=%s;%s=%s".formatted(
                TRINO_FQDN, trino.getHost(),
                TRINO_ALIAS, trino.getHost()));
        return DriverManager.getConnection(
                "jdbc:trino://%s:%d".formatted(TRINO_FQDN, trino.getMappedPort(HTTPS_PORT)),
                properties);
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private String jdbcUrl()
    {
        return "jdbc:trino://%s:%d?SSL=true&SSLVerification=FULL&SSLTrustStorePath=%s&SSLTrustStorePassword=123456"
                .formatted(TRINO_FQDN, trino.getMappedPort(HTTPS_PORT), locateKeystore());
    }

    private static String tlsConfigProperties()
    {
        return """
                node.id=trino-coordinator
                node.environment=test
                node.internal-address-source=FQDN
                coordinator=true
                node-scheduler.include-coordinator=true
                discovery.uri=https://trino.docker.cluster:7778
                http-server.http.enabled=false
                http-server.https.enabled=true
                http-server.https.port=7778
                http-server.https.keystore.path=/etc/trino/docker.cluster.jks
                http-server.https.keystore.key=123456
                internal-communication.https.required=true
                internal-communication.shared-secret=internal-shared-secret
                internal-communication.https.keystore.path=/etc/trino/docker.cluster.jks
                internal-communication.https.keystore.key=123456
                catalog.management=dynamic
                query.min-expire-age=1m
                task.info.max-age=1m
                """.replace("internal-shared-secret", SHARED_SECRET);
    }

    private static Path locateKeystore()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(KEYSTORE_RESOURCE_PATH);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate TLS keystore at " + KEYSTORE_RESOURCE_PATH);
    }

    private void waitUntilJdbcReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection ignored = createTrinoConnection()) {
                return;
            }
            catch (SQLException ignored) {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for TLS Trino readiness", e);
                }
            }
        }
        throw new IllegalStateException("TLS Trino was not ready within timeout");
    }
}
