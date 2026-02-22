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
package io.trino.tests.product.ranger;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Ranger product test environment based on launcher multinode-ranger configuration.
 */
public class RangerEnvironment
        extends ProductTestEnvironment
{
    private static final String RANGER_CONFIG_DIR =
            "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/environment/multinode-ranger";

    private Network network;
    private MariaDBContainer mariadb;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        mariadb = new MariaDBContainer(DockerImageName.parse("mariadb:10.7.1"))
                .withNetwork(network)
                .withNetworkAliases("mariadb")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        mariadb.start();

        Path rangerConfigDir = locateRangerConfigDir();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("mariadb", Map.of(
                        "connector.name", "mariadb",
                        "connection-url", "jdbc:mariadb://mariadb:3306/",
                        "connection-user", "test",
                        "connection-password", "test"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();

        trino.withCopyToContainer(
                Transferable.of("""
                        access-control.name=ranger
                        ranger.service.name=dev_trino
                        ranger.plugin.config.resource=/etc/trino/ranger-trino-security.xml,/etc/trino/ranger-trino-audit.xml,/etc/trino/ranger-policymgr-ssl.xml
                        """),
                "/etc/trino/access-control.properties");
        trino.withCopyFileToContainer(forHostPath(rangerConfigDir.resolve("ranger-trino-security.xml").toString()), "/etc/trino/ranger-trino-security.xml");
        trino.withCopyFileToContainer(forHostPath(rangerConfigDir.resolve("ranger-trino-audit.xml").toString()), "/etc/trino/ranger-trino-audit.xml");
        trino.withCopyFileToContainer(forHostPath(rangerConfigDir.resolve("ranger-policymgr-ssl.xml").toString()), "/etc/trino/ranger-policymgr-ssl.xml");
        trino.withCopyFileToContainer(forHostPath(rangerConfigDir.resolve("trino-policies.json").toString()), "/tmp/ranger-policycache/trino_dev_trino.json");
        trino.withUsername("hive");
        trino.waitingFor(Wait.forHttp("/v1/info").forStatusCode(200));

        trino.start();
        waitForTrinoReady(trino);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, "hive");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (mariadb != null) {
            mariadb.close();
            mariadb = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private static Path locateRangerConfigDir()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(RANGER_CONFIG_DIR);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate Ranger config directory: " + RANGER_CONFIG_DIR);
    }

    private static void waitForTrinoReady(TrinoContainer trino)
    {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (Connection ignored = TrinoProductTestContainer.createConnection(trino, "hive")) {
                return;
            }
            catch (SQLException ignored) {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for Trino readiness", e);
                }
            }
        }
        throw new IllegalStateException("Trino cluster not ready within timeout");
    }
}
