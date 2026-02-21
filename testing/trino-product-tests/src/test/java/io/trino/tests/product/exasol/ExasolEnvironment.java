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
package io.trino.tests.product.exasol;

import com.github.dockerjava.api.model.HostConfig;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.lang.String.format;

/**
 * Exasol product test environment.
 */
public class ExasolEnvironment
        extends ProductTestEnvironment
{
    private static final int EXASOL_PORT = 8563;

    private Network network;
    private GenericContainer<?> exasol;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        exasol = new GenericContainer<>(DockerImageName.parse("exadockerci4/docker-db:2025.1.8_dev_java_slc_only"))
                .withNetwork(network)
                .withNetworkAliases("exasol")
                .withExposedPorts(EXASOL_PORT)
                .withEnv("COSLWD_ENABLED", "1")
                .waitingFor(Wait.forListeningPort())
                .withCreateContainerCmdModifier(cmd -> {
                    HostConfig hostConfig = cmd.getHostConfig();
                    if (hostConfig == null) {
                        cmd.withHostConfig(new HostConfig().withPrivileged(true));
                        return;
                    }
                    hostConfig.withPrivileged(true);
                });
        exasol.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("exasol", Map.of(
                        "connector.name", "exasol",
                        "connection-url", "jdbc:exa:exasol:8563;validateservercertificate=0",
                        "connection-user", "sys",
                        "connection-password", "exasol"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    public Connection createExasolConnection()
            throws SQLException
    {
        return DriverManager.getConnection(
                format("jdbc:exa:%s:%s;validateservercertificate=0", exasol.getHost(), exasol.getMappedPort(EXASOL_PORT)),
                "sys",
                "exasol");
    }

    public void executeExasolUpdate(String sql)
    {
        try (Connection connection = createExasolConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Exasol query: " + sql, e);
        }
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
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
        if (exasol != null) {
            exasol.close();
            exasol = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
