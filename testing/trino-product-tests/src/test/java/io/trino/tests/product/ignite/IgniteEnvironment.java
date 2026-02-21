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
package io.trino.tests.product.ignite;

import io.trino.testing.containers.IgniteContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Ignite product test environment.
 */
public class IgniteEnvironment
        extends ProductTestEnvironment
{
    private static final String IGNITE_JAVA_TOOL_OPTIONS = "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED";

    private Network network;
    private IgniteContainer ignite;
    private IgniteContainer igniteSecondary;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
    {
        if (trinoCluster != null && trinoCluster.getCoordinator().isRunning()) {
            return;
        }

        network = Network.newNetwork();

        ignite = new IgniteContainer()
                .withNetwork(network)
                .withNetworkAliases(IgniteContainer.HOST_NAME, IgniteContainer.HOST_NAME + "-1", "localhost");
        ignite.start();

        igniteSecondary = new IgniteContainer()
                .withNetwork(network)
                .withNetworkAliases(IgniteContainer.HOST_NAME + "-2", "localhost");
        igniteSecondary.start();

        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withCatalog("ignite", Map.of(
                        "connector.name", "ignite",
                        "connection-url", ignite.getInternalJdbcUrl() + "/",
                        "connection-user", "exampleuser",
                        "connection-password", "examplepassword",
                        "write.batch-size", "1"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCoordinatorCustomizer(coordinator -> coordinator.withEnv("JAVA_TOOL_OPTIONS", IGNITE_JAVA_TOOL_OPTIONS))
                .withWorkerCustomizer(worker -> worker.withEnv("JAVA_TOOL_OPTIONS", IGNITE_JAVA_TOOL_OPTIONS))
                .build();
        trinoCluster.start();

        try {
            trinoCluster.waitForClusterReady();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection(createConnectionProperties("hive"));
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(createConnectionProperties(user));
    }

    private static Properties createConnectionProperties(String user)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", user);
        connectionProperties.setProperty("catalog", "ignite");
        connectionProperties.setProperty("schema", "public");
        return connectionProperties;
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoCluster != null ? trinoCluster.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trinoCluster != null && trinoCluster.getCoordinator().isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trinoCluster != null) {
            trinoCluster.close();
            trinoCluster = null;
        }
        if (ignite != null) {
            ignite.close();
            ignite = null;
        }
        if (igniteSecondary != null) {
            igniteSecondary.close();
            igniteSecondary = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
