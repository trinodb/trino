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
package io.trino.tests.product.loki;

import io.trino.testing.containers.LokiContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Product test environment for Grafana Loki log aggregation testing.
 * <p>
 * This environment configures:
 * <ul>
 *   <li>Loki container for log aggregation</li>
 *   <li>Trino with the Loki connector configured</li>
 * </ul>
 * <p>
 * The Loki catalog connects to the Loki HTTP API, enabling log queries
 * through Trino SQL.
 */
public class LokiEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private LokiContainer loki;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        loki = new LokiContainer()
                .withNetwork(network)
                .withNetworkAliases(LokiContainer.HOST_NAME);
        loki.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("loki", Map.of(
                        "connector.name", "loki",
                        "loki.uri", loki.getInternalHttpUrl()))
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

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    /**
     * Returns the Loki HTTP API URL for external access from the host.
     */
    public String getLokiHttpUrl()
    {
        return loki != null ? loki.getHttpUrl() : null;
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
        if (loki != null) {
            loki.close();
            loki = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
