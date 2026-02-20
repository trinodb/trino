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
package io.trino.tests.product.cassandra;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Cassandra product test environment.
 */
public class CassandraEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private CassandraContainer cassandra;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        cassandra = new CassandraContainer("cassandra:4.1")
                .withNetwork(network)
                .withNetworkAliases("cassandra");
        cassandra.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("cassandra", Map.of(
                        "connector.name", "cassandra",
                        "cassandra.contact-points", "cassandra",
                        "cassandra.native-protocol-port", "9042",
                        "cassandra.load-policy.dc-aware.local-dc", "datacenter1",
                        "cassandra.allow-drop-table", "true"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
            // Create test keyspace via Trino's cassandra.system.execute procedure
            createTestKeyspace();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    private void createTestKeyspace()
            throws SQLException
    {
        try (Connection conn = createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("CALL cassandra.system.execute(" +
                    "'CREATE KEYSPACE IF NOT EXISTS test " +
                    "WITH REPLICATION = {''class'':''SimpleStrategy'', ''replication_factor'': 1}')");
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
        if (cassandra != null) {
            cassandra.close();
            cassandra = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
