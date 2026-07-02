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
package io.trino.tests.product.mariadb;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.mariadb.MariaDBContainer;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * MariaDB product test environment with Trino configured to connect to MariaDB.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>MariaDB container with test database</li>
 *   <li>Trino container with mariadb catalog</li>
 * </ul>
 */
public class MariaDbEnvironment
        extends ProductTestEnvironment
{
    private static final int MARIADB_PORT = 3306;

    private Network network;
    private MariaDBContainer mariadb;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        mariadb = new MariaDBContainer(DockerImageName.parse("mariadb:10.10"))
                .withNetwork(network)
                .withNetworkAliases("mariadb")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test")
                .withCommand("--character-set-server", "utf8mb4", "--explicit-defaults-for-timestamp=1");
        mariadb.start();

        // Grant privileges to the test user
        try (Connection connection = DriverManager.getConnection(
                format("jdbc:mariadb://%s:%s", mariadb.getHost(), mariadb.getMappedPort(MARIADB_PORT)),
                "root",
                mariadb.getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", mariadb.getUsername()));
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to grant privileges", e);
        }

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("mariadb", Map.of(
                        "connector.name", "mariadb",
                        "connection-url", "jdbc:mariadb://mariadb:3306",
                        "connection-user", "test",
                        "connection-password", "test"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for Trino cluster", e);
        }
        catch (SQLException e) {
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

    /**
     * Creates a direct connection to MariaDB for test setup/teardown.
     */
    public Connection createMariaDbConnection()
            throws SQLException
    {
        return mariadb.createConnection("");
    }

    @Override
    protected void afterEachTest()
            throws Exception
    {
        clearTestSchema();
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
        if (mariadb != null) {
            mariadb.close();
            mariadb = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void clearTestSchema()
            throws SQLException
    {
        try (Connection connection = createMariaDbConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET FOREIGN_KEY_CHECKS = 0");
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            for (String table : tables) {
                statement.execute("DROP TABLE IF EXISTS `" + table + "`");
            }
            statement.execute("SET FOREIGN_KEY_CHECKS = 1");
        }
    }
}
