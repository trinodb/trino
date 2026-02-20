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
package io.trino.tests.product.sqlserver;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * SQL Server product test environment.
 */
public class SqlServerEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private MSSQLServerContainer<?> sqlserver;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        sqlserver = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
                .withNetwork(network)
                .withNetworkAliases("sqlserver")
                .acceptLicense();
        sqlserver.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("sqlserver", Map.of(
                        "connector.name", "sqlserver",
                        "connection-url", "jdbc:sqlserver://sqlserver:1433;encrypt=false",
                        "connection-user", sqlserver.getUsername(),
                        "connection-password", sqlserver.getPassword()))
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
        if (sqlserver != null) {
            sqlserver.close();
            sqlserver = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
