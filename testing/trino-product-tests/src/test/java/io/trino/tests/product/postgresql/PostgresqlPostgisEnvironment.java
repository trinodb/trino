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

import io.trino.testing.containers.TrinoProductTestContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class PostgresqlPostgisEnvironment
        extends PostgresqlEnvironment
{
    private Network network;
    private PostgreSQLContainer<?> postgresql;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        postgresql = new PostgreSQLContainer<>(DockerImageName.parse("postgis/postgis:11-3.2").asCompatibleSubstituteFor("postgres"))
                .withNetwork(network)
                .withNetworkAliases("postgresql")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        postgresql.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("postgresql", Map.of(
                        "connector.name", "postgresql",
                        "connection-url", "jdbc:postgresql://postgresql:5432/test",
                        "connection-user", "test",
                        "connection-password", "test"))
                .build();
        TrinoProductTestContainer.startAndWait(trino);
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
    public Connection createPostgresqlConnection()
            throws SQLException
    {
        return postgresql.createConnection("");
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
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
