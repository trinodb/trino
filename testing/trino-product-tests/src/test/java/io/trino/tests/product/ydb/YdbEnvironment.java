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
package io.trino.tests.product.ydb;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

/**
 * YDB product test environment.
 */
public class YdbEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private GenericContainer<?> ydb;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        ydb = new GenericContainer<>("ydbplatform/local-ydb:latest")
                .withNetwork(network)
                .withNetworkAliases("ydb")
                .withExposedPorts(2136)
                .withEnv("YDB_DEFAULT_LOG_LEVEL", "NOTICE")
                .withEnv("GRPC_PORT", "2136")
                .withEnv("GRPC_TLS_PORT", "2135")
                .withEnv("MON_PORT", "8765")
                .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true")
                .waitingFor(Wait.forLogMessage(".*Server run loop enter.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        ydb.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("ydb", Map.of(
                        "connector.name", "ydb",
                        "connection-url", "jdbc:ydb:grpc://ydb:2136/local"))
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
        if (ydb != null) {
            ydb.close();
            ydb = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
