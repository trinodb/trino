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
package io.trino.tests.product.snowflake;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Product test environment for Snowflake connector validation.
 *
 * Mirrors launcher's multinode-snowflake setup by wiring Snowflake credentials
 * into a single Trino container with tpch and snowflake catalogs.
 */
public class SnowflakeEnvironment
        extends ProductTestEnvironment
{
    private static final Path CONFIG_DIR = Path.of(
            "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/environment/multinode-snowflake");
    private static final String JVM_CONFIG_TARGET = "/etc/trino/jvm.config";

    private Network network;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        String snowflakeUrl = requireEnv("SNOWFLAKE_URL");
        String snowflakeUser = requireEnv("SNOWFLAKE_USER");
        String snowflakePassword = requireEnv("SNOWFLAKE_PASSWORD");
        String snowflakeDatabase = requireEnv("SNOWFLAKE_DATABASE");
        String snowflakeRole = requireEnv("SNOWFLAKE_ROLE");
        String snowflakeWarehouse = requireEnv("SNOWFLAKE_WAREHOUSE");

        network = Network.newNetwork();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("snowflake", Map.of(
                        "connector.name", "snowflake",
                        "connection-url", "${ENV:SNOWFLAKE_URL}",
                        "connection-user", "${ENV:SNOWFLAKE_USER}",
                        "connection-password", "${ENV:SNOWFLAKE_PASSWORD}",
                        "snowflake.database", "${ENV:SNOWFLAKE_DATABASE}",
                        "snowflake.role", "${ENV:SNOWFLAKE_ROLE}",
                        "snowflake.warehouse", "${ENV:SNOWFLAKE_WAREHOUSE}"))
                .build();

        trino.withEnv("SNOWFLAKE_URL", snowflakeUrl);
        trino.withEnv("SNOWFLAKE_USER", snowflakeUser);
        trino.withEnv("SNOWFLAKE_PASSWORD", snowflakePassword);
        trino.withEnv("SNOWFLAKE_DATABASE", snowflakeDatabase);
        trino.withEnv("SNOWFLAKE_ROLE", snowflakeRole);
        trino.withEnv("SNOWFLAKE_WAREHOUSE", snowflakeWarehouse);
        trino.withCopyFileToContainer(forHostPath(CONFIG_DIR.resolve("jvm.config").toString()), JVM_CONFIG_TARGET);
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
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
