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
package io.trino.tests.product.faulttolerant;

import io.trino.testing.containers.Minio;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Product test environment for task-retry/fault-tolerant smoke tests.
 */
public class FaultTolerantEnvironment
        extends ProductTestEnvironment
{
    private static final String EXCHANGE_BUCKET = "test-bucket";

    private Network network;
    private Minio minio;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
    {
        if (trinoCluster != null) {
            return;
        }

        network = Network.newNetwork();

        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(EXCHANGE_BUCKET);

        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withConfigProperty("retry-policy", "task")
                .withConfigProperty("task.min-writer-count", "2")
                .withConfigProperty("task.concurrency", "2")
                .withConfigProperty("task.max-writer-count", "2")
                .withConfigProperty("query.min-expire-age", "1m")
                .withConfigProperty("task.info.max-age", "1m")
                .withExchangeManager(Map.of(
                        "exchange-manager.name", "filesystem",
                        "exchange.s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT + "/",
                        "exchange.s3.region", Minio.MINIO_REGION,
                        "exchange.s3.aws-access-key", Minio.MINIO_ROOT_USER,
                        "exchange.s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD,
                        "exchange.s3.path-style-access", "true",
                        "exchange.base-directories", "s3://" + EXCHANGE_BUCKET))
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
        return trinoCluster.createConnection();
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user);
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
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
