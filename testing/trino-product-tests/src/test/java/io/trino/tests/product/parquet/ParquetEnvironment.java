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
package io.trino.tests.product.parquet;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.hive.HiveCatalogPropertiesBuilder;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Environment for parquet product tests.
 *
 * Mirrors launcher's parquet lane by exposing hive, tpch and tpcds catalogs.
 */
public class ParquetEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private HadoopContainer hadoop;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
            throws Exception
    {
        if (isRunning()) {
            return;
        }

        network = Network.newNetwork();

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withConfigProperty("node-scheduler.include-coordinator", "false")
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", HiveCatalogPropertiesBuilder.hiveCatalog(hadoop.getHiveMetastoreUri())
                        .withHadoopFileSystem()
                        .withPartitionProcedures()
                        .withCommonProperties()
                        .put("hive.fs.cache.max-size", "10")
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("tpcds", Map.of("connector.name", "tpcds"))
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection("hive", "hive", "default");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user, "hive", "default");
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
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
