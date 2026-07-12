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
package io.trino.tests.product.tpc;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

abstract class AbstractTpcEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private HadoopContainer hadoop;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public final void start()
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
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", hiveCatalog(hadoop.getHiveMetastoreUri())
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog(sourceCatalog(), sourceCatalogProperties())
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();

        executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + targetSchema());
        for (String table : tables()) {
            executeTrinoUpdate("CREATE TABLE IF NOT EXISTS hive.%s.%s WITH (format = 'TEXTFILE') AS %s"
                    .formatted(targetSchema(), table, sourceTableQuery(table)));
        }
    }

    protected abstract String sourceCatalog();

    protected abstract Map<String, String> sourceCatalogProperties();

    protected abstract String targetSchema();

    protected abstract List<String> tables();

    protected String sourceTableQuery(String table)
    {
        return "SELECT * FROM %s.sf1.%s".formatted(sourceCatalog(), table);
    }

    @Override
    public final Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection("hive", "hive", targetSchema());
    }

    @Override
    public final Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user, "hive", targetSchema());
    }

    @Override
    public final String getTrinoJdbcUrl()
    {
        return trinoCluster != null ? trinoCluster.getJdbcUrl() : null;
    }

    @Override
    public final boolean isRunning()
    {
        return trinoCluster != null && trinoCluster.getCoordinator().isRunning();
    }

    @Override
    protected final void doClose()
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
