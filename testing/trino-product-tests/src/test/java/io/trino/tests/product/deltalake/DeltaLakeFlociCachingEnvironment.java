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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.Floci;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Map.entry;

/**
 * Multi-node Delta Lake environment with Floci and filesystem caching.
 */
public class DeltaLakeFlociCachingEnvironment
        extends ProductTestEnvironment
{
    private static final String FLOCI_HOST_NAME = "floci";
    private static final String BUCKET_NAME = "delta-cache-test-bucket";
    private static final String CACHE_DIRECTORY = "/tmp/cache/delta";
    private static final String CACHE_MANAGER_PATH = "/etc/trino/cache-manager-alluxio.properties";

    private Network network;
    private Floci floci;
    private HadoopContainer hadoop;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
            throws SQLException, InterruptedException
    {
        if (isRunning()) {
            return;
        }

        network = Network.newNetwork();
        String s3Endpoint = "http://" + FLOCI_HOST_NAME + ":" + Floci.FLOCI_PORT;

        floci = new Floci()
                .withNetwork(network)
                .withNetworkAliases(FLOCI_HOST_NAME);
        floci.start();
        floci.createBucket(BUCKET_NAME);

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                .withS3Config(s3Endpoint, Floci.FLOCI_ACCESS_KEY, Floci.FLOCI_SECRET_KEY);
        hadoop.start();

        String metastoreUri = "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;
        Map<String, String> commonDeltaCatalogProperties = Map.ofEntries(
                entry("connector.name", "delta_lake"),
                entry("hive.metastore.uri", metastoreUri),
                entry("fs.native-s3.enabled", "true"),
                entry("fs.hadoop.enabled", "false"),
                entry("s3.endpoint", s3Endpoint),
                entry("s3.aws-access-key", Floci.FLOCI_ACCESS_KEY),
                entry("s3.aws-secret-key", Floci.FLOCI_SECRET_KEY),
                entry("s3.path-style-access", "true"),
                entry("s3.region", Floci.FLOCI_REGION),
                entry("delta.enable-non-concurrent-writes", "true"),
                entry("delta.register-table-procedure.enabled", "true"));

        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withConfigProperty("node-scheduler.include-coordinator", "false")
                .withCatalog("delta", Map.ofEntries(
                        entry("connector.name", commonDeltaCatalogProperties.get("connector.name")),
                        entry("hive.metastore.uri", commonDeltaCatalogProperties.get("hive.metastore.uri")),
                        entry("fs.native-s3.enabled", commonDeltaCatalogProperties.get("fs.native-s3.enabled")),
                        entry("fs.hadoop.enabled", commonDeltaCatalogProperties.get("fs.hadoop.enabled")),
                        entry("s3.endpoint", commonDeltaCatalogProperties.get("s3.endpoint")),
                        entry("s3.aws-access-key", commonDeltaCatalogProperties.get("s3.aws-access-key")),
                        entry("s3.aws-secret-key", commonDeltaCatalogProperties.get("s3.aws-secret-key")),
                        entry("s3.path-style-access", commonDeltaCatalogProperties.get("s3.path-style-access")),
                        entry("s3.region", commonDeltaCatalogProperties.get("s3.region")),
                        entry("delta.enable-non-concurrent-writes", commonDeltaCatalogProperties.get("delta.enable-non-concurrent-writes")),
                        entry("delta.register-table-procedure.enabled", commonDeltaCatalogProperties.get("delta.register-table-procedure.enabled")),
                        entry("fs.cache.enabled", "true")))
                .withCatalog("delta_non_cached", commonDeltaCatalogProperties)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("jmx", Map.of("connector.name", "jmx"))
                .withConfigProperty("cache-manager.config-files", "etc/cache-manager-alluxio.properties")
                .withFile(
                        CACHE_MANAGER_PATH,
                        """
                        cache-manager.name=alluxio
                        fs.cache.directories=%s
                        fs.cache.max-disk-usage-percentages=90
                        """.formatted(CACHE_DIRECTORY))
                .withCoordinatorCustomizer(coordinator -> coordinator.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .withWorkerCustomizer(worker -> worker.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .build();

        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    public String getBucketName()
    {
        return BUCKET_NAME;
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection("hive");
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
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (floci != null) {
            floci.close();
            floci = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
