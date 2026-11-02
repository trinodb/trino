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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.Floci;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static io.trino.tests.product.iceberg.IcebergCatalogPropertiesBuilder.icebergCatalog;

/**
 * Product test environment for multi-node Trino with Iceberg on S3 (Floci) with caching.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with Hive Metastore for Iceberg metadata</li>
 *   <li>Floci container for S3-compatible object storage</li>
 *   <li>Trino with Iceberg catalog configured for:
 *       <ul>
 *         <li>Native S3 filesystem (fs.s3.enabled)</li>
 *         <li>File system caching (fs.cache.enabled)</li>
 *         <li>Path-style S3 access for Floci compatibility</li>
 *       </ul>
 *   </li>
 * </ul>
 * <p>
 * This environment is based on the launcher environment EnvMultinodeIcebergMinioCaching.
 */
public class MultiNodeIcebergFlociCachingEnvironment
        extends ProductTestEnvironment
{
    private static final String S3_BUCKET_NAME = "test-bucket";
    private static final String CACHE_DIRECTORY = "/tmp/cache/alluxio";
    private static final String CACHE_MANAGER_PATH = "/etc/trino/cache-manager-alluxio.properties";

    private Network network;
    private HadoopContainer hadoop;
    private Floci floci;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
            throws SQLException, InterruptedException
    {
        if (trinoCluster != null) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop for Hive Metastore
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                // HMS validates schema/table locations; wire Floci S3A support for s3:// paths.
                .withS3Config(
                        "http://floci:" + Floci.FLOCI_PORT,
                        Floci.FLOCI_ACCESS_KEY,
                        Floci.FLOCI_SECRET_KEY);
        hadoop.start();

        floci = new Floci()
                .withNetwork(network)
                .withNetworkAliases("floci");
        floci.start();

        floci.createBucket(S3_BUCKET_NAME);

        String s3Endpoint = "http://floci:" + Floci.FLOCI_PORT + "/";

        // Match the legacy StandardMultinode topology so reads execute on a worker.
        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withCatalog("iceberg", icebergCatalog(hadoop.getHiveMetastoreUri())
                        .put("fs.cache.enabled", "true")
                        .put("fs.s3.enabled", "true")
                        .put("s3.region", Floci.FLOCI_REGION)
                        .put("s3.aws-access-key", Floci.FLOCI_ACCESS_KEY)
                        .put("s3.aws-secret-key", Floci.FLOCI_SECRET_KEY)
                        .put("s3.endpoint", s3Endpoint)
                        .put("s3.path-style-access", "true")
                        .build())
                .withCatalog("jmx", Map.of("connector.name", "jmx"))
                .withConfigProperty("cache-manager.config-files", "etc/cache-manager-alluxio.properties")
                .withFile(
                        CACHE_MANAGER_PATH,
                        """
                        cache-manager.name=alluxio
                        fs.cache.directories=%s
                        fs.cache.max-disk-usage-percentages=90
                        """.formatted(CACHE_DIRECTORY))
                .withCoordinatorCustomizer(coordinator ->
                        coordinator.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .withWorkerCustomizer(worker ->
                        worker.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    /**
     * Returns the S3 bucket name used for test data.
     */
    public String getBucketName()
    {
        return S3_BUCKET_NAME;
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
        if (floci != null) {
            floci.close();
            floci = null;
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
