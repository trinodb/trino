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

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Map.entry;

/**
 * Product test environment for multi-node Trino with Iceberg on S3 (Minio) with caching.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with Hive Metastore for Iceberg metadata</li>
 *   <li>Minio container for S3-compatible object storage</li>
 *   <li>Trino with Iceberg catalog configured for:
 *       <ul>
 *         <li>Native S3 filesystem (fs.native-s3.enabled)</li>
 *         <li>File system caching (fs.cache.enabled)</li>
 *         <li>Path-style S3 access for Minio compatibility</li>
 *       </ul>
 *   </li>
 * </ul>
 * <p>
 * This environment is based on the launcher environment EnvMultinodeIcebergMinioCaching.
 * Note: Currently runs as single-node as TrinoProductTestContainer does not yet support
 * multi-node configuration. Caching works the same in single-node mode.
 */
public class MultiNodeIcebergMinioCachingEnvironment
        extends ProductTestEnvironment
{
    private static final String S3_BUCKET_NAME = "test-bucket";

    private Network network;
    private HadoopContainer hadoop;
    private Minio minio;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop for Hive Metastore
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                // HMS validates schema/table locations; wire Minio S3A support for s3:// paths.
                .withS3Config(
                        "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD);
        hadoop.start();

        // Start Minio for S3-compatible storage
        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();

        // Create test bucket
        minio.createBucket(S3_BUCKET_NAME);

        // Build internal S3 endpoint URL for container-to-container communication
        String s3Endpoint = "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT + "/";

        // Start Trino with Iceberg catalog configured for S3 and caching
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("iceberg", Map.ofEntries(
                        entry("connector.name", "iceberg"),
                        entry("hive.metastore.uri", hadoop.getHiveMetastoreUri()),
                        entry("fs.cache.enabled", "true"),
                        entry("fs.cache.directories", "/tmp/cache/iceberg"),
                        entry("fs.cache.max-disk-usage-percentages", "90"),
                        entry("fs.native-s3.enabled", "true"),
                        entry("fs.hadoop.enabled", "false"),
                        entry("s3.region", Minio.MINIO_REGION),
                        entry("s3.aws-access-key", Minio.MINIO_ROOT_USER),
                        entry("s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD),
                        entry("s3.endpoint", s3Endpoint),
                        entry("s3.path-style-access", "true")))
                .build();

        // Add tmpfs mount for cache directory - mount at exact path Trino expects
        trino.withTmpFs(Map.of("/tmp/cache/iceberg", "rw,mode=777"));

        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
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
        if (minio != null) {
            minio.close();
            minio = null;
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
