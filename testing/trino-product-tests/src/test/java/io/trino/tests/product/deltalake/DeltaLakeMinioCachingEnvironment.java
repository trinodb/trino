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
 * Delta Lake environment with Minio and filesystem caching enabled for the default delta catalog.
 */
public class DeltaLakeMinioCachingEnvironment
        extends ProductTestEnvironment
{
    private static final String BUCKET_NAME = "delta-cache-test-bucket";
    private static final String CACHE_DIRECTORY = "/tmp/cache/delta";

    private Network network;
    private Minio minio;
    private HadoopContainer hadoop;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(BUCKET_NAME);

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                .withS3Config(
                        "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD);
        hadoop.start();

        String metastoreUri = "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;
        String s3Endpoint = "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT;

        Map<String, String> commonDeltaCatalogProperties = Map.ofEntries(
                entry("connector.name", "delta_lake"),
                entry("hive.metastore.uri", metastoreUri),
                entry("fs.native-s3.enabled", "true"),
                entry("fs.hadoop.enabled", "false"),
                entry("s3.endpoint", s3Endpoint),
                entry("s3.aws-access-key", Minio.MINIO_ROOT_USER),
                entry("s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD),
                entry("s3.path-style-access", "true"),
                entry("s3.region", Minio.MINIO_REGION),
                entry("delta.enable-non-concurrent-writes", "true"),
                entry("delta.register-table-procedure.enabled", "true"));

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
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
                        entry("fs.cache.enabled", "true"),
                        entry("fs.cache.directories", CACHE_DIRECTORY),
                        entry("fs.cache.max-disk-usage-percentages", "90")))
                .withCatalog("delta_non_cached", commonDeltaCatalogProperties)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("jmx", Map.of("connector.name", "jmx"))
                .build();

        trino.withTmpFs(Map.of("/tmp/cache", "rw,mode=777"));
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    public String getBucketName()
    {
        return BUCKET_NAME;
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
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
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
