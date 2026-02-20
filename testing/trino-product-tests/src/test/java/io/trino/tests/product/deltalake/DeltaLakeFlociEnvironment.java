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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Floci;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Multi-node Delta Lake environment for reading S3 fixtures through Floci.
 */
public class DeltaLakeFlociEnvironment
        extends ProductTestEnvironment
{
    private static final String FLOCI_HOST_NAME = "floci";
    private static final String BUCKET_NAME = "delta-test-bucket";

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
        Map<String, String> storageProperties = Map.of(
                "hive.metastore.uri", metastoreUri,
                "fs.native-s3.enabled", "true",
                "fs.hadoop.enabled", "false",
                "s3.endpoint", s3Endpoint,
                "s3.aws-access-key", Floci.FLOCI_ACCESS_KEY,
                "s3.aws-secret-key", Floci.FLOCI_SECRET_KEY,
                "s3.path-style-access", "true",
                "s3.region", Floci.FLOCI_REGION);

        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withConfigProperty("node-scheduler.include-coordinator", "false")
                .withCatalog("delta", ImmutableMap.<String, String>builder()
                        .putAll(storageProperties)
                        .put("connector.name", "delta_lake")
                        .put("delta.register-table-procedure.enabled", "true")
                        .buildOrThrow())
                .withCatalog("hive", ImmutableMap.<String, String>builder()
                        .putAll(storageProperties)
                        .put("connector.name", "hive")
                        .put("hive.parquet.time-zone", "UTC")
                        .put("hive.rcfile.time-zone", "UTC")
                        .buildOrThrow())
                .withCatalog("iceberg", ImmutableMap.<String, String>builder()
                        .putAll(storageProperties)
                        .put("connector.name", "iceberg")
                        .put("iceberg.file-format", "PARQUET")
                        .buildOrThrow())
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    public void copyResources(String resourcePath, String target)
    {
        floci.copyResources(resourcePath, BUCKET_NAME, target);
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
