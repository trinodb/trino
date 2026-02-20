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
import io.trino.testing.containers.SparkDeltaContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Single-node Delta Lake environment with Spark, Hive Metastore, and Floci storage.
 */
public class DeltaLakeOssEnvironment
        extends ProductTestEnvironment
{
    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver", e);
        }
    }

    private static final String FLOCI_HOST_NAME = "floci";
    private static final String BUCKET_NAME = "delta-oss-test-bucket";

    private Network network;
    private Floci floci;
    private HadoopContainer hadoop;
    private SparkDeltaContainer spark;
    private TrinoContainer trino;

    @Override
    public void start()
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

        spark = new SparkDeltaContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkDeltaContainer.HOST_NAME)
                .withS3Config(s3Endpoint, Floci.FLOCI_ACCESS_KEY, Floci.FLOCI_SECRET_KEY, Floci.FLOCI_REGION)
                .withWarehouseDir("s3a://" + BUCKET_NAME + "/warehouse")
                .build();
        spark.dependsOn(hadoop);
        spark.start();

        String metastoreUri = "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("delta", Map.ofEntries(
                        Map.entry("connector.name", "delta_lake"),
                        Map.entry("hive.metastore.uri", metastoreUri),
                        Map.entry("fs.native-s3.enabled", "true"),
                        Map.entry("fs.hadoop.enabled", "false"),
                        Map.entry("s3.endpoint", s3Endpoint),
                        Map.entry("s3.aws-access-key", Floci.FLOCI_ACCESS_KEY),
                        Map.entry("s3.aws-secret-key", Floci.FLOCI_SECRET_KEY),
                        Map.entry("s3.path-style-access", "true"),
                        Map.entry("s3.region", Floci.FLOCI_REGION),
                        Map.entry("delta.enable-non-concurrent-writes", "true"),
                        Map.entry("delta.hive-catalog-name", "hive")))
                .withCatalog("hive", Map.ofEntries(
                        Map.entry("connector.name", "hive"),
                        Map.entry("hive.metastore.uri", metastoreUri),
                        Map.entry("hive.non-managed-table-writes-enabled", "true"),
                        Map.entry("fs.native-s3.enabled", "true"),
                        Map.entry("fs.hadoop.enabled", "false"),
                        Map.entry("s3.endpoint", s3Endpoint),
                        Map.entry("s3.aws-access-key", Floci.FLOCI_ACCESS_KEY),
                        Map.entry("s3.aws-secret-key", Floci.FLOCI_SECRET_KEY),
                        Map.entry("s3.path-style-access", "true"),
                        Map.entry("s3.region", Floci.FLOCI_REGION),
                        Map.entry("hive.hive-views.enabled", "true"),
                        Map.entry("hive.delta-lake-catalog-name", "delta"),
                        Map.entry("hive.parquet.time-zone", "UTC"),
                        Map.entry("hive.rcfile.time-zone", "UTC")))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        TrinoProductTestContainer.startAndWait(trino);
    }

    public Connection createSparkConnection()
            throws SQLException
    {
        return DriverManager.getConnection(spark.getJdbcUrl(), "hive", "");
    }

    public QueryResult executeSpark(String sql)
    {
        try (Connection connection = createSparkConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return QueryResult.forResultSet(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark query: " + sql, e);
        }
    }

    public int executeSparkUpdate(String sql)
    {
        try (Connection connection = createSparkConnection();
                Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark update: " + sql, e);
        }
    }

    public List<String> listObjects(String prefix)
    {
        return floci.listObjects(BUCKET_NAME, prefix);
    }

    public void deleteObject(String key)
    {
        floci.deleteObject(BUCKET_NAME, key);
    }

    public void deleteObjects(String prefix)
    {
        for (String key : floci.listObjects(BUCKET_NAME, prefix)) {
            floci.deleteObject(BUCKET_NAME, key);
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
        if (spark != null) {
            spark.close();
            spark = null;
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
