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
package io.trino.testing.containers;

import io.trino.testing.TestingProperties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * A reusable Spark container for Hudi product tests.
 * <p>
 * This container provides a Spark Thrift Server configured with Hudi catalogs
 * that share the same Hive Metastore as Trino, enabling interoperability testing.
 * <p>
 * Key features:
 * <ul>
 *   <li>Spark Thrift Server on port 10213</li>
 *   <li>HoodieCatalog for spark_catalog</li>
 *   <li>HoodieSparkSessionExtension for Hudi SQL support</li>
 *   <li>Kryo serializer (required by Hudi)</li>
 *   <li>Supports HDFS or S3 storage backends</li>
 * </ul>
 */
public class SparkHudiContainer
        extends GenericContainer<SparkHudiContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/spark3-hudi";

    public static final String HOST_NAME = "spark";
    public static final int SPARK_THRIFT_PORT = 10213;

    // Default HDFS configuration
    private static final String SPARK_DEFAULTS_CONF = """
            spark.sql.catalogImplementation=hive
            spark.sql.warehouse.dir=hdfs://hadoop-master:9000/user/hive/warehouse
            spark.sql.hive.thriftServer.singleSession=false

            spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
            spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
            spark.serializer=org.apache.spark.serializer.KryoSerializer

            spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000
            spark.hive.metastore.uris=thrift://hadoop-master:9083
            spark.hive.metastore.warehouse.dir=hdfs://hadoop-master:9000/user/hive/warehouse
            spark.hive.metastore.schema.verification=false

            # HDFS configuration for reliable container communication
            spark.hadoop.dfs.client.use.datanode.hostname=true
            spark.hadoop.dfs.client.socket-timeout=180000
            spark.hadoop.dfs.datanode.socket.write.timeout=600000

            # Single-node HDFS configuration - disable datanode replacement on write failure
            # Required for Hudi writes in single-datanode environments
            spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable=false
            spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy=NEVER
            spark.hadoop.dfs.replication=1
            """;

    // S3 configuration template: accessKey, secretKey, endpoint, metastoreUri, warehouseDir
    private static final String SPARK_DEFAULTS_S3_CONF_TEMPLATE = """
            spark.sql.catalogImplementation=hive
            spark.sql.warehouse.dir=%5$s
            spark.sql.hive.thriftServer.singleSession=false

            spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
            spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
            spark.serializer=org.apache.spark.serializer.KryoSerializer

            spark.hive.metastore.uris=%4$s
            spark.hive.metastore.warehouse.dir=%5$s
            spark.hive.metastore.schema.verification=false

            # S3A configuration for MinIO
            spark.hadoop.fs.s3a.access.key=%1$s
            spark.hadoop.fs.s3a.secret.key=%2$s
            spark.hadoop.fs.s3a.endpoint=%3$s
            spark.hadoop.fs.s3a.path.style.access=true
            spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
            spark.hadoop.fs.s3a.connection.ssl.enabled=false
            """;

    private static final String LOG4J2_PROPERTIES = """
            rootLogger.level = WARN
            rootLogger.appenderRef.console.ref = console
            appender.console.type = Console
            appender.console.name = console
            appender.console.target = SYSTEM_ERR
            appender.console.layout.type = PatternLayout
            appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
            """;

    public SparkHudiContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public SparkHudiContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(SPARK_THRIFT_PORT);
        withEnv("HADOOP_USER_NAME", "hive");
        withCopyToContainer(
                Transferable.of(SPARK_DEFAULTS_CONF),
                "/spark/conf/spark-defaults.conf");
        withCopyToContainer(
                Transferable.of(LOG4J2_PROPERTIES),
                "/spark/conf/log4j2.properties");
        withCommand(
                "spark-submit",
                "--master", "local[*]",
                "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                "--name", "Thrift JDBC/ODBC Server",
                "--packages", "org.apache.spark:spark-avro_2.12:3.2.1",
                "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                "spark-internal");
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    /**
     * Configures Spark to use S3 storage instead of HDFS.
     *
     * @param accessKey S3 access key
     * @param secretKey S3 secret key
     * @param s3Host S3 endpoint host
     * @param s3Port S3 endpoint port
     * @param metastoreUri the Thrift URI of the Hive Metastore
     * @param warehouseDir the S3 warehouse directory (e.g., "s3a://bucket/warehouse")
     * @return this container for chaining
     */
    public SparkHudiContainer withS3Config(String accessKey, String secretKey, String s3Host, int s3Port, String metastoreUri, String warehouseDir)
    {
        String endpoint = "http://" + s3Host + ":" + s3Port;
        String sparkConf = SPARK_DEFAULTS_S3_CONF_TEMPLATE.formatted(accessKey, secretKey, endpoint, metastoreUri, warehouseDir);
        withCopyToContainer(
                Transferable.of(sparkConf),
                "/spark/conf/spark-defaults.conf");
        // Set environment variables for S3 access
        withEnv("AWS_ACCESS_KEY_ID", accessKey);
        withEnv("AWS_SECRET_ACCESS_KEY", secretKey);
        return this;
    }

    /**
     * Returns the JDBC URL for connecting to Spark Thrift Server from the host.
     */
    public String getJdbcUrl()
    {
        return "jdbc:hive2://" + getHost() + ":" + getMappedPort(SPARK_THRIFT_PORT);
    }

    /**
     * Returns the internal JDBC URL for connecting from other containers on the same network.
     */
    public String getInternalJdbcUrl()
    {
        return "jdbc:hive2://" + HOST_NAME + ":" + SPARK_THRIFT_PORT;
    }
}
