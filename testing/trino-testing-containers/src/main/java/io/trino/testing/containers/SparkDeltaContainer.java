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
import java.util.Optional;

/**
 * A reusable Spark container for Delta Lake product tests.
 * <p>
 * This container provides a Spark Thrift Server configured with Delta Lake
 * that shares the same Hive Metastore as Trino, enabling interoperability testing.
 * <p>
 * Key features:
 * <ul>
 *   <li>Spark Thrift Server on port 10213</li>
 *   <li>Delta Lake catalog (spark_catalog) with DeltaCatalog</li>
 *   <li>HDFS access via hadoop-master:9000 (default) or S3 via Minio</li>
 * </ul>
 * <p>
 * Storage can be configured to use S3-compatible storage (like Minio) instead of HDFS
 * by using {@link #withS3Config(String, String, String, String)} and {@link #withWarehouseDir(String)}.
 */
public class SparkDeltaContainer
        extends GenericContainer<SparkDeltaContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/spark4-delta";

    public static final String HOST_NAME = "spark";
    public static final int SPARK_THRIFT_PORT = 10213;

    private Optional<S3Config> s3Config = Optional.empty();
    private Optional<String> warehouseDir = Optional.empty();

    public record S3Config(String endpoint, String accessKey, String secretKey, String region) {}

    private static final String LOG4J2_PROPERTIES = """
            rootLogger.level = WARN
            rootLogger.appenderRef.console.ref = console
            appender.console.type = Console
            appender.console.name = console
            appender.console.target = SYSTEM_ERR
            appender.console.layout.type = PatternLayout
            appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
            """;

    // Startup script that waits for config file then runs spark-submit
    private static final String STARTUP_SCRIPT = """
            #!/bin/bash
            # Wait for config file to be copied (up to 30 seconds)
            for i in {1..30}; do
                if [ -f /spark/conf/spark-defaults.conf ]; then
                    echo "Config file found, starting Spark..."
                    break
                fi
                echo "Waiting for config file... ($i)"
                sleep 1
            done
            exec spark-submit \\
                --master 'local[*]' \\
                --driver-memory 3g \\
                --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \\
                --name 'Thrift JDBC/ODBC Server' \\
                --packages 'org.apache.spark:spark-avro_2.12:3.2.1' \\
                --properties-file /spark/conf/spark-defaults.conf \\
                --conf spark.hive.server2.thrift.port=%d \\
                spark-internal
            """.formatted(SPARK_THRIFT_PORT);

    public SparkDeltaContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public SparkDeltaContainer(String imageName)
    {
        super(DockerImageName.parse(imageName));
        withExposedPorts(SPARK_THRIFT_PORT);
        withEnv("HADOOP_USER_NAME", "hive");
        withCopyToContainer(
                Transferable.of(LOG4J2_PROPERTIES),
                "/spark/conf/log4j2.properties");
        // Use startup script that waits for config file
        withCopyToContainer(
                Transferable.of(STARTUP_SCRIPT, 0755),
                "/spark/conf/startup.sh");
        withCommand("/spark/conf/startup.sh");
        waitingFor(Wait.forListeningPort()
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    public SparkDeltaContainer withS3Config(String endpoint, String accessKey, String secretKey, String region)
    {
        this.s3Config = Optional.of(new S3Config(endpoint, accessKey, secretKey, region));
        // Set environment variables for S3 access (fallback if config file not read in time)
        withEnv("AWS_ACCESS_KEY_ID", accessKey);
        withEnv("AWS_SECRET_ACCESS_KEY", secretKey);
        return this;
    }

    public SparkDeltaContainer withWarehouseDir(String warehouseDir)
    {
        this.warehouseDir = Optional.of(warehouseDir);
        return this;
    }

    /**
     * Finalizes the container configuration. Must be called after all configuration
     * methods (withS3Config, withWarehouseDir) but before start().
     *
     * @return this container for chaining
     */
    public SparkDeltaContainer build()
    {
        String config = generateSparkDefaultsConf();
        withCopyToContainer(
                Transferable.of(config),
                "/spark/conf/spark-defaults.conf");
        return this;
    }

    private String generateSparkDefaultsConf()
    {
        StringBuilder config = new StringBuilder();
        config.append("spark.sql.catalogImplementation=hive\n");
        config.append("spark.sql.hive.thriftServer.singleSession=false\n");
        config.append("spark.driver.memory=3g\n");
        config.append("\n");
        config.append("spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension\n");
        config.append("spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\n");
        config.append("\n");
        config.append("spark.hive.metastore.uris=thrift://hadoop-master:9083\n");
        config.append("spark.hive.metastore.schema.verification=false\n");
        config.append("\n");

        if (s3Config.isPresent()) {
            S3Config s3 = s3Config.get();
            String warehouse = warehouseDir.orElse("s3a://default/warehouse");
            config.append("# S3 configuration\n");
            config.append("spark.sql.warehouse.dir=").append(warehouse).append("\n");
            config.append("spark.hive.metastore.warehouse.dir=").append(warehouse).append("\n");
            config.append("spark.hadoop.fs.s3a.endpoint=").append(s3.endpoint()).append("\n");
            config.append("spark.hadoop.fs.s3a.access.key=").append(s3.accessKey()).append("\n");
            config.append("spark.hadoop.fs.s3a.secret.key=").append(s3.secretKey()).append("\n");
            config.append("spark.hadoop.fs.s3a.path.style.access=true\n");
            config.append("spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n");
            config.append("spark.hadoop.fs.s3a.connection.ssl.enabled=false\n");
            config.append("spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\n");
            // Additional S3A tuning for better reliability
            config.append("spark.hadoop.fs.s3a.threads.keepalivetime=6000\n");
            config.append("spark.hadoop.fs.s3a.connection.establish.timeout=3000\n");
            config.append("spark.hadoop.fs.s3a.connection.timeout=200000\n");
            // Map s3:// and s3n:// schemes to S3AFileSystem so tests using those URIs work with OSS Spark
            // Note: S3AFileSystem reads config from fs.s3a.* properties regardless of scheme used
            config.append("spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n");
            config.append("spark.hadoop.fs.s3n.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n");
            // Set multipart purge age in seconds (required because default may use duration string)
            config.append("spark.hadoop.fs.s3a.multipart.purge.age=86400\n");
        }
        else {
            String warehouse = warehouseDir.orElse("hdfs://hadoop-master:9000/user/hive/warehouse");
            config.append("# HDFS configuration\n");
            config.append("spark.sql.warehouse.dir=").append(warehouse).append("\n");
            config.append("spark.hive.metastore.warehouse.dir=").append(warehouse).append("\n");
            config.append("spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000\n");
            config.append("spark.hadoop.dfs.client.use.datanode.hostname=true\n");
            config.append("spark.hadoop.dfs.client.socket-timeout=180000\n");
            config.append("spark.hadoop.dfs.datanode.socket.write.timeout=600000\n");
        }

        return config.toString();
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
