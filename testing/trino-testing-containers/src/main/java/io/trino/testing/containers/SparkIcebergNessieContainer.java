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
 * A Spark container configured for Iceberg with Nessie catalog.
 * <p>
 * This container provides a Spark Thrift Server configured with an Iceberg catalog
 * that uses Nessie for version-controlled catalog operations:
 * <ul>
 *   <li>Spark Thrift Server on port 10213</li>
 *   <li>Iceberg catalog (iceberg_test) using NessieCatalog implementation</li>
 *   <li>HDFS access via hadoop-master:9000 for data storage</li>
 *   <li>Nessie server for catalog metadata</li>
 * </ul>
 */
public class SparkIcebergNessieContainer
        extends GenericContainer<SparkIcebergNessieContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/spark4-iceberg";

    public static final String HOST_NAME = "spark";
    public static final int SPARK_THRIFT_PORT = 10213;

    private static final String SPARK_DEFAULTS_CONF = """
            spark.sql.catalog.iceberg_test=org.apache.iceberg.spark.SparkCatalog
            spark.sql.catalog.iceberg_test.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
            spark.sql.catalog.iceberg_test.uri=http://nessie-server:19120/api/v2
            spark.sql.catalog.iceberg_test.authentication.type=NONE
            spark.sql.catalog.iceberg_test.warehouse=hdfs://hadoop-master:9000/user/hive/warehouse
            spark.sql.catalog.iceberg_test.cache-enabled=false
            spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

            spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000
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

    public SparkIcebergNessieContainer()
    {
        this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
    }

    public SparkIcebergNessieContainer(String imageName)
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
