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
 * A reusable Spark container for Delta Lake product tests.
 * <p>
 * This container provides a Spark Thrift Server configured with Delta Lake
 * that shares the same Hive Metastore as Trino, enabling interoperability testing.
 * <p>
 * Key features:
 * <ul>
 *   <li>Spark Thrift Server on port 10213</li>
 *   <li>Delta Lake catalog (spark_catalog) with DeltaCatalog</li>
 *   <li>HDFS access via hadoop-master:9000</li>
 * </ul>
 */
public class SparkDeltaContainer
        extends GenericContainer<SparkDeltaContainer>
{
    private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/spark4-delta";

    public static final String HOST_NAME = "spark";
    public static final int SPARK_THRIFT_PORT = 10213;

    private static final String SPARK_DEFAULTS_CONF = """
            spark.sql.catalogImplementation=hive
            spark.sql.warehouse.dir=hdfs://hadoop-master:9000/user/hive/warehouse
            spark.sql.hive.thriftServer.singleSession=false

            spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
            spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

            spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000
            spark.hive.metastore.uris=thrift://hadoop-master:9083
            spark.hive.metastore.warehouse.dir=hdfs://hadoop-master:9000/user/hive/warehouse
            spark.hive.metastore.schema.verification=false

            # HDFS configuration for reliable container communication
            spark.hadoop.dfs.client.use.datanode.hostname=true
            spark.hadoop.dfs.client.socket-timeout=180000
            spark.hadoop.dfs.datanode.socket.write.timeout=600000
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
