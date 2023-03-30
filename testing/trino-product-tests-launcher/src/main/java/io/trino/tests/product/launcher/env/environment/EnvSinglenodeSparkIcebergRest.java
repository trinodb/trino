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
package io.trino.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with Spark running against a REST server
 */
@TestsEnvironment
public class EnvSinglenodeSparkIcebergRest
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final int REST_SERVER_PORT = 8181;
    private static final String SPARK_CONTAINER_NAME = "spark";
    private static final String REST_CONTAINER_NAME = "iceberg-with-rest";
    private static final String REST_SERVER_IMAGE = "tabulario/iceberg-rest:0.2.0";
    private static final String CATALOG_WAREHOUSE = "hdfs://hadoop-master:9000/user/hive/warehouse";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvSinglenodeSparkIcebergRest(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createRESTContainer());
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath(
                "conf/environment/singlenode-spark-iceberg-rest/iceberg.properties")));
        builder.addContainer(createSparkContainer()).containerDependsOn(SPARK_CONTAINER_NAME, HADOOP);
    }

    @SuppressWarnings("resource")
    private DockerContainer createRESTContainer()
    {
        DockerContainer container = new DockerContainer(REST_SERVER_IMAGE, REST_CONTAINER_NAME)
                .withEnv("CATALOG_WAREHOUSE", CATALOG_WAREHOUSE)
                .withEnv("REST_PORT", Integer.toString(REST_SERVER_PORT))
                .withEnv("HADOOP_USER_NAME", "hive")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(REST_SERVER_PORT));

        portBinder.exposePort(container, REST_SERVER_PORT);
        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createSparkContainer()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, SPARK_CONTAINER_NAME)
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath(
                                "conf/environment/singlenode-spark-iceberg-rest/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCommand(
                        "spark-submit",
                        "--master", "local[*]",
                        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                        "--name", "Thrift JDBC/ODBC Server",
                        "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                        "spark-internal")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);
        return container;
    }
}
