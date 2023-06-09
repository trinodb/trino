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
import com.google.inject.Inject;
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

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeSparkIcebergNessie
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final int NESSIE_PORT = 19120;
    private static final String NESSIE_VERSION = "0.59.0";
    private static final String SPARK = "spark";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvSinglenodeSparkIcebergNessie(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createNessieContainer());
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg-nessie/iceberg.properties")));

        builder.addContainer(createSparkContainer()).containerDependsOn(SPARK, HADOOP);
    }

    @SuppressWarnings("resource")
    private DockerContainer createSparkContainer()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, SPARK)
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg-nessie/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("common/spark/log4j2.properties")),
                        "/spark/conf/log4j2.properties")
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

    private DockerContainer createNessieContainer()
    {
        DockerContainer container = new DockerContainer("projectnessie/nessie:" + NESSIE_VERSION, "nessie-server")
                .withEnv("NESSIE_VERSION_STORE_TYPE", "INMEMORY")
                .withEnv("QUARKUS_HTTP_PORT", Integer.valueOf(NESSIE_PORT).toString())
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(NESSIE_PORT));

        portBinder.exposePort(container, NESSIE_PORT);
        return container;
    }
}
