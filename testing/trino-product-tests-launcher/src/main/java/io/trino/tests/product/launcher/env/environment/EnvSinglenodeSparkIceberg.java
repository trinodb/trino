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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.File;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentDefaults.HADOOP_BASE_IMAGE;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeSparkIceberg
        extends EnvironmentProvider
{
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");

    private static final int SPARK_THRIFT_PORT = 10213;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvSinglenodeSparkIceberg(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-spark-iceberg");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName(HADOOP_BASE_IMAGE);
            container.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg/apply-hive-config-for-iceberg.sh")),
                    CONTAINER_HADOOP_INIT_D + "/apply-hive-config-for-iceberg.sh");
        });

        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg/iceberg.properties")));

        builder.addContainer(createSpark())
                .containerDependsOn("spark", HADOOP);

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer
                // Binding instead of copying for avoiding OutOfMemoryError https://github.com/testcontainers/testcontainers-java/issues/2863
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));

        configureTempto(builder, configDir);
    }

    @SuppressWarnings("resource")
    private DockerContainer createSpark()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, "spark")
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("common/spark/log4j2.properties")),
                        "/spark/conf/log4j2.properties")
                .withCommand(
                        "spark-submit",
                        "--master", "local[*]",
                        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                        "--name", "Thrift JDBC/ODBC Server",
                        "--packages", "org.apache.spark:spark-avro_2.12:3.2.1",
                        "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                        "spark-internal")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }
}
