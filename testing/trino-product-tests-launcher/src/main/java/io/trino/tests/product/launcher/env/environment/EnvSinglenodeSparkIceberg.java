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
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.EnvironmentDefaults.HADOOP_BASE_IMAGE;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeSparkIceberg
        extends EnvironmentProvider
{
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");
    private static final File ICEBERG_SPARK_RUNTIME_JAR = new File("testing/trino-product-tests-launcher/target/iceberg-spark-runtime.jar");

    private static final int SPARK_THRIFT_PORT = 10213;
    private static final int LOCALSTACK_PORT = 4566;
    private static final String LOCALSTACK_CONTAINER = "localstack";
    private static final String LOCALSTACK_ENDPOINT_URL = "http://" + LOCALSTACK_CONTAINER + ":" + LOCALSTACK_PORT;
    private static final String AWS_REGION = "us-east-1";
    private static final String AWS_ACCESS_KEY_ID = "test";
    private static final String AWS_SECRET_ACCESS_KEY = "test";
    // Must be kept in sync with dep.iceberg.version in the root pom.xml
    private static final String ICEBERG_VERSION = "1.11.0";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvSinglenodeSparkIceberg(Standard standard, Hadoop hadoop, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        super(ImmutableList.of(standard, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
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

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container
                        .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
                        .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
                        .withEnv("AWS_REGION", AWS_REGION)
                        .withEnv("AWS_ENDPOINT_URL_KMS", LOCALSTACK_ENDPOINT_URL);
            }
        });

        builder.addContainer(createLocalStack());
        builder.addContainer(createSpark())
                .containerDependsOn("spark", HADOOP)
                .containerDependsOn("spark", LOCALSTACK_CONTAINER);

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer
                .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
                .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
                .withEnv("AWS_REGION", AWS_REGION)
                .withEnv("AWS_ENDPOINT_URL_KMS", LOCALSTACK_ENDPOINT_URL)
                // Binding instead of copying for avoiding OutOfMemoryError https://github.com/testcontainers/testcontainers-java/issues/2863
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));
    }

    @SuppressWarnings("resource")
    private DockerContainer createLocalStack()
    {
        DockerContainer container = new DockerContainer("localstack/localstack:4.14.0", LOCALSTACK_CONTAINER)
                .withEnv("SERVICES", "kms")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(LOCALSTACK_PORT));

        portBinder.exposePort(container, LOCALSTACK_PORT);

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createSpark()
    {
        // The spark4-iceberg image bundles iceberg-spark-runtime 1.10.x in /spark/jars/, which
        // takes startup-classloader precedence over jars resolved by --packages. Replace it with
        // 1.11.0 in /spark/jars/ so the SparkSessionCatalog class (referenced from spark-defaults.conf)
        // resolves to the 1.11 implementation. Required for V3 + KMS encryption to work.
        String sparkSubmit = String.join(
                " ",
                "spark-submit",
                "--master",
                "local[*]",
                "--class",
                "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                "--name",
                "'Thrift JDBC/ODBC Server'",
                "--packages",
                "org.apache.spark:spark-avro_2.13:4.0.0,org.apache.iceberg:iceberg-aws-bundle:" + ICEBERG_VERSION,
                "--conf",
                "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                "spark-internal");
        String[] command = {
                "bash", "-c",
                "rm -f /spark/jars/iceberg-spark-runtime-*.jar && exec " + sparkSubmit,
        };

        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark4-iceberg:" + hadoopImagesVersion, "spark")
                .withCopyFileToContainer(
                        forHostPath(ICEBERG_SPARK_RUNTIME_JAR.getAbsolutePath()),
                        "/spark/jars/iceberg-spark-" + ICEBERG_VERSION + ".jar")
                .withEnv("HADOOP_USER_NAME", "hive")
                .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
                .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
                .withEnv("AWS_REGION", AWS_REGION)
                .withEnv("AWS_ENDPOINT_URL_KMS", LOCALSTACK_ENDPOINT_URL)
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-spark-iceberg/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("common/spark/log4j2.properties")),
                        "/spark/conf/log4j2.properties")
                .withCommand(command)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }
}
