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
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeAzure
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");

    private final DockerFiles.ResourceProvider configDir;
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopBaseImage;
    private final String hadoopImagesVersion;

    @Inject
    public EnvMultinodeAzure(DockerFiles dockerFiles, StandardMultinode standardMultinode, Hadoop hadoop, EnvironmentConfig environmentConfig, PortBinder portBinder)
    {
        super(ImmutableList.of(standardMultinode, hadoop));
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-azure");
        requireNonNull(environmentConfig, "environmentConfig is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        hadoopBaseImage = environmentConfig.getHadoopBaseImage();
        hadoopImagesVersion = environmentConfig.getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = hadoopBaseImage + ":" + hadoopImagesVersion;
        String schema = "test_" + randomNameSuffix();

        String abfsContainer = requireEnv("ABFS_CONTAINER");
        String abfsAccount = requireEnv("ABFS_ACCOUNT");
        builder.configureContainer(HADOOP, container -> {
            container.setDockerImageName(dockerImageName);
            container.withCopyFileToContainer(
                    forHostPath(getCoreSiteOverrideXml()),
                    "/docker/presto-product-tests/conf/environment/multinode-azure/core-site-overrides.xml");
            container.withCopyFileToContainer(
                    forHostPath(configDir.getPath("apply-azure-config.sh")),
                    CONTAINER_HADOOP_INIT_D + "apply-azure-config.sh");
            container
                    .withEnv("ABFS_CONTAINER", abfsContainer)
                    .withEnv("ABFS_ACCOUNT", abfsAccount)
                    .withEnv("ABFS_SCHEMA", schema);
            container.withCopyFileToContainer(
                    forHostPath(configDir.getPath("update-location.sh")),
                    CONTAINER_HADOOP_INIT_D + "update-location.sh");
        });

        String abfsAccessKey = requireEnv("ABFS_ACCESS_KEY");
        builder.configureContainer(COORDINATOR, container -> container
                .withEnv("ABFS_ACCOUNT", abfsAccount)
                .withEnv("ABFS_ACCESS_KEY", abfsAccessKey));

        builder.configureContainer(WORKER, container -> container
                .withEnv("ABFS_ACCOUNT", abfsAccount)
                .withEnv("ABFS_ACCESS_KEY", abfsAccessKey));

        String temptoConfig = "/docker/presto-product-tests/conf/tempto/tempto-configuration-abfs.yaml";
        builder.configureContainer(TESTS, container -> container
                .withEnv("ABFS_CONTAINER", abfsContainer)
                .withEnv("ABFS_ACCOUNT", abfsAccount)
                .withCopyFileToContainer(
                        forHostPath(getTemptoConfiguration(schema)),
                        temptoConfig)
                .withEnv("TEMPTO_CONFIG_FILES", temptoConfigFiles ->
                        temptoConfigFiles
                                .map(files -> files + "," + temptoConfig)
                                .orElse(temptoConfig))
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES);
        builder.addConnector("delta_lake", forHostPath(configDir.getPath("delta.properties")), CONTAINER_TRINO_ETC + "/catalog/delta.properties");
        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")), CONTAINER_TRINO_ETC + "/catalog/iceberg.properties");

        builder.addContainer(createSpark())
                .containerDependsOn("spark", HADOOP);
    }

    private DockerContainer createSpark()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, "spark")
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(getSparkConf()),
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

    private Path getSparkConf()
    {
        try {
            String sparkConf = Files.readString(configDir.getPath("spark-defaults.conf"))
                    .replace("%ABFS_ACCOUNT%", requireEnv("ABFS_ACCOUNT"))
                    .replace("%ABFS_ACCESS_KEY%", requireEnv("ABFS_ACCESS_KEY"));
            File sparkConfFile = Files.createTempFile("spark-defaults", ".conf", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            sparkConfFile.deleteOnExit();
            Files.writeString(sparkConfFile.toPath(), sparkConf);
            return sparkConfFile.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getCoreSiteOverrideXml()
    {
        try {
            String coreSite = Files.readString(configDir.getPath("core-site-overrides-template.xml"))
                    .replace("%ABFS_ACCOUNT%", requireEnv("ABFS_ACCOUNT"))
                    .replace("%ABFS_ACCESS_KEY%", requireEnv("ABFS_ACCESS_KEY"));
            File coreSiteXml = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            coreSiteXml.deleteOnExit();
            Files.writeString(coreSiteXml.toPath(), coreSite);
            return coreSiteXml.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getTemptoConfiguration(String schema)
    {
        try {
            File temptoConfiguration = Files.createTempFile("tempto-configuration", ".yaml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            temptoConfiguration.deleteOnExit();
            String contents = """
databases:
    presto:
        abfs_schema: "%s"
                    """.formatted(schema);
            Files.writeString(temptoConfiguration.toPath(), contents);
            return temptoConfiguration.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }
}
