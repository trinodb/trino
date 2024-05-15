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
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.UUID;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_TRINO_HIVE_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * This test requires these environment variables be set to connect to GCS:
 * - GCP_STORAGE_BUCKET: The name of the bucket to store tables in. The bucket must already exist.
 * - GCP_CREDENTIALS_KEY: A base64 encoded copy of the JSON authentication file for the service account used to connect to GCP.
 *   For example, `cat service-account-key.json | base64`
 */
@TestsEnvironment
public class EnvMultinodeGcs
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");
    private final String gcsTestDirectory = "env_multinode_gcs_" + UUID.randomUUID();
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvMultinodeGcs(DockerFiles dockerFiles, StandardMultinode multinode, Hadoop hadoop, EnvironmentConfig environmentConfig, PortBinder portBinder)
    {
        super(ImmutableList.of(multinode, hadoop));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        hadoopImagesVersion = environmentConfig.getHadoopImagesVersion();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String gcpBase64EncodedCredentials = requireEnv("GCP_CREDENTIALS_KEY");
        String gcpStorageBucket = requireEnv("GCP_STORAGE_BUCKET");

        byte[] gcpCredentialsBytes = Base64.getDecoder().decode(gcpBase64EncodedCredentials);
        String gcpCredentials = new String(gcpCredentialsBytes, UTF_8);
        File gcpCredentialsFile;
        try {
            gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".xml", PosixFilePermissions.asFileAttribute(fromString("rw-r--r--"))).toFile();
            gcpCredentialsFile.deleteOnExit();
            Files.write(gcpCredentialsFile.toPath(), gcpCredentialsBytes);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String containerGcpCredentialsFile = CONTAINER_TRINO_ETC + "gcp-credentials.json";
        builder.configureContainer(HADOOP, container -> {
            container.withCopyFileToContainer(
                    forHostPath(getCoreSiteOverrideXml(containerGcpCredentialsFile)),
                    "/docker/presto-product-tests/conf/environment/multinode-gcs/core-site-overrides.xml");
            container.withCopyFileToContainer(
                    forHostPath(getHiveSiteOverrideXml(gcpStorageBucket)),
                    "/docker/presto-product-tests/conf/environment/multinode-gcs/hive-site-overrides.xml");
            container.withCopyFileToContainer(
                    forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-gcs/apply-gcs-config.sh")),
                    CONTAINER_HADOOP_INIT_D + "apply-gcs-config.sh");
            container.withCopyFileToContainer(forHostPath(gcpCredentialsFile.toPath()), containerGcpCredentialsFile);
        });

        builder.configureContainer(COORDINATOR, container -> container
                .withEnv("GCP_CREDENTIALS", gcpCredentials));

        builder.configureContainer(WORKER, container -> container
                .withEnv("GCP_CREDENTIALS", gcpCredentials));

        builder.configureContainer(TESTS, container -> container
                .withEnv("GCP_STORAGE_BUCKET", gcpStorageBucket)
                .withEnv("GCP_TEST_DIRECTORY", gcsTestDirectory)
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));

        builder.addConnector("hive", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-gcs/hive.properties")), CONTAINER_TRINO_HIVE_PROPERTIES);
        builder.addConnector("delta_lake", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-gcs/delta.properties")), CONTAINER_TRINO_ETC + "/catalog/delta.properties");
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-gcs/iceberg.properties")), CONTAINER_TRINO_ETC + "/catalog/iceberg.properties");

        configureTempto(builder, dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-gcs/"));

        builder.addContainer(createSpark(forHostPath(gcpCredentialsFile.toPath())))
                .containerDependsOn("spark", HADOOP);
    }

    private DockerContainer createSpark(MountableFile credentialsFile)
    {
        String containerGcpCredentialsFile = "/spark/conf/gcp-credentials.json";
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, "spark")
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-gcs/spark-defaults.conf")),
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
                .withCopyFileToContainer(credentialsFile, containerGcpCredentialsFile)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }

    private Path getCoreSiteOverrideXml(String containerGcpCredentialsFilePath)
    {
        try {
            String coreSite = Files.readString(dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-gcs").getPath("core-site-overrides-template.xml"))
                    .replace("%GCP_CREDENTIALS_FILE_PATH%", containerGcpCredentialsFilePath);
            File coreSiteXml = Files.createTempFile("core-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            coreSiteXml.deleteOnExit();
            Files.writeString(coreSiteXml.toPath(), coreSite);
            return coreSiteXml.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getHiveSiteOverrideXml(String gcpStorageBucket)
    {
        try {
            String hiveSite = Files.readString(dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-gcs").getPath("hive-site-overrides-template.xml"))
                    .replace("%GCP_STORAGE_BUCKET%", gcpStorageBucket)
                    .replace("%GCP_WAREHOUSE_DIR%", gcsTestDirectory);
            File hiveSiteXml = Files.createTempFile("hive-site", ".xml", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            hiveSiteXml.deleteOnExit();
            Files.writeString(hiveSiteXml.toPath(), hiveSite);
            return hiveSiteXml.toPath();
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
