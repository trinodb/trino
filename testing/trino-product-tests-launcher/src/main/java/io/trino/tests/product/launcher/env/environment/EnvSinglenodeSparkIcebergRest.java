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
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with Spark running against a REST server
 */
@TestsEnvironment
public class EnvSinglenodeSparkIcebergRest
        extends EnvironmentProvider
{
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");

    private static final int SPARK_THRIFT_PORT = 10213;
    private static final int REST_SERVER_PORT = 8181;
    private static final String SPARK_CONTAINER_NAME = "spark";
    private static final String REST_CONTAINER_NAME = "iceberg-with-rest";
    // TODO Pin version once Iceberg community releases it
    private static final String REST_SERVER_IMAGE = "apache/iceberg-rest-fixture:latest";
    private static final String CATALOG_WAREHOUSE = "s3://test-bucket/default";
    private static final String S3_BUCKET_NAME = "test-bucket";

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvSinglenodeSparkIcebergRest(Standard standard, Hadoop hadoop, Minio minio, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder)
    {
        // TODO Remove Hadoop when replace Hadoop with MinIO in all Iceberg product tests
        super(ImmutableList.of(standard, hadoop, minio));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-spark-iceberg-rest");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createRESTContainer());
        builder.addConnector("iceberg", forHostPath(dockerFiles.getDockerFilesHostPath(
                "conf/environment/singlenode-spark-iceberg-rest/iceberg.properties")));
        builder.addContainer(createSparkContainer()).containerDependsOn(SPARK_CONTAINER_NAME, HADOOP);

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer
                // Binding instead of copying for avoiding OutOfMemoryError https://github.com/testcontainers/testcontainers-java/issues/2863
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));

        // Initialize buckets in Minio
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("test-bucket-contents", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME));

        configureTempto(builder, configDir);
    }

    @SuppressWarnings("resource")
    private DockerContainer createRESTContainer()
    {
        DockerContainer container = new DockerContainer(REST_SERVER_IMAGE, REST_CONTAINER_NAME)
                .withEnv("CATALOG_INCLUDE__CREDENTIALS", "true")
                .withEnv("CATALOG_WAREHOUSE", CATALOG_WAREHOUSE)
                .withEnv("CATALOG_URI", "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory")
                .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                .withEnv("AWS_REGION", "us-east-1")
                .withEnv("CATALOG_S3_ACCESS__KEY__ID", "minio-access-key")
                .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", "minio-secret-key")
                .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9080")
                .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                .withEnv("REST_PORT", Integer.toString(REST_SERVER_PORT))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(REST_SERVER_PORT));

        portBinder.exposePort(container, REST_SERVER_PORT);
        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createSparkContainer()
    {
        System.out.println(hadoopImagesVersion);
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-iceberg:" + hadoopImagesVersion, SPARK_CONTAINER_NAME)
                .withEnv("AWS_REGION", "us-east-1")
                .withEnv("AWS_ACCESS_KEY_ID", "minio-access-key")
                .withEnv("AWS_SECRET_ACCESS_KEY", "minio-secret-key")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath(
                                "conf/environment/singlenode-spark-iceberg-rest/spark-defaults.conf")),
                        "/spark/conf/spark-defaults.conf")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath(
                                "common/spark/log4j2.properties")),
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
}
