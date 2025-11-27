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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Minio;
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
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeLanceMinio
        extends EnvironmentProvider
{
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");
    private static final String S3_BUCKET_NAME = "test-bucket";
    // TODO: use official image from Lance once https://github.com/lancedb/lance-spark/issues/102 is fixed
    private static final String SPARK_IMAGE = "docker.io/hluoulh/lance-spark:3.5.7-15";
    private static final String SPARK_CONTAINER_NAME = "spark";
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final String SPARK = "spark";
    private static final String MINIO = "minio";

    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeLanceMinio(StandardMultinode standardMultinode, Minio minio, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode, minio);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-lance-minio");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(TESTS, dockerContainer -> dockerContainer.withEnv("S3_BUCKET", S3_BUCKET_NAME));

        builder.addContainer(createSparkContainer()).containerDependsOn(SPARK, MINIO);
        // initialize buckets in minio
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
        {
            container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME);
        });

        builder.addConnector("lance", forHostPath(configDir.getPath("lance.properties")));

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));
    }

    @SuppressWarnings("resource")
    private DockerContainer createSparkContainer()
    {
        DockerContainer container = new DockerContainer(SPARK_IMAGE, SPARK_CONTAINER_NAME)
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("spark-defaults.conf")),
                        "/opt/spark/conf/spark-defaults.conf")
                .withEnv(ImmutableMap.of("HIVE_SERVER2_THRIFT_PORT", String.valueOf(SPARK_THRIFT_PORT)))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));
        portBinder.exposePort(container, SPARK_THRIFT_PORT);
        return container;
    }
}
