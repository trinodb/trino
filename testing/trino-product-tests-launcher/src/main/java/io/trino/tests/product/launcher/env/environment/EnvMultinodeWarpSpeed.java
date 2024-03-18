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

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Hadoop;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeWarpSpeed
        extends EnvironmentProvider
{
    private static final int SPARK_THRIFT_PORT = 10213;
    private static final String SPARK_CONTAINER_NAME = "spark";
    private static final String S3_BUCKET_NAME = "trino-ci-test";

    private final ResourceProvider configDir;
    private final PortBinder portBinder;
    private final String hadoopImagesVersion;

    @Inject
    public EnvMultinodeWarpSpeed(DockerFiles dockerFiles,
            PortBinder portBinder,
            StandardMultinode standardMultinode,
            EnvironmentConfig config,
            Hadoop hadoop,
            Minio minio)
    {
        super(standardMultinode, hadoop, minio);
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-warp-speed");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        // Using hdp3.1 so we are using Hive metastore with version close to versions of hive-*.jars Spark uses
        builder.configureContainer(HADOOP, container -> container.setDockerImageName("ghcr.io/trinodb/testing/hdp3.1-hive:" + hadoopImagesVersion));

        builder.configureContainer(COORDINATOR, this::configureTrinoContainer);
        builder.configureContainer(COORDINATOR, this::configureWarpResource);
        builder.configureContainer(WORKER, this::configureTrinoContainer);
        builder.configureContainer(WORKER, this::configureWarpStorage);
        builder.addConnector("warp_speed");

        builder.configureContainer(TESTS, dockerContainer -> dockerContainer.withEnv("S3_BUCKET", S3_BUCKET_NAME));

        builder.addContainer(createSparkContainer())
                // Ensure Hive metastore is up; Spark needs to access it during startup
                .containerDependsOn(SPARK_CONTAINER_NAME, HADOOP);

        // Initialize buckets in Minio
        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path minioBucketDirectory;
        try {
            minioBucketDirectory = Files.createTempDirectory("trino-ci-test", posixFilePermissions);
            minioBucketDirectory.toFile().deleteOnExit();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.configureContainer(MINIO_CONTAINER_NAME, container ->
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_BUCKET_NAME));
    }

    private void configureTrinoContainer(DockerContainer container)
    {
        container.withPrivilegedMode(true)
                .withCopyFileToContainer(forHostPath(configDir.getPath("trino/etc")), CONTAINER_TRINO_ETC)
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("jvm.config")),
                        CONTAINER_TRINO_JVM_CONFIG);
    }

    private void configureWarpResource(DockerContainer container)
    {
        portBinder.exposePort(container, 8089);
    }

    private void configureWarpStorage(DockerContainer container)
    {
        container.withCopyFileToContainer(
                forHostPath(configDir.getPath("setup-warp-speed.sh"), 0755),
                "/docker/presto-init.d/setup-warp-speed.sh");
    }

    private DockerContainer createSparkContainer()
    {
        DockerContainer container = new DockerContainer("ghcr.io/trinodb/testing/spark3-hudi:" + hadoopImagesVersion, SPARK_CONTAINER_NAME)
                .withCopyFileToContainer(forHostPath(configDir.getPath("spark-defaults.conf")), "/spark/conf/spark-defaults.conf")
                .waitingFor(forSelectedPorts(SPARK_THRIFT_PORT));

        portBinder.exposePort(container, SPARK_THRIFT_PORT);

        return container;
    }
}
