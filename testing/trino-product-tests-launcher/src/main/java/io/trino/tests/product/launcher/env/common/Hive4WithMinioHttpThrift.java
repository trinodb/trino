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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Hive 4 + MinIO with the standalone metastore serving Thrift-over-HTTP (for Trino {@code hive.metastore.uri=http://...}).
 * <p>
 * HiveServer2 is intentionally <strong>not</strong> started: with HTTP Thrift on the metastore, HS2 often never binds
 * its Thrift port within the launcher timeout, which caused multi-hour environment startup failures. Product smoke
 * tests for this feature only query Trino ({@code onTrino()}), which talks to HMS directly.
 */
public class Hive4WithMinioHttpThrift
        implements EnvironmentExtender
{
    public static final String METASTORE = "metastore";
    private static final int HIVE_METASTORE_PORT = 9083;
    private static final String APACHE_HIVE_IMAGE = "ghcr.io/trinodb/testing/hive4.0-hive";
    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");
    private static final String S3_BUCKET_NAME = "test-bucket";

    private final PortBinder portBinder;
    private final String hadoopImagesVersion;
    private final DockerFiles.ResourceProvider configDir;
    private final Minio minio;

    @Inject
    public Hive4WithMinioHttpThrift(
            DockerFiles dockerFiles,
            PortBinder portBinder,
            EnvironmentConfig config,
            Minio minio)
    {
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = requireNonNull(config, "config is null").getHadoopImagesVersion();
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/hive4-with-minio-http-thrift");
        this.minio = requireNonNull(minio, "minio is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createMetastoreServer());

        configureMinio(builder);
        configureTests(builder);
        configureTempto(builder, configDir);
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(minio);
    }

    private DockerContainer createMetastoreServer()
    {
        DockerContainer container = new DockerContainer(APACHE_HIVE_IMAGE + ":" + hadoopImagesVersion, METASTORE)
                .withEnv("SERVICE_NAME", "metastore")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("metastore-hive-site.xml")),
                        "/opt/hive/conf/hive-site.xml")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, HIVE_METASTORE_PORT);
        return container;
    }

    private void configureMinio(Environment.Builder builder)
    {
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
    }

    private void configureTests(Environment.Builder builder)
    {
        builder.configureContainer(TESTS, dockerContainer ->
                dockerContainer
                        .withEnv("S3_BUCKET", S3_BUCKET_NAME)
                        .withCopyFileToContainer(forHostPath(HIVE_JDBC_PROVIDER.getAbsolutePath()), "/docker/jdbc/hive-jdbc.jar"));
    }
}
