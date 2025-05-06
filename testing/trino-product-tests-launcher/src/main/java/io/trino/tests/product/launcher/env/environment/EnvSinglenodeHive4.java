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
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvSinglenodeHive4
        extends EnvironmentProvider
{
    public static final int HIVE_SERVER_PORT = 10000;
    public static final int HIVE_METASTORE_PORT = 9083;
    public static final String CONTAINER_HADOOP_INIT_D = "/etc/hadoop-init.d/";

    private static final File HIVE_JDBC_PROVIDER = new File("testing/trino-product-tests-launcher/target/hive-jdbc.jar");
    private static final String HIVE_OSS_IMAGE = "ghcr.io/trinodb/testing/hive4.0-hive";
    private static final String HIVESERVER2 = "hiveserver2";
    private static final String METASTORE = "metastore";
    private static final String S3_BUCKET_NAME = "test-bucket";

    private final PortBinder portBinder;
    private final String hadoopImagesVersion;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvSinglenodeHive4(Standard standard, DockerFiles dockerFiles, EnvironmentConfig config, PortBinder portBinder, Minio minio)
    {
        super(ImmutableList.of(standard, minio));
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.hadoopImagesVersion = config.getHadoopImagesVersion();
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/singlenode-hive4");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createMetastoreServer());
        builder.addContainer(createHiveserver2());
        builder.containerDependsOn(HIVESERVER2, METASTORE);

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));

        builder.configureContainer(TESTS, dockerContainer ->
                dockerContainer.withEnv("S3_BUCKET", S3_BUCKET_NAME)
                // Binding instead of copying for avoiding OutOfMemoryError https://github.com/testcontainers/testcontainers-java/issues/2863
                .withFileSystemBind(HIVE_JDBC_PROVIDER.getParent(), "/docker/jdbc", BindMode.READ_ONLY));

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

    private DockerContainer createMetastoreServer()
    {
        DockerContainer container = new DockerContainer(HIVE_OSS_IMAGE + ":" + hadoopImagesVersion, METASTORE)
                .withEnv("SERVICE_NAME", "metastore")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("hive-site.xml")),
                        "/opt/hive/conf/hive-site.xml")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, HIVE_METASTORE_PORT);
        return container;
    }

    private DockerContainer createHiveserver2()
    {
        DockerContainer container = new DockerContainer(HIVE_OSS_IMAGE + ":" + hadoopImagesVersion, HIVESERVER2)
                .withEnv("SERVICE_NAME", "hiveserver2")
                .withEnv("SERVICE_OPTS", "-Xmx1G -Dhive.metastore.uris=%s".formatted(URI.create("thrift://%s:%d".formatted(METASTORE, HIVE_METASTORE_PORT))))
                .withEnv("IS_RESUME", "true")
                .withEnv("AWS_ACCESS_KEY_ID", "minio-access-key")
                .withEnv("AWS_SECRET_KEY", "minio-secret-key")
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("hive-site.xml")),
                        "/opt/hive/conf/hive-site.xml")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(HIVE_SERVER_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, HIVE_SERVER_PORT);
        portBinder.exposePort(container, 10002);
        portBinder.exposePort(container, 9000);
        portBinder.exposePort(container, 8020);
        portBinder.exposePort(container, 9870);
        portBinder.exposePort(container, 9868);
        return container;
    }
}
