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
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Minio;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

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
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoWorker;
import static io.trino.tests.product.launcher.env.common.Minio.MINIO_CONTAINER_NAME;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodePostgresqlSpooling
        extends EnvironmentProvider
{
    private static final String S3_SPOOLING_BUCKET = "spooling";

    // Use non-default PostgreSQL port to avoid conflicts with locally installed PostgreSQL if any.
    public static final int POSTGRESQL_PORT = 15432;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodePostgresqlSpooling(StandardMultinode standardMultinode, Minio minio, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standardMultinode, minio));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-postgresql-spooling");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector(
                "postgresql",
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-postgresql/postgresql.properties")));
        builder.addContainer(createPostgreSql());

        // initialize spooling bucket in minio
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
                container.withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + S3_SPOOLING_BUCKET));

        String temptoConfig = "/docker/presto-product-tests/conf/tempto/tempto-configuration-spooling.yaml";
        builder.configureContainer(TESTS, testContainer -> {
            testContainer
                    .withCopyFileToContainer(forHostPath(configDir.getPath("tempto-configuration.yaml")), temptoConfig)
                    .withEnv("TEMPTO_CONFIG_FILES", temptoConfigFiles ->
                            temptoConfigFiles
                                    .map(files -> files + "," + temptoConfig)
                                    .orElse(temptoConfig));
        });

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(
                        forHostPath(configDir.getPath("spooling-manager.properties")),
                        CONTAINER_TRINO_ETC + "/spooling-manager.properties");

                if (isTrinoWorker(container.getLogicalName())) {
                    container.withCopyFileToContainer(
                            forHostPath(configDir.getPath("worker-config.properties")),
                            CONTAINER_TRINO_CONFIG_PROPERTIES);
                }
                else {
                    container.withCopyFileToContainer(
                            forHostPath(configDir.getPath("coordinator-config.properties")),
                            CONTAINER_TRINO_CONFIG_PROPERTIES);
                }
            }
        });
    }

    @SuppressWarnings("resource")
    private DockerContainer createPostgreSql()
    {
        // Use the oldest supported PostgreSQL version
        DockerContainer container = new DockerContainer("postgres:11", "postgresql")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("PGPORT", Integer.toString(POSTGRESQL_PORT))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(POSTGRESQL_PORT));

        portBinder.exposePort(container, POSTGRESQL_PORT);

        return container;
    }
}
