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
import io.trino.tests.product.launcher.env.common.EnvironmentExtender;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class SpoolingMinio
        implements EnvironmentExtender
{
    private static final String MINIO_SPOOLING_BUCKET = "spooling";
    private static final String MINIO_SPOOLING_CONTAINER_NAME = "spooling-minio";
    private static final String MINIO_ACCESS_KEY = "minio-access-key";
    private static final String MINIO_SECRET_KEY = "minio-secret-key";
    private static final String MINIO_RELEASE = "RELEASE.2025-01-20T14-49-07Z";

    private static final int MINIO_PORT = 9080; // minio uses 9000 by default, which conflicts with hadoop
    private static final int MINIO_CONSOLE_PORT = 9001;
    private final DockerFiles.ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public SpoolingMinio(DockerFiles dockerFiles, PortBinder portBinder)
    {
        requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/spooling");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createSpoolingMinioContainer());
        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(
                        forHostPath(configDir.getPath("spooling-manager.properties")),
                        CONTAINER_TRINO_ETC + "/spooling-manager.properties");

                if (isTrinoContainer(container.getLogicalName())) {
                    container.addEnv("CONTAINER_TRINO_CONFIG_PROPERTIES", CONTAINER_TRINO_CONFIG_PROPERTIES);
                    container.withCopyFileToContainer(
                            forHostPath(configDir.getPath("enable_spooling.sh")),
                            "/docker/presto-init.d/enable_spooling.sh");
                }
            }
        });
    }

    private DockerContainer createSpoolingMinioContainer()
    {
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

        DockerContainer container = new DockerContainer("minio/minio:" + MINIO_RELEASE, MINIO_SPOOLING_CONTAINER_NAME)
                .withEnv(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                        .buildOrThrow())
                .withCopyFileToContainer(forHostPath(minioBucketDirectory), "/data/" + MINIO_SPOOLING_BUCKET)
                .withCommand("server", "--address", format("0.0.0.0:%d", MINIO_PORT), "--console-address", format("0.0.0.0:%d", MINIO_CONSOLE_PORT), "/data")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(MINIO_PORT))
                .withStartupTimeout(Duration.ofMinutes(1));

        portBinder.exposePort(container, MINIO_PORT);
        portBinder.exposePort(container, MINIO_CONSOLE_PORT);

        return container;
    }
}
