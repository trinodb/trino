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

import com.google.common.collect.ImmutableMap;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.HADOOP;
import static io.trino.tests.product.launcher.env.common.Hadoop.CONTAINER_HADOOP_INIT_D;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class Minio
        implements EnvironmentExtender
{
    private final DockerFiles dockerFiles;

    public static final String MINIO_CONTAINER_NAME = "minio";

    private static final String MINIO_ACCESS_KEY = "minio-access-key";
    private static final String MINIO_SECRET_KEY = "minio-secret-key";
    private static final String MINIO_RELEASE = "RELEASE.2022-05-26T05-48-41Z";

    private static final int MINIO_PORT = 9080; // minio uses 9000 by default, which conflicts with hadoop
    private static final int MINIO_CONSOLE_PORT = 9001;

    private final PortBinder portBinder;

    @Inject
    public Minio(DockerFiles dockerFiles, PortBinder portBinder)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createMinioContainer());

        builder.configureContainers(container -> {
            if (container.getLogicalName().equals(HADOOP)) {
                container.withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("common/minio/apply-minio-config.sh")),
                        CONTAINER_HADOOP_INIT_D + "apply-minio-config.sh");
            }
        });
    }

    private DockerContainer createMinioContainer()
    {
        DockerContainer container = new DockerContainer("minio/minio:" + MINIO_RELEASE, MINIO_CONTAINER_NAME)
                .withEnv(ImmutableMap.<String, String>builder()
                        .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                        .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                        .buildOrThrow())
                .withCommand("server", "--address", format("0.0.0.0:%d", MINIO_PORT), "--console-address", format("0.0.0.0:%d", MINIO_CONSOLE_PORT), "/data")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(MINIO_PORT))
                .withStartupTimeout(Duration.ofMinutes(1));

        portBinder.exposePort(container, MINIO_PORT);
        portBinder.exposePort(container, MINIO_CONSOLE_PORT);

        return container;
    }
}
