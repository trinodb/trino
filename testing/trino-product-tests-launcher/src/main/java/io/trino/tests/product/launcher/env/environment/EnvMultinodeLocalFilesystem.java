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
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.testcontainers.containers.BindMode.READ_WRITE;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with the native local file system connected to every coordinator and worker container
 * via the same bind-mounted host directory, simulating a shared mount (e.g. NFS) available at
 * the same path on every node of a real cluster.
 */
@TestsEnvironment
public final class EnvMultinodeLocalFilesystem
        extends EnvironmentProvider
{
    private static final String CONTAINER_MOUNT_PATH = "/mnt/trino-local-fs";

    private final ResourceProvider configDir;
    private final Closer closer = Closer.create();

    @Inject
    public EnvMultinodeLocalFilesystem(StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standardMultinode));
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("conf/environment/multinode-local-filesystem");
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        closer.close();
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String sharedMountHostDirectory = createSharedMountHostDirectory().toString();

        // Bind the same host directory at the same container path on every container
        // (coordinator and every worker), so they all see a literal shared file system,
        // the closest available proxy for "N machines mounting the same NFS export".
        builder.configureContainers(container ->
                container.withFileSystemBind(sharedMountHostDirectory, CONTAINER_MOUNT_PATH, READ_WRITE));

        builder.addConnector("hive", forHostPath(configDir.getPath("hive.properties")));
        builder.addConnector("iceberg", forHostPath(configDir.getPath("iceberg.properties")));
        builder.addConnector(
                "delta_lake",
                forHostPath(configDir.getPath("delta.properties")),
                CONTAINER_TRINO_ETC + "/catalog/delta.properties");
    }

    private Path createSharedMountHostDirectory()
    {
        try {
            // Cannot use Files.createTempDirectory() because on Mac by default it uses /var/folders/
            // which is not visible to Docker Desktop/OrbStack.
            Path temporaryDirectory = Files.createDirectory(Path.of("/tmp/trino-local-fs-" + randomUUID()));
            closer.register(() -> deleteRecursively(temporaryDirectory, ALLOW_INSECURE));
            return temporaryDirectory;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
