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

package io.trino.testing.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.PullResponseItem;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.Closeable;

import static com.github.dockerjava.api.model.Ports.Binding.bindPort;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.ConditionalPullPolicy.TESTCONTAINERS_NEVER_PULL;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class TestContainers
{
    // To reuse container set TESTCONTAINERS_REUSE_ENABLE=true environment variable.
    // TESTCONTAINERS_REUSE_ENABLE is an environment variable defined in testcontainers library.
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = parseBoolean(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    private TestContainers() {}

    // You should not close the container directly if you want to reuse it.
    // Instead you should close closeable returned by {@link this::startOrReuse}
    public static Closeable startOrReuse(GenericContainer<?> container)
    {
        boolean reuse = TestcontainersConfiguration.getInstance().environmentSupportsReuse();
        checkState(reuse == TESTCONTAINERS_REUSE_ENABLE, "Unable to enable or disable container reuse");

        container.withReuse(TESTCONTAINERS_REUSE_ENABLE);
        container.start();
        if (reuse) {
            return () -> {};
        }
        return container::stop;
    }

    public static String getPathFromClassPathResource(String resourcePath)
    {
        return forClasspathResource(resourcePath)
                // Container fails to mount jar:file:/<host_path>!<resource_path> resources
                // This ensures that JAR resources are being copied out to tmp locations
                // and mounted from there.
                .getResolvedPath();
    }

    public static void exposeFixedPorts(GenericContainer<?> container)
    {
        checkState(System.getenv("CONTINUOUS_INTEGRATION") == null, "" +
                "Exposing fixed ports should not be used in regular test code. This could break parallel test execution. " +
                "This method is supposed to be invoked from local development helpers only e.g. QueryRunner.main(), " +
                "hence it should never run on CI");

        container.withCreateContainerCmdModifier(cmd -> cmd
                .withHostConfig(requireNonNull(cmd.getHostConfig(), "hostConfig is null")
                        .withPortBindings(container.getExposedPorts().stream()
                                .map(exposedPort -> new PortBinding(bindPort(exposedPort), new ExposedPort(exposedPort)))
                                .collect(toImmutableList()))));
    }

    public static DockerArchitectureInfo getDockerArchitectureInfo(DockerImageName imageName)
    {
        checkState(!TESTCONTAINERS_NEVER_PULL, "Cannot get arch for image %s without pulling it, and pulling is forbidden", imageName);
        DockerClient client = DockerClientFactory.lazyClient();
        if (!imageExists(client, imageName)) {
            pullImage(client, imageName);
        }
        return new DockerArchitectureInfo(DockerArchitecture.fromString(client.versionCmd().exec().getArch()), DockerArchitecture.fromString(getImageArch(client, imageName)));
    }

    private static String getImageArch(DockerClient client, DockerImageName imageName)
    {
        return client.inspectImageCmd(imageName.asCanonicalNameString())
                .exec()
                .getArch()
                .toLowerCase(ENGLISH);
    }

    private static boolean imageExists(DockerClient client, DockerImageName imageName)
    {
        try {
            getImageArch(client, imageName);
            return true;
        }
        catch (NotFoundException e) {
            return false;
        }
    }

    private static void pullImage(DockerClient client, DockerImageName imageName)
    {
        try {
            client.pullImageCmd(imageName.asCanonicalNameString()).exec(new PullImageResultCallback() {
                @Override
                public void onNext(PullResponseItem item)
                {
                    String progress = item.getProgress();
                    if (progress != null) {
                        System.err.println(padEnd(imageName.asCanonicalNameString() + ":" + item.getId(), 50, ' ') + ' ' + progress);
                    }
                }
            }).awaitCompletion();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public record DockerArchitectureInfo(DockerArchitecture hostArch, DockerArchitecture imageArch) {}

    public enum DockerArchitecture
    {
        ARM64,
        AMD64,
        PPC64;

        public static DockerArchitecture fromString(String value)
        {
            return switch (value.toLowerCase(ENGLISH)) {
                case "linux/arm64", "arm64" -> ARM64;
                case "linux/amd64", "amd64" -> AMD64;
                case "ppc64", "ppc64le" -> PPC64;
                default -> throw new IllegalArgumentException("Unrecognized docker image architecture: " + value);
            };
        }
    }
}
