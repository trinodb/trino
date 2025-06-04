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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HostAndPort.fromParts;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

public abstract class BaseTestContainer
        implements AutoCloseable
{
    private final Logger log;

    private final String hostName;
    private final Set<Integer> ports;
    private final Map<String, String> filesToMount;
    private final Map<String, String> envVars;
    private final Optional<Network> network;
    private final int startupRetryLimit;

    private GenericContainer<?> container;

    protected BaseTestContainer(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit)
    {
        checkArgument(startupRetryLimit > 0, "startupRetryLimit needs to be greater or equal to 0");
        this.log = Logger.get(this.getClass());
        this.container = new GenericContainer<>(requireNonNull(image, "image is null"));
        this.ports = requireNonNull(ports, "ports is null");
        this.hostName = requireNonNull(hostName, "hostName is null");
        this.filesToMount = requireNonNull(filesToMount, "filesToMount is null");
        this.envVars = requireNonNull(envVars, "envVars is null");
        this.network = requireNonNull(network, "network is null");
        this.startupRetryLimit = startupRetryLimit;
        setupContainer();
    }

    protected void setupContainer()
    {
        for (int port : this.ports) {
            container.addExposedPort(port);
        }
        filesToMount.forEach((dockerPath, filePath) -> container.withCopyFileToContainer(forHostPath(filePath), dockerPath));
        container.withEnv(envVars);
        container.withCreateContainerCmdModifier(c -> c.withHostName(hostName))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forListeningPort())
                .withStartupTimeout(Duration.ofMinutes(5));
        network.ifPresent(net -> container.withNetwork(net).withNetworkAliases(hostName));
    }

    protected void withRunCommand(List<String> runCommand)
    {
        container.withCommand(runCommand.toArray(new String[runCommand.size()]));
    }

    protected void withLogConsumer(Consumer<OutputFrame> logConsumer)
    {
        container.withLogConsumer(logConsumer);
    }

    protected void copyResourceToContainer(String resourcePath, String dockerPath)
    {
        container.withCopyFileToContainer(
                forHostPath(
                        forClasspathResource(resourcePath)
                                // Container fails to mount jar:file:/<host_path>!<resource_path> resources
                                // This assures that JAR resources are being copied out to tmp locations
                                // and mounted from there.
                                .getResolvedPath()),
                dockerPath);
    }

    protected void mountDirectory(String hostPath, String dockerPath)
    {
        container.addFileSystemBind(hostPath, dockerPath, BindMode.READ_WRITE);
    }

    protected void withCreateContainerModifier(Consumer<CreateContainerCmd> modifier)
    {
        container.withCreateContainerCmdModifier(modifier);
    }

    protected HostAndPort getMappedHostAndPortForExposedPort(int exposedPort)
    {
        return fromParts(container.getHost(), container.getMappedPort(exposedPort));
    }

    public void start()
    {
        GenericContainer<?> container = this.container;
        try {
            Failsafe.with(RetryPolicy.builder()
                            .withMaxRetries(startupRetryLimit)
                            .onRetry(event -> log.warn(
                                    "%s initialization failed (attempt %s), will retry. Exception: %s",
                                    this.getClass().getSimpleName(),
                                    event.getAttemptCount(),
                                    event.getLastException().getMessage()))
                            .build())
                    .get(() -> TestContainers.startOrReuse(container));
        }
        catch (Throwable e) {
            try (container) {
                throw e;
            }
        }
    }

    public void stop()
    {
        container.stop();
    }

    public String getContainerId()
    {
        return container.getContainerId();
    }

    public String executeInContainerFailOnError(String... commandAndArgs)
    {
        Container.ExecResult execResult = executeInContainer(commandAndArgs);
        if (execResult.getExitCode() != 0) {
            String message = format("Command [%s] exited with %s", String.join(" ", commandAndArgs), execResult.getExitCode());
            log.error("%s", message);
            log.error("stderr: %s", execResult.getStderr());
            log.error("stdout: %s", execResult.getStdout());
            throw new RuntimeException(message);
        }
        return execResult.getStdout();
    }

    public Container.ExecResult executeInContainer(String... commandAndArgs)
    {
        try {
            return container.execInContainer(commandAndArgs);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Exception while running command: " + String.join(" ", commandAndArgs), e);
        }
    }

    @Override
    public void close()
    {
        stop();
    }

    @ResourcePresence
    public boolean isPresent()
    {
        return container.isRunning() || container.getContainerId() != null;
    }

    protected abstract static class Builder<SELF extends BaseTestContainer.Builder<SELF, BUILD>, BUILD extends BaseTestContainer>
    {
        protected String image;
        protected String hostName;
        protected Set<Integer> exposePorts = ImmutableSet.of();
        protected Map<String, String> filesToMount = ImmutableMap.of();
        protected Map<String, String> envVars = ImmutableMap.of();
        protected Optional<Network> network = Optional.empty();
        protected int startupRetryLimit = 1;

        protected SELF self;

        @SuppressWarnings("unchecked")
        public Builder()
        {
            this.self = (SELF) this;
        }

        public SELF withImage(String image)
        {
            this.image = image;
            return self;
        }

        public SELF withHostName(String hostName)
        {
            this.hostName = hostName;
            return self;
        }

        public SELF withExposePorts(Set<Integer> exposePorts)
        {
            this.exposePorts = exposePorts;
            return self;
        }

        public SELF withFilesToMount(Map<String, String> filesToMount)
        {
            this.filesToMount = filesToMount;
            return self;
        }

        public SELF withEnvVars(Map<String, String> envVars)
        {
            this.envVars = envVars;
            return self;
        }

        public SELF withNetwork(Network network)
        {
            this.network = Optional.of(network);
            return self;
        }

        public SELF withStartupRetryLimit(int startupRetryLimit)
        {
            this.startupRetryLimit = startupRetryLimit;
            return self;
        }

        public abstract BUILD build();
    }
}
