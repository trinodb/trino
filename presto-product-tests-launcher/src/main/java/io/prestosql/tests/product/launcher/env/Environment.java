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
package io.prestosql.tests.product.launcher.env;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.testcontainers.PrintingLogConsumer;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.prestosql.tests.product.launcher.env.Environments.pruneEnvironment;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Environment
        implements AutoCloseable
{
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME = Environment.class.getName() + ".ptl-started";
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE = "true";
    public static final String PRODUCT_TEST_LAUNCHER_NETWORK = "ptl-network";

    private static final Logger log = Logger.get(Environment.class);

    private final String name;
    private final Map<String, DockerContainer> containers;
    private final Optional<EnvironmentListener> listener;

    public Environment(String name, Map<String, DockerContainer> containers, Optional<EnvironmentListener> listener)
    {
        this.name = requireNonNull(name, "name is null");
        this.containers = requireNonNull(containers, "containers is null");
        this.listener = requireNonNull(listener, "listener is null");
    }

    public void start()
    {
        start(1);
    }

    public Environment start(int startupRetries)
    {
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxRetries(startupRetries)
                .onFailedAttempt(event -> log.warn("Could not start environment '%s': %s", this, getStackTraceAsString(event.getLastFailure())))
                .onRetry(event -> log.info("Trying to start environment '%s', %d failed attempt(s)", this, event.getAttemptCount() + 1))
                .onSuccess(event -> log.info("Environment '%s' started in %s, %d attempt(s)", this, event.getElapsedTime(), event.getAttemptCount()))
                .onFailure(event -> log.info("Environment '%s' failed to start in attempt(s): %d: %s", this, event.getAttemptCount(), event.getFailure()));

        return Failsafe
                .with(retryPolicy)
                .get(() -> tryStart());
    }

    private Environment tryStart()
    {
        try {
            pruneEnvironment();
            Startables.deepStart(ImmutableList.copyOf(containers.values())).get();
            this.listener.ifPresent(listener -> listener.environmentStarted(this));
            return this;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {
        this.listener.ifPresent(listener -> listener.environmentStopping(this));

        ImmutableList.copyOf(containers.values())
                .forEach(DockerContainer::stop);

        awaitContainersStopped();
        pruneEnvironment();
    }

    public void awaitContainersStopped()
    {
        try {
            while (ImmutableList.copyOf(containers.values()).stream().anyMatch(ContainerState::isRunning)) {
                Thread.sleep(1_000);
            }

            this.listener.ifPresent(listener -> listener.environmentStopped(this));
            return;
        }
        catch (InterruptedException e) {
            log.info("Interrupted");
            // It's OK not to restore interrupt flag here. When we return we're exiting the process.
        }
    }

    public long awaitTestsCompletion()
    {
        Container<?> container = getContainer(TESTS);
        log.info("Waiting for test completion");

        try {
            while (container.isRunning()) {
                Thread.sleep(1000);
            }

            InspectContainerResponse containerInfo = container.getCurrentContainerInfo();
            InspectContainerResponse.ContainerState containerState = containerInfo.getState();
            Long exitCode = containerState.getExitCodeLong();
            log.info("Test container %s is %s, with exitCode %s", containerInfo.getId(), containerState.getStatus(), exitCode);
            checkState(exitCode != null, "No exitCode for tests container %s in state %s", container, containerState);

            return exitCode;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }

    public DockerContainer getContainer(String name)
    {
        return Optional.ofNullable(containers.get(requireNonNull(name, "name is null")))
                .orElseThrow(() -> new IllegalArgumentException("No container with name " + name));
    }

    public Collection<Container<?>> getContainers()
    {
        return ImmutableList.copyOf(containers.values());
    }

    @Override
    public String toString()
    {
        return name;
    }

    public static Builder builder(String name)
    {
        return new Builder(name);
    }

    @Override
    public void close()
    {
        stop();
    }

    public static class Builder
    {
        private final String name;

        @SuppressWarnings("resource")
        private Network network = Network.builder()
                .createNetworkCmdModifier(createNetworkCmd ->
                        createNetworkCmd
                                .withName(PRODUCT_TEST_LAUNCHER_NETWORK)
                                .withLabels(ImmutableMap.of(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)))
                .build();

        private Map<String, DockerContainer> containers = new HashMap<>();

        public Builder(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public Builder addContainers(DockerContainer... containers)
        {
            Arrays.stream(containers)
                    .forEach(this::addContainer);

            return this;
        }

        public Builder addContainer(DockerContainer container)
        {
            String containerName = container.getLogicalName();

            checkState(!containers.containsKey(containerName), "Container with name %s is already registered", containerName);
            containers.put(containerName, requireNonNull(container, "container is null"));

            container
                    .withNetwork(network)
                    .withNetworkAliases(containerName)
                    .withLabel(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                            .withName("ptl-" + containerName)
                            .withHostName(containerName));

            return this;
        }

        public Builder containerDependsOnRest(String logicalName)
        {
            checkState(containers.containsKey(logicalName), "Container with name %s does not exist", logicalName);
            DockerContainer container = containers.get(logicalName);

            containers.entrySet()
                    .stream()
                    .filter(entry -> !entry.getKey().equals(logicalName))
                    .map(entry -> entry.getValue())
                    .forEach(dependant -> container.dependsOn(dependant));

            return this;
        }

        public Builder configureContainer(String logicalName, Consumer<DockerContainer> configurer)
        {
            requireNonNull(logicalName, "logicalName is null");
            checkState(containers.containsKey(logicalName), "Container with name %s is not registered", logicalName);
            requireNonNull(configurer, "configurer is null").accept(containers.get(logicalName));
            return this;
        }

        public Builder configureContainers(Consumer<DockerContainer> configurer)
        {
            requireNonNull(configurer, "configurer is null");
            containers.values().forEach(configurer::accept);
            return this;
        }

        public Builder removeContainer(String logicalName)
        {
            requireNonNull(logicalName, "logicalName is null");
            GenericContainer<?> container = containers.remove(logicalName);
            if (container != null) {
                container.close();
            }
            return this;
        }

        public Environment build()
        {
            return build(Optional.empty());
        }

        public Environment build(EnvironmentListener listener)
        {
            return build(Optional.of(listener));
        }

        private Environment build(Optional<EnvironmentListener> listener)
        {
            // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
            PrintStream out;
            try {
                //noinspection resource
                out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            containers.forEach((name, container) -> {
                container
                        .withEnvironmentListener(listener)
                        .withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", name)))
                        .withCreateContainerCmdModifier(createContainerCmd -> {
                            Map<String, Bind> binds = new HashMap<>();
                            HostConfig hostConfig = createContainerCmd.getHostConfig();
                            for (Bind bind : firstNonNull(hostConfig.getBinds(), new Bind[0])) {
                                binds.put(bind.getVolume().getPath(), bind); // last bind wins
                            }
                            hostConfig.setBinds(binds.values().toArray(new Bind[0]));
                        });
            });

            return new Environment(name, containers, listener);
        }

        public Builder exposeLogsInHostPath(Path basePath)
        {
            log.info("Exposing environment '%s' logs in: '%s'", name, basePath);

            return configureContainers(dockerContainer -> {
                Path containerLogPath = basePath.resolve(dockerContainer.getLogicalName());
                log.info("Exposing container '%s' logs in host directory '%s'", dockerContainer.getLogicalName(), containerLogPath);
                dockerContainer.exposeLogsInHostPath(containerLogPath);
            });
        }
    }
}
