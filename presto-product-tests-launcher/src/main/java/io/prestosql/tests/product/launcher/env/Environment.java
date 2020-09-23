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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ulimit;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.testcontainers.PrintingLogConsumer;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.Timeout;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.lifecycle.Startables;

import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tests.product.launcher.env.DockerContainer.ensurePathExists;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.prestosql.tests.product.launcher.env.Environments.pruneEnvironment;
import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;

public final class Environment
        implements AutoCloseable
{
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME = Environment.class.getName() + ".ptl-started";
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE = "true";
    public static final String PRODUCT_TEST_LAUNCHER_NETWORK = "ptl-network";
    public static final String PRODUCT_TEST_LAUNCHER_ENVIRONMENT_LABEL_NAME = "ptl-environment-name";

    public static final Integer ENVIRONMENT_FAILED_EXIT_CODE = 99;

    private static final Logger log = Logger.get(Environment.class);

    private final String name;
    private final int startupRetries;
    private final Map<String, DockerContainer> containers;
    private final Optional<EnvironmentListener> listener;

    private Environment(String name, int startupRetries, Map<String, DockerContainer> containers, Optional<EnvironmentListener> listener)
    {
        this.name = requireNonNull(name, "name is null");
        this.startupRetries = startupRetries;
        this.containers = requireNonNull(containers, "containers is null");
        this.listener = requireNonNull(listener, "listener is null");
    }

    public Environment start()
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

    public List<String> getContainerNames()
    {
        return containers.values().stream()
                .map(DockerContainer::getLogicalName)
                .collect(toImmutableList());
    }

    private Environment tryStart()
    {
        pruneEnvironment();

        // Create new network when environment tries to start
        try (Network network = createNetwork(name)) {
            List<DockerContainer> containers = ImmutableList.copyOf(this.containers.values());
            attachNetwork(containers, network);

            String containerNames = Joiner.on(", ").join(getContainerNames());

            log.info("Starting containers %s for environment %s", containerNames, name);
            Startables.deepStart(containers).get();
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

        // Allow containers to take up to 5 minutes to stop
        Timeout<Object> timeout = Timeout.of(ofMinutes(5))
                .withCancel(true);

        RetryPolicy retry = new RetryPolicy()
                .withMaxAttempts(3);

        FailsafeExecutor<Object> executor = Failsafe.with(timeout, retry);

        ImmutableList.copyOf(containers.values())
                .stream()
                .filter(DockerContainer::isRunning)
                .forEach(container -> executor.run(container::tryStop));

        this.listener.ifPresent(listener -> listener.environmentStopped(this));
        pruneEnvironment();
    }

    public void awaitContainersStopped()
    {
        try {
            // Before checking for health check state, let container become healthy first
            Thread.sleep(15_000);

            log.info("Started monitoring containers for health");

            while (allContainersHealthy(getContainers())) {
                Thread.sleep(10_000);
            }

            log.warn("Some of the containers are stopped or unhealthy");

            return;
        }
        catch (InterruptedException e) {
            log.info("Interrupted");
            // It's OK not to restore interrupt flag here. When we return we're exiting the process.
        }
        catch (Exception e) {
            log.warn("Could not query for containers state: %s", getStackTraceAsString(e));
        }
    }

    public long awaitTestsCompletion()
    {
        DockerContainer testContainer = getContainer(TESTS);
        Collection<DockerContainer> containers = getContainers()
                .stream()
                .filter(container -> !container.equals(testContainer))
                .collect(toImmutableList());

        log.info("Waiting for test completion on container %s...", testContainer.getContainerId());

        try {
            while (testContainer.isRunning()) {
                Thread.sleep(10000); // check every 10 seconds

                if (!allContainersHealthy(containers)) {
                    log.warn("Environment %s is not healthy, interrupting tests", name);
                    return ENVIRONMENT_FAILED_EXIT_CODE;
                }
            }

            InspectContainerResponse containerInfo = testContainer.getCurrentContainerInfo();
            InspectContainerResponse.ContainerState containerState = containerInfo.getState();
            Long exitCode = containerState.getExitCodeLong();
            log.info("Test container %s is %s, with exitCode %s", containerInfo.getId(), containerState.getStatus(), exitCode);
            checkState(exitCode != null, "No exitCode for tests container %s in state %s", testContainer, containerState);

            return exitCode;
        }
        catch (InterruptedException e) {
            // Gracefully stop environment and trigger listeners
            Thread.currentThread().interrupt();
            stop();
            throw new RuntimeException("Interrupted", e);
        }
    }

    public DockerContainer getContainer(String name)
    {
        return Optional.ofNullable(containers.get(requireNonNull(name, "name is null")))
                .orElseThrow(() -> new IllegalArgumentException("No container with name " + name));
    }

    public Collection<DockerContainer> getContainers()
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
        try {
            stop();
        }
        catch (Exception e) {
            log.warn("Exception occured while closing environment: %s", getStackTraceAsString(e));
        }
    }

    private static boolean allContainersHealthy(Iterable<DockerContainer> containers)
    {
        return Streams.stream(containers)
                .allMatch(Environment::containerIsHealthy);
    }

    private static boolean containerIsHealthy(DockerContainer container)
    {
        if (!container.isRunning()) {
            log.warn("Container %s is not running", container.getLogicalName());
            return false;
        }

        if (!container.isHealthy()) {
            log.warn("Container %s is not healthy", container.getLogicalName());
            return false;
        }

        return true;
    }

    private static void attachNetwork(Collection<DockerContainer> values, Network network)
    {
        values.forEach(container -> container.withNetwork(network));
    }

    private static Network createNetwork(String environmentName)
    {
        Network network = Network.builder()
                .createNetworkCmdModifier(createNetworkCmd ->
                        createNetworkCmd
                                .withName(PRODUCT_TEST_LAUNCHER_NETWORK)
                                .withLabels(ImmutableMap.of(
                                        PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE,
                                        PRODUCT_TEST_LAUNCHER_ENVIRONMENT_LABEL_NAME, environmentName)))
                .build();
        log.info("Created new network %s for environment %s", network.getId(), environmentName);
        return network;
    }

    public static class Builder
    {
        private final String name;
        private DockerContainer.OutputMode outputMode;
        private int startupRetries = 1;
        private Map<String, DockerContainer> containers = new HashMap<>();
        private Optional<Path> logsBaseDir = Optional.empty();

        public Builder(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public String getEnvironmentName()
        {
            return name;
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
                    .withNetworkAliases(containerName)
                    .withLabel(PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME, PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE)
                    .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                            .withName("ptl-" + containerName)
                            .withHostName(containerName));

            container.withCreateContainerCmdModifier(Builder::updateContainerHostConfig);

            if (!container.getLogicalName().equals(TESTS)) {
                // Tests container cannot be auto removed as we need to inspect it's exit code
                container.withCreateContainerCmdModifier(Builder::setContainerAutoRemove);
            }

            return this;
        }

        private static void updateContainerHostConfig(CreateContainerCmd createContainerCmd)
        {
            HostConfig hostConfig = requireNonNull(createContainerCmd.getHostConfig(), "hostConfig is null");

            // Disable OOM killer
            hostConfig.withOomKillDisable(true);
            // Apply ulimits
            hostConfig.withUlimits(standardUlimits());
        }

        private static void setContainerAutoRemove(CreateContainerCmd createContainerCmd)
        {
            HostConfig hostConfig = requireNonNull(createContainerCmd.getHostConfig(), "hostConfig is null");
            // Automatically remove container on exit
            hostConfig.withAutoRemove(true);
        }

        private static List<Ulimit> standardUlimits()
        {
            return ImmutableList.of(
                // Number of open file descriptors
                new Ulimit("nofile", 65535L, 65535L),
                // Number of processes
                new Ulimit("nproc", 8096L, 8096L));
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
            log.info("Removing container %s", logicalName);

            requireNonNull(logicalName, "logicalName is null");
            DockerContainer container = containers.remove(logicalName);
            if (container != null) {
                container.close();
            }
            return this;
        }

        public Builder removeContainers(Predicate<DockerContainer> predicate)
        {
            requireNonNull(predicate, "predicate is null");

            List<String> containersNames = containers.values().stream()
                    .filter(predicate)
                    .map(DockerContainer::getLogicalName)
                    .collect(toImmutableList());

            for (String containerName : containersNames) {
                removeContainer(containerName);
            }
            return this;
        }

        public Builder setContainerOutputMode(DockerContainer.OutputMode outputMode)
        {
            this.outputMode = outputMode;
            return this;
        }

        public Builder setStartupRetries(int retries)
        {
            this.startupRetries = retries;
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
            switch (outputMode) {
                case DISCARD:
                    log.warn("Containers logs are not printed to stdout");
                    setContainerOutputConsumer(this::discardContainerLogs);
                    break;

                case PRINT:
                    setContainerOutputConsumer(this::printContainerLogs);
                    break;

                case PRINT_WRITE:
                    verify(logsBaseDir.isPresent(), "--logs-dir must be set with --output WRITE");

                    setContainerOutputConsumer(container -> combineConsumers(
                            writeContainerLogs(container, logsBaseDir.get()),
                            printContainerLogs(container)));
                    break;

                case WRITE:
                    verify(logsBaseDir.isPresent(), "--logs-dir must be set with --output WRITE");
                    setContainerOutputConsumer(container -> writeContainerLogs(container, logsBaseDir.get()));
            }

            containers.forEach((name, container) -> {
                container
                        .withEnvironmentListener(listener)
                        .withCreateContainerCmdModifier(createContainerCmd -> {
                            Map<String, Bind> binds = new HashMap<>();
                            HostConfig hostConfig = createContainerCmd.getHostConfig();
                            for (Bind bind : firstNonNull(hostConfig.getBinds(), new Bind[0])) {
                                binds.put(bind.getVolume().getPath(), bind); // last bind wins
                            }
                            hostConfig.setBinds(binds.values().toArray(new Bind[0]));
                        });
            });

            return new Environment(name, startupRetries, containers, listener);
        }

        private Consumer<OutputFrame> writeContainerLogs(DockerContainer container, Path path)
        {
            Path containerLogFile = path.resolve(container.getLogicalName() + "/container.log");
            log.info("Writing container %s logs to %s", container, containerLogFile);

            try {
                ensurePathExists(containerLogFile.getParent());
                return new PrintingLogConsumer(new PrintStream(containerLogFile.toFile()), "");
            }
            catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Consumer<OutputFrame> printContainerLogs(DockerContainer container)
        {
            try {
                // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
                //noinspection resource
                PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
                return new PrintingLogConsumer(out, format("%-20s| ", container.getLogicalName()));
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        private Consumer<OutputFrame> discardContainerLogs(DockerContainer container)
        {
            // Discard log frames
            return outputFrame -> {};
        }

        private Consumer<OutputFrame> combineConsumers(Consumer<OutputFrame>... consumers)
        {
            return outputFrame -> Arrays.stream(consumers).forEach(consumer -> consumer.accept(outputFrame));
        }

        private void setContainerOutputConsumer(Function<DockerContainer, Consumer<OutputFrame>> consumer)
        {
            configureContainers(container -> container.withLogConsumer(consumer.apply(container)));
        }

        public Builder setLogsBaseDir(Optional<Path> baseDir)
        {
            this.logsBaseDir = baseDir;
            return this;
        }
    }
}
