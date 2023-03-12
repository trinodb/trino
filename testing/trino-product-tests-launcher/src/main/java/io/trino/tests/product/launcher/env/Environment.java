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
package io.trino.tests.product.launcher.env;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ulimit;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.Timeout;
import io.airlift.log.Logger;
import io.trino.tests.product.launcher.testcontainers.PrintingLogConsumer;
import io.trino.tests.product.launcher.util.ConsoleTable;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.server.PluginLoader.CONNECTOR;
import static io.trino.server.PluginLoader.PASSWORD_AUTHENTICATOR;
import static io.trino.tests.product.launcher.env.DockerContainer.ensurePathExists;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.Environments.pruneEnvironment;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.lang.String.format;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testcontainers.utility.MountableFile.forHostPath;

public final class Environment
        implements AutoCloseable
{
    private static final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed("environment-%d"));
    private static final Logger log = Logger.get(Environment.class);

    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_NAME = Environment.class.getName() + ".ptl-started";
    public static final String PRODUCT_TEST_LAUNCHER_STARTED_LABEL_VALUE = "true";
    public static final String PRODUCT_TEST_LAUNCHER_NETWORK = "ptl-network";
    public static final String PRODUCT_TEST_LAUNCHER_ENVIRONMENT_LABEL_NAME = "ptl-environment-name";

    public static final Integer ENVIRONMENT_FAILED_EXIT_CODE = 99;

    private final String name;
    private final int startupRetries;
    private final Map<String, DockerContainer> containers;
    private final Optional<EnvironmentListener> listener;
    private final boolean attached;
    private final Map<String, List<String>> configuredFeatures;

    private Environment(
            String name,
            int startupRetries,
            Map<String, DockerContainer> containers,
            Optional<EnvironmentListener> listener,
            boolean attached,
            Map<String, List<String>> configuredFeatures)
    {
        this.name = requireNonNull(name, "name is null");
        this.startupRetries = startupRetries;
        this.containers = requireNonNull(containers, "containers is null");
        this.listener = requireNonNull(listener, "listener is null");
        this.attached = attached;
        this.configuredFeatures = requireNonNull(configuredFeatures, "configuredFeatures is null");
    }

    public Environment start()
    {
        RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
                .withMaxRetries(startupRetries)
                .onFailedAttempt(event -> log.warn(event.getLastException(), "Could not start environment '%s'", this))
                .onRetry(event -> log.info("Trying to start environment '%s', %d failed attempt(s)", this, event.getAttemptCount() + 1))
                .onSuccess(event -> log.info("Environment '%s' started in %s, %d attempt(s)", this, event.getElapsedTime(), event.getAttemptCount()))
                .onFailure(event -> log.info("Environment '%s' failed to start in attempt(s): %d: %s", this, event.getAttemptCount(), event.getException()))
                .build();

        return Failsafe
                .with(retryPolicy)
                .with(executorService)
                .get(this::tryStart);
    }

    private Environment tryStart()
    {
        pruneEnvironment();

        // Reset containerId so that containers can start when failed in previous attempt
        List<DockerContainer> containers = ImmutableList.copyOf(this.containers.values());
        for (DockerContainer container : containers) {
            container.reset();
        }

        for (DockerContainer container : containers) {
            log.info("Will start container '%s' from image '%s'", container.getLogicalName(), container.getDockerImageName());
        }

        // Create new network when environment tries to start
        try (Network network = createNetwork(name)) {
            attachNetwork(containers, network);
            Startables.deepStart(containers).get();

            ConsoleTable table = new ConsoleTable();
            table.addHeader("container", "name", "image", "startup", "ports");
            Joiner joiner = Joiner.on(", ");

            containers.forEach(container -> table.addRow(
                    container.getLogicalName(),
                    container.getContainerName().substring(1), // first char is always slash
                    container.getDockerImageName(),
                    container.getStartupTime(),
                    joiner.join(container.getExposedPorts())));
            table.addSeparator();

            log.info("Started environment %s with containers:\n%s", name, table.render());

            // After deepStart all containers should be running and healthy
            checkState(allContainersHealthy(containers), "Not all containers are running or healthy");

            this.listener.ifPresent(listener -> listener.environmentStarted(this));
            return this;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {
        checkState(!attached, "Cannot stop environment that is attached");
        this.listener.ifPresent(listener -> listener.environmentStopping(this));

        // Allow containers to take up to 5 minutes to stop
        Timeout<Object> timeout = Timeout.builder(ofMinutes(5))
                .withInterrupt()
                .build();

        RetryPolicy<Object> retry = RetryPolicy.builder()
                .withMaxAttempts(3)
                .build();

        FailsafeExecutor<Object> executor = Failsafe
                .with(timeout, retry)
                .with(executorService);

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
        }
        catch (InterruptedException e) {
            log.info("Interrupted");
            // It's OK not to restore interrupt flag here. When we return we're exiting the process.
        }
        catch (RuntimeException e) {
            log.warn(e, "Could not query for containers state");
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

                if (!attached && !allContainersHealthy(containers)) {
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

    public Map<String, List<String>> getConfiguredFeatures()
    {
        return configuredFeatures;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<String> getFeatures()
    {
        return configuredFeatures.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(feature -> entry.getKey() + feature))
                .collect(toImmutableList());
    }

    public static Builder builder(String name)
    {
        return new Builder(name);
    }

    @Override
    public void close()
    {
        if (attached) {
            // do not stop attached, externally controlled environment
            return;
        }

        try {
            stop();
        }
        catch (RuntimeException e) {
            log.warn(e, "Exception occurred while closing environment");
        }
    }

    private static boolean allContainersHealthy(Iterable<DockerContainer> containers)
    {
        return Streams.stream(containers)
                .allMatch(Environment::containerIsHealthy);
    }

    private static boolean containerIsHealthy(DockerContainer container)
    {
        if (container.isTemporary()) {
            return true;
        }

        if (!container.isRunning()) {
            log.warn("Container %s is not running", container.getLogicalName());
            return false;
        }

        if (!container.isHealthy()) {
            log.warn("Container %s is not healthy, logs of container healthcheck:\n%s", container.getLogicalName(), container.getCurrentContainerInfo().getState().getHealth().getLog());
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
        private boolean attached;
        private Multimap<String, String> configuredFeatures = HashMultimap.create();

        public Builder(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public String getEnvironmentName()
        {
            return name;
        }

        public Builder containerDependsOn(String container, String dependencyContainer)
        {
            checkState(containers.containsKey(container), "Container with name %s is not registered", container);
            checkState(containers.containsKey(dependencyContainer), "Dependency container with name %s is not registered", dependencyContainer);
            containers.get(container).dependsOn(containers.get(dependencyContainer));

            return this;
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

            if (!container.getLogicalName().equals(TESTS) && !container.isTemporary()) {
                // Tests container cannot be auto removed as we need to inspect it's exit code.
                // Temporal containers might exit earlier than the startup check polling interval.
                container.withCreateContainerCmdModifier(Builder::setContainerAutoRemove);
            }

            return this;
        }

        public Builder addConnector(String connectorName)
        {
            requireNonNull(connectorName, "connectorName is null");
            checkState(connectorName.length() != 0, "Cannot register empty string as a connector in an Environment");
            return addFeature(CONNECTOR, connectorName);
        }

        public Builder addConnector(String connectorName, MountableFile configFile)
        {
            requireNonNull(connectorName, "connectorName is null");
            requireNonNull(configFile, "configFile is null");
            return addConnector(connectorName, configFile, CONTAINER_TRINO_ETC + "/catalog/" + connectorName + ".properties");
        }

        public Builder addConnector(String connectorName, MountableFile configFile, String containerPath)
        {
            requireNonNull(configFile, "configFile is null");
            requireNonNull(containerPath, "containerPath is null");
            configureContainers(container -> {
                if (isTrinoContainer(container.getLogicalName())) {
                    container.withCopyFileToContainer(configFile, containerPath);
                }
            });
            return addConnector(connectorName);
        }

        public Builder addPasswordAuthenticator(String name)
        {
            requireNonNull(name, "name is null");
            checkState(name.length() != 0, "Cannot register empty string as a password authenticator in an Environment");
            return addFeature(PASSWORD_AUTHENTICATOR, name);
        }

        public Builder addPasswordAuthenticator(String name, MountableFile configFile)
        {
            requireNonNull(name, "name is null");
            requireNonNull(configFile, "configFile is null");
            return addPasswordAuthenticator(name, configFile, CONTAINER_TRINO_ETC + "/password-authenticator.properties");
        }

        public Builder addPasswordAuthenticator(String name, MountableFile configFile, String containerPath)
        {
            requireNonNull(configFile, "catalogConfig is null");
            requireNonNull(containerPath, "containerPath is null");
            configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(configFile, containerPath));
            return addPasswordAuthenticator(name);
        }

        public Builder addFeature(String feature, String name)
        {
            configuredFeatures.put(feature, name);
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

        public Builder configureContainer(String logicalName, Consumer<DockerContainer> configurer)
        {
            requireNonNull(logicalName, "logicalName is null");
            checkState(containers.containsKey(logicalName), "Container with name %s is not registered", logicalName);
            configurer.accept(containers.get(logicalName));
            return this;
        }

        public Builder configureContainers(Consumer<DockerContainer> configurer)
        {
            requireNonNull(configurer, "configurer is null");
            containers.values().forEach(configurer);
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
                    setContainerOutputConsumer(Environment.Builder::discardContainerLogs);
                    break;

                case PRINT:
                    setContainerOutputConsumer(Environment.Builder::printContainerLogs);
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

            addConfiguredFeaturesConfig();

            Map<String, List<String>> configuredFeatures = this.configuredFeatures.asMap().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
            return new Environment(
                    name,
                    startupRetries,
                    containers,
                    listener,
                    attached,
                    configuredFeatures);
        }

        private static Consumer<OutputFrame> writeContainerLogs(DockerContainer container, Path path)
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

        private static Consumer<OutputFrame> printContainerLogs(DockerContainer container)
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

        private static Consumer<OutputFrame> discardContainerLogs(DockerContainer container)
        {
            // Discard log frames
            return outputFrame -> {};
        }

        private void addConfiguredFeaturesConfig()
        {
            if (!containers.containsKey(TESTS)) {
                return;
            }
            DockerContainer testContainer = containers.get(TESTS);
            // write a custom tempto config with list of connectors the environment declares to have configured
            // since it's needed in TestConfiguredFeatures
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            File tempFile;
            try {
                tempFile = File.createTempFile("tempto-configured-features-", ".yaml");
                objectMapper.writeValue(tempFile,
                        Map.of("databases",
                                Map.of("presto",
                                        Map.of(
                                                "configured_connectors",
                                                configuredFeatures.asMap().getOrDefault(CONNECTOR, ImmutableList.of()),
                                                "configured_password_authenticators",
                                                configuredFeatures.asMap().getOrDefault(PASSWORD_AUTHENTICATOR, ImmutableList.of())))));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            String temptoConfig = "/docker/presto-product-tests/conf/tempto/tempto-configured-features.yaml";
            testContainer.withCopyFileToContainer(forHostPath(tempFile.getPath()), temptoConfig);
            // add custom tempto config and configured_features to arguments if there are any other groups enabled
            String[] commandParts = testContainer.getCommandParts();
            for (int i = 0; i < commandParts.length; i++) {
                if (commandParts[i].equals("--config")) {
                    commandParts[i + 1] += (commandParts[i + 1].length() == 0 ? "" : ",") + temptoConfig;
                }
            }
            testContainer.setCommandParts(commandParts.clone());
        }

        private static Consumer<OutputFrame> combineConsumers(Consumer<OutputFrame>... consumers)
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

        public Builder setAttached(boolean attached)
        {
            this.attached = attached;
            return this;
        }
    }
}
