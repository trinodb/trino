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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HealthCheck;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.tests.product.launcher.env.DelegateContainers.DelegateContainerFactory;
import io.prestosql.tests.product.launcher.testcontainers.ExistingNetwork;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.Timeout;
import net.jodah.failsafe.function.CheckedRunnable;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.containers.traits.LinkableContainer;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.ofBytes;
import static io.prestosql.tests.product.launcher.env.DelegateContainers.fixedHostPort;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.size;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class DockerContainer
        implements Container<DockerContainer>, AutoCloseable, WaitStrategyTarget, Startable
{
    private static final Logger log = Logger.get(DockerContainer.class);
    private static final long NANOSECONDS_PER_SECOND = 1_000 * 1_000 * 1_000L;

    private static final Timeout asyncTimeout = Timeout.of(ofSeconds(30))
            .withCancel(true);

    private static final FailsafeExecutor executor = Failsafe
            .with(asyncTimeout)
            .with(Executors.newCachedThreadPool(daemonThreadsNamed("docker-container-%d")));

    private final FixedPortsAdapter portsAdapter = new FixedPortsAdapter();
    private final String logicalName;
    private final EnvironmentListenerAdapter listenerAdapter;
    private final GenericContainer<? extends GenericContainer> delegate;

    private List<String> logPaths = new ArrayList<>();

    public DockerContainer(String dockerImageName, String logicalName)
    {
        this(logicalName, fixedHostPort(dockerImageName), container -> {});
    }

    public <T extends GenericContainer<T>> DockerContainer(String logicalName, DelegateContainerFactory<T> containerFactory, Consumer<T> containerClosure)
    {
        this.logicalName = requireNonNull(logicalName, "logicalName is null");
        requireNonNull(containerFactory, "containerFactory is null");
        requireNonNull(containerClosure, "containerFactory is null");

        this.listenerAdapter = new EnvironmentListenerAdapter(this);

        T container = containerFactory.create(this.listenerAdapter, this.portsAdapter);
        containerClosure.accept(container);
        this.delegate = container;

        // workaround for https://github.com/testcontainers/testcontainers-java/pull/2861
        delegate.setCopyToFileContainerPathMap(new LinkedHashMap<>());
    }

    public String getLogicalName()
    {
        return logicalName;
    }

    public DockerContainer withEnvironmentListener(Optional<EnvironmentListener> listener)
    {
        requireNonNull(listener, "listener is null")
                .ifPresent(listenerAdapter::set);
        return this;
    }

    @Override
    public void setCommand(String command)
    {
        delegate.setCommand(command);
    }

    @Override
    public void setCommand(String... commands)
    {
        delegate.setCommand(commands);
    }

    @Override
    public void addEnv(String key, String value)
    {
        delegate.addEnv(key, value);
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        delegate.addFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext)
    {
        verifyHostPath(hostPath);
        delegate.addFileSystemBind(hostPath, containerPath, mode, selinuxContext);
    }

    @Override
    public void addLink(LinkableContainer linkableContainer, String alias)
    {
        delegate.addLink(linkableContainer, alias);
    }

    @Override
    public void addExposedPort(Integer port)
    {
        delegate.addExposedPort(port);
    }

    @Override
    public void addExposedPorts(int... ports)
    {
        delegate.addExposedPorts(ports);
    }

    @Override
    public DockerContainer waitingFor(WaitStrategy waitStrategy)
    {
        delegate.waitingFor(waitStrategy);
        return this;
    }

    public DockerContainer withFileSystemBind(String hostPath, String containerPath)
    {
        verifyHostPath(hostPath);
        delegate.withFileSystemBind(hostPath, containerPath);
        return this;
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        delegate.withFileSystemBind(hostPath, containerPath, mode);
        return this;
    }

    @Override
    public DockerContainer withVolumesFrom(Container container, BindMode bindMode)
    {
        delegate.withVolumesFrom(container, bindMode);
        return this;
    }

    @Override
    public DockerContainer withExposedPorts(Integer... ports)
    {
        delegate.withExposedPorts(ports);
        return this;
    }

    @Override
    public DockerContainer withCopyFileToContainer(MountableFile mountableFile, String containerPath)
    {
        delegate.withCopyFileToContainer(mountableFile, containerPath);
        return this;
    }

    @Override
    public DockerContainer withEnv(String key, String value)
    {
        delegate.withEnv(key, value);
        return this;
    }

    @Override
    public DockerContainer withEnv(Map<String, String> env)
    {
        delegate.withEnv(env);
        return this;
    }

    @Override
    public DockerContainer withLabel(String key, String value)
    {
        delegate.withLabel(key, value);
        return this;
    }

    @Override
    public DockerContainer withLabels(Map<String, String> labels)
    {
        delegate.withLabels(labels);
        return this;
    }

    @Override
    public DockerContainer withCommand(String command)
    {
        delegate.withCommand(command);
        return this;
    }

    @Override
    public DockerContainer withCommand(String... commands)
    {
        delegate.withCommand(commands);
        return this;
    }

    @Override
    public DockerContainer withExtraHost(String hostName, String ipAddress)
    {
        delegate.withExtraHost(hostName, ipAddress);
        return this;
    }

    @Override
    public DockerContainer withNetworkMode(String networMode)
    {
        delegate.withNetworkMode(networMode);
        return this;
    }

    @Override
    public DockerContainer withNetwork(Network network)
    {
        delegate.withNetwork(network);
        return this;
    }

    @Override
    public DockerContainer withNetworkAliases(String... aliases)
    {
        delegate.withNetworkAliases(aliases);
        return this;
    }

    @Override
    public DockerContainer withImagePullPolicy(ImagePullPolicy imagePullPolicy)
    {
        delegate.withImagePullPolicy(imagePullPolicy);
        return this;
    }

    @Override
    public DockerContainer withClasspathResourceMapping(String resourcePath, String containerPath, BindMode bindMode, SelinuxContext selinuxContext)
    {
        delegate.withClasspathResourceMapping(resourcePath, containerPath, bindMode, selinuxContext);
        return this;
    }

    @Override
    public DockerContainer withStartupTimeout(java.time.Duration duration)
    {
        delegate.withStartupTimeout(duration);
        return this;
    }

    @Override
    public DockerContainer withPrivilegedMode(boolean mode)
    {
        delegate.withPrivilegedMode(mode);
        return this;
    }

    @Override
    public DockerContainer withMinimumRunningDuration(java.time.Duration duration)
    {
        delegate.withMinimumRunningDuration(duration);
        return this;
    }

    @Override
    public DockerContainer withStartupCheckStrategy(StartupCheckStrategy startupCheckStrategy)
    {
        delegate.withStartupCheckStrategy(startupCheckStrategy);
        return this;
    }

    @Override
    public DockerContainer withWorkingDirectory(String workingDir)
    {
        delegate.withWorkingDirectory(workingDir);
        return this;
    }

    @Override
    public void setDockerImageName(String dockerImageName)
    {
        delegate.setDockerImageName(dockerImageName);
    }

    @Override
    public String getDockerImageName()
    {
        return delegate.getDockerImageName();
    }

    @Override
    public String getTestHostIpAddress()
    {
        return delegate.getTestHostIpAddress();
    }

    @Override
    public DockerContainer withLogConsumer(Consumer<OutputFrame> consumer)
    {
        delegate.withLogConsumer(consumer);
        return this;
    }

    @Override
    public List<String> getPortBindings()
    {
        return delegate.getPortBindings();
    }

    @Override
    public InspectContainerResponse getContainerInfo()
    {
        return delegate.getContainerInfo();
    }

    @Override
    public List<String> getExtraHosts()
    {
        return delegate.getExtraHosts();
    }

    @Override
    public Future<String> getImage()
    {
        return delegate.getImage();
    }

    @Override
    public List<String> getEnv()
    {
        return delegate.getEnv();
    }

    @Override
    public Map<String, String> getEnvMap()
    {
        return delegate.getEnvMap();
    }

    @Override
    public String[] getCommandParts()
    {
        return delegate.getCommandParts();
    }

    @Override
    public List<Bind> getBinds()
    {
        return delegate.getBinds();
    }

    @Override
    public Map<String, LinkableContainer> getLinkedContainers()
    {
        return delegate.getLinkedContainers();
    }

    @Override
    public DockerClient getDockerClient()
    {
        return delegate.getDockerClient();
    }

    @Override
    public void setExposedPorts(List<Integer> ports)
    {
        delegate.setExposedPorts(ports);
    }

    @Override
    public void setPortBindings(List<String> bindings)
    {
        delegate.setPortBindings(bindings);
    }

    @Override
    public void setExtraHosts(List<String> hosts)
    {
        delegate.setExtraHosts(hosts);
    }

    @Override
    public void setImage(Future<String> image)
    {
        delegate.setImage(image);
    }

    @Override
    public void setEnv(List<String> env)
    {
        delegate.setEnv(env);
    }

    @Override
    public void setCommandParts(String[] commandParts)
    {
        delegate.setCommandParts(commandParts);
    }

    @Override
    public void setBinds(List<Bind> binds)
    {
        delegate.setBinds(binds);
    }

    @Override
    public void setLinkedContainers(Map<String, LinkableContainer> linkedContainers)
    {
        delegate.setLinkedContainers(linkedContainers);
    }

    @Override
    public void setWaitStrategy(WaitStrategy waitStrategy)
    {
        delegate.setWaitStrategy(waitStrategy);
    }

    @Override
    public void copyFileToContainer(Transferable transferable, String containerPath)
    {
        copyFileToContainer(containerPath, () -> delegate.copyFileToContainer(transferable, containerPath));
    }

    public DockerContainer withFixedExposedPort(int hostPort, int containerPort)
    {
        portsAdapter.addFixedExposedPort(hostPort, containerPort);
        return this;
    }

    public DockerContainer withExposedLogPaths(String... logPaths)
    {
        requireNonNull(this.logPaths, "log paths are already exposed");
        this.logPaths.addAll(Arrays.asList(logPaths));
        return this;
    }

    public DockerContainer withHealthCheck(Path healthCheckScript)
    {
        HealthCheck healthCheck = new HealthCheck()
                .withTest(ImmutableList.of("CMD", "health.sh"))
                .withInterval(NANOSECONDS_PER_SECOND * 15)
                .withStartPeriod(NANOSECONDS_PER_SECOND * 5 * 60)
                .withRetries(3); // try health checking 3 times before marking container as unhealthy

        delegate.withCopyFileToContainer(forHostPath(healthCheckScript), "/usr/local/bin/health.sh")
                .withCreateContainerCmdModifier(command -> command.withHealthcheck(healthCheck));

        return this;
    }

    public Duration getStartupTime()
    {
        return Duration.succinctNanos(listenerAdapter.getStartupTime().elapsed(NANOSECONDS))
                .convertToMostSuccinctTimeUnit();
    }

    private void copyFileToContainer(String containerPath, CheckedRunnable copy)
    {
        final Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            ((CompletableFuture<?>) executor.runAsync(copy)).whenComplete((Object ignore, Throwable throwable) -> {
                if (throwable == null) {
                    log.info("Copied files into %s %s in %.1f s", this, containerPath, stopwatch.elapsed(MILLISECONDS) / 1000.);
                }
                else {
                    log.warn(throwable, "Could not copy files into %s %s", this, containerPath);
                }
            }).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public String execCommand(String... command)
    {
        String fullCommand = Joiner.on(" ").join(command);
        if (!isRunning()) {
            throw new RuntimeException(format("Could not execute command '%s' in stopped container %s", fullCommand, logicalName));
        }

        log.info("Executing command '%s' in container %s", fullCommand, logicalName);

        try {
            ExecResult result = (ExecResult) executor.getAsync(() -> execInContainer(command)).get();
            if (result.getExitCode() == 0) {
                return result.getStdout();
            }

            throw new RuntimeException(format("Could not execute command '%s' in container %s: %s", fullCommand, logicalName, result.getStderr()));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void copyLogsToHostPath(Path hostPath)
    {
        if (!isRunning()) {
            log.warn("Could not copy files from stopped container %s", logicalName);
            return;
        }

        log.info("Copying container %s logs to '%s'", logicalName, hostPath);

        Path hostLogPath = Paths.get(hostPath.toString(), logicalName);
        ensurePathExists(hostLogPath);

        ImmutableList.Builder<String> files = ImmutableList.builder();

        for (String containerLogPath : logPaths) {
            try {
                files.addAll(listFilesInContainer(containerLogPath));
            }
            catch (RuntimeException e) {
                log.warn("Could not list files in container %s path %s", logicalName, containerLogPath);
            }
        }

        ImmutableList<String> filesToCopy = files.build();
        if (filesToCopy.isEmpty()) {
            log.warn("There are no log files to copy from container %s", logicalName);
            return;
        }

        try {
            String filesList = Joiner.on("\n")
                    .skipNulls()
                    .join(filesToCopy);

            String containerLogsListingFile = format("/tmp/%s-logs-list.txt", UUID.randomUUID());
            String containerLogsArchive = format("/tmp/logs-%s-%s.tar.gz", logicalName, UUID.randomUUID());

            log.info("Creating logs archive %s from file list %s (%d files)", containerLogsArchive, containerLogsListingFile, filesToCopy.size());
            executor.runAsync(() -> copyFileToContainer(Transferable.of(filesList.getBytes(UTF_8)), containerLogsListingFile)).get();

            execCommand("tar", "-cf", containerLogsArchive, "-T", containerLogsListingFile);
            copyFileFromContainer(containerLogsArchive, hostPath.resolve(format("%s/logs.tar.gz", logicalName)));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | RuntimeException e) {
            log.warn(e, "Could not copy logs archive from %s", logicalName);
        }
    }

    public DockerContainer waitingForAll(WaitStrategy... strategies)
    {
        WaitAllStrategy waitAllStrategy = new WaitAllStrategy();
        for (WaitStrategy strategy : strategies) {
            waitAllStrategy.withStrategy(strategy);
        }
        waitingFor(waitAllStrategy);
        return this;
    }

    private void copyFileFromContainer(String filename, Path targetPath)
    {
        ensurePathExists(targetPath.getParent());

        try {
            executor.runAsync(() -> {
                log.info("Copying file %s to %s", filename, targetPath);
                copyFileFromContainer(filename, targetPath.toString());
                log.info("Copied file %s to %s (size: %s bytes)", filename, targetPath, ofBytes(size(targetPath)).succinct());
            }).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | RuntimeException e) {
            log.warn(e, "Could not copy file from %s to %s", filename, targetPath);
        }
    }

    private List<String> listFilesInContainer(String path)
    {
        try {
            return Splitter.on("\n")
                    .omitEmptyStrings()
                    .splitToList(execCommand("/usr/bin/find", path, "-type", "f", "-print"));
        }
        catch (RuntimeException e) {
            log.warn(e, "Could not list files in container '%s' path %s", logicalName, path);
        }

        return ImmutableList.of();
    }

    @Override
    public boolean isHealthy()
    {
        try {
            return delegate.isHealthy();
        }
        catch (RuntimeException ignored) {
            // Container without health checks will throw
            return true;
        }
    }

    @Override
    public List<Integer> getExposedPorts()
    {
        return delegate.getExposedPorts();
    }

    public void reset()
    {
        // When retrying environment startup we need to stop created containers to reset it's containerId
        stop();
    }

    @Override
    public String toString()
    {
        return logicalName;
    }

    // Mounting a non-existing file results in docker creating a directory. This is often not the desired effect. Fail fast instead.
    private static void verifyHostPath(String hostPath)
    {
        if (!Files.exists(Paths.get(hostPath))) {
            throw new IllegalArgumentException("Host path does not exist: " + hostPath);
        }
    }

    public static void cleanOrCreateHostPath(Path path)
    {
        try {
            if (Files.exists(path)) {
                deleteRecursively(path, RecursiveDeleteOption.ALLOW_INSECURE);
                log.info("Removed host directory: '%s'", path);
            }

            ensurePathExists(path);
            log.info("Created host directory: '%s'", path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void ensurePathExists(Path path)
    {
        try {
            Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void tryStop()
    {
        if (!isRunning()) {
            log.warn("Could not stop already stopped container: %s", logicalName);
            return;
        }

        try {
            executor.runAsync(this::stop).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException | RuntimeException e) {
            log.warn(e, "Could not stop container correctly");
        }

        checkState(!isRunning(), "Container %s is still running", logicalName);
    }

    @Override
    public String getContainerName()
    {
        return delegate.getContainerName();
    }

    @Override
    public void start()
    {
        delegate.start();
    }

    @Override
    public void stop()
    {
        delegate.stop();
    }

    public DockerContainer dependsOn(Startable... dockerContainers)
    {
        delegate.dependsOn(dockerContainers);
        return this;
    }

    public DockerContainer dependsOn(List<? extends Startable> dockerContainers)
    {
        delegate.dependsOn(dockerContainers);
        return this;
    }

    public DockerContainer dependsOn(Iterable<? extends Startable> dockerContainers)
    {
        delegate.dependsOn(dockerContainers);
        return this;
    }

    public DockerContainer withCreateContainerCmdModifier(Consumer<CreateContainerCmd> modifier)
    {
        delegate.withCreateContainerCmdModifier(modifier);
        return this;
    }

    public DockerContainer withTmpFs(ImmutableMap<String, String> mapping)
    {
        delegate.withTmpFs(mapping);
        return this;
    }

    public void setNetwork(ExistingNetwork existingNetwork)
    {
        delegate.setNetwork(existingNetwork);
    }

    public enum OutputMode
    {
        PRINT,
        DISCARD,
        WRITE,
        PRINT_WRITE,
        /**/;
    }
}
