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

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.HealthCheck;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.RecursiveDeleteOption;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Timeout;
import dev.failsafe.function.CheckedRunnable;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.testing.containers.ConditionalPullPolicy;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.ofBytes;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.size;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class DockerContainer
        extends FixedHostPortGenericContainer<DockerContainer>
{
    private static final Logger log = Logger.get(DockerContainer.class);
    private static final long NANOSECONDS_PER_SECOND = 1_000 * 1_000 * 1_000L;

    private static final Timeout<ExecResult> asyncTimeout = Timeout.<ExecResult>builder(ofSeconds(30))
            .withInterrupt()
            .build();

    private static final FailsafeExecutor<ExecResult> executor = Failsafe
            .with(asyncTimeout)
            .with(Executors.newCachedThreadPool(daemonThreadsNamed("docker-container-%d")));

    private final String logicalName;

    // start is retried, we are recording the last attempt only
    @GuardedBy("this")
    private OptionalLong lastStartUpCommenceTimeNanos = OptionalLong.empty();
    @GuardedBy("this")
    private OptionalLong lastStartFinishTimeNanos = OptionalLong.empty();

    private List<String> logPaths = new ArrayList<>();
    private Optional<EnvironmentListener> listener = Optional.empty();
    private boolean temporary;
    private static final ImagePullPolicy pullPolicy = new ConditionalPullPolicy();

    public DockerContainer(String dockerImageName, String logicalName)
    {
        super(dockerImageName);
        this.logicalName = requireNonNull(logicalName, "logicalName is null");

        // workaround for https://github.com/testcontainers/testcontainers-java/pull/2861
        setCopyToFileContainerPathMap(new LinkedHashMap<>());

        this.withImagePullPolicy(pullPolicy);
    }

    @Override
    public void setDockerImageName(String dockerImageName)
    {
        DockerImageName canonicalName = DockerImageName.parse(requireNonNull(dockerImageName, "dockerImageName is null"));
        setImage(CompletableFuture.completedFuture(canonicalName.toString()));
        withImagePullPolicy(pullPolicy);
    }

    public String getLogicalName()
    {
        return logicalName;
    }

    public DockerContainer withEnvironmentListener(Optional<EnvironmentListener> listener)
    {
        this.listener = requireNonNull(listener, "listener is null");
        return this;
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void addFileSystemBind(String hostPath, String containerPath, BindMode mode, SelinuxContext selinuxContext)
    {
        verifyHostPath(hostPath);
        super.addFileSystemBind(hostPath, containerPath, mode, selinuxContext);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath);
    }

    @Override
    public DockerContainer withFileSystemBind(String hostPath, String containerPath, BindMode mode)
    {
        verifyHostPath(hostPath);
        return super.withFileSystemBind(hostPath, containerPath, mode);
    }

    @Override
    public void copyFileToContainer(Transferable transferable, String containerPath)
    {
        copyFileToContainer(containerPath, () -> super.copyFileToContainer(transferable, containerPath));
    }

    public DockerContainer withExposedLogPaths(String... logPaths)
    {
        requireNonNull(this.logPaths, "log paths are already exposed");
        this.logPaths.addAll(Arrays.asList(logPaths));
        return this;
    }

    public DockerContainer withHealthCheck(Path healthCheckScript)
    {
        HealthCheck cmd = new HealthCheck()
                .withTest(ImmutableList.of("CMD", "health.sh"))
                .withInterval(NANOSECONDS_PER_SECOND * 15)
                .withStartPeriod(NANOSECONDS_PER_SECOND * 5 * 60)
                .withRetries(3); // try health checking 3 times before marking container as unhealthy

        return withCopyFileToContainer(forHostPath(healthCheckScript), "/usr/local/bin/health.sh")
                .withCreateContainerCmdModifier(command -> command.withHealthcheck(cmd));
    }

    /**
     * Marks this container as temporary, which means that it's not expected to be working at the end of environment creation.
     * Mostly used to execute short configuration scripts shipped as a part of docker images.
     */
    public DockerContainer setTemporary(boolean temporary)
    {
        this.temporary = temporary;
        return this;
    }

    public synchronized Duration getStartupTime()
    {
        checkState(lastStartUpCommenceTimeNanos.isPresent(), "Container did not commence starting");
        checkState(lastStartFinishTimeNanos.isPresent(), "Container not started");
        return Duration.succinctNanos(lastStartFinishTimeNanos.getAsLong() - lastStartUpCommenceTimeNanos.getAsLong()).convertToMostSuccinctTimeUnit();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo)
    {
        synchronized (this) {
            lastStartUpCommenceTimeNanos = OptionalLong.of(System.nanoTime());
            lastStartFinishTimeNanos = OptionalLong.empty();
        }
        super.containerIsStarting(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStarting(this, containerInfo));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo)
    {
        synchronized (this) {
            checkState(lastStartUpCommenceTimeNanos.isPresent(), "containerIsStarting has not been called yet");
            lastStartFinishTimeNanos = OptionalLong.of(System.nanoTime());
        }
        super.containerIsStarted(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStarted(this, containerInfo));
    }

    @Override
    protected void containerIsStopping(InspectContainerResponse containerInfo)
    {
        super.containerIsStopping(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStopping(this, containerInfo));
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo)
    {
        super.containerIsStopped(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStopped(this, containerInfo));
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
        ExecResult result = execCommandForResult(command);
        if (result.getExitCode() == 0) {
            return result.getStdout();
        }
        String fullCommand = Joiner.on(" ").join(command);
        throw new RuntimeException(format("Could not execute command '%s' in container %s: %s", fullCommand, logicalName, result.getStderr()));
    }

    public ExecResult execCommandForResult(String... command)
    {
        String fullCommand = Joiner.on(" ").join(command);
        if (!isRunning()) {
            throw new RuntimeException(format("Could not execute command '%s' in stopped container %s", fullCommand, logicalName));
        }

        log.info("Executing command '%s' in container %s", fullCommand, logicalName);

        try {
            return executor.getAsync(() -> execInContainer(command)).get();
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
            ExecResult execResult = execCommandForResult("/usr/bin/find", path, "-type", "f", "-print");
            if (execResult.getExitCode() != 0) {
                log.warn("Could not list files in container '%s' path %s: %s", logicalName, path, execResult.getStderr());
                return ImmutableList.of();
            }
            return Splitter.on("\n")
                    .omitEmptyStrings()
                    .splitToList(execResult.getStdout());
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
            return super.isHealthy();
        }
        catch (RuntimeException ignored) {
            // Container without health checks will throw
            return true;
        }
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

    public boolean isTemporary()
    {
        return this.temporary;
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
