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
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.HealthCheck;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.core.InvocationBuilder.AsyncResultCallback;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.log.Logger;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.Timeout;
import net.jodah.failsafe.function.CheckedRunnable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.images.builder.Transferable;

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
import java.util.UUID;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
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

    private static final Timeout asyncTimeout = Timeout.of(ofSeconds(30))
            .withCancel(true);

    private static final FailsafeExecutor executor = Failsafe
            .with(asyncTimeout)
            .with(Executors.newCachedThreadPool(daemonThreadsNamed("docker-container-%d")));

    private String logicalName;
    private List<String> logPaths = new ArrayList<>();
    private Optional<EnvironmentListener> listener = Optional.empty();

    public DockerContainer(String dockerImageName, String logicalName)
    {
        super(dockerImageName);
        this.logicalName = requireNonNull(logicalName, "logicalName is null");

        // workaround for https://github.com/testcontainers/testcontainers-java/pull/2861
        setCopyToFileContainerPathMap(new LinkedHashMap<>());
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

    public DockerContainer withHealthCheck(String healthCheckScript)
    {
        HealthCheck cmd = new HealthCheck()
                .withTest(ImmutableList.of("CMD", "health.sh"))
                .withInterval(NANOSECONDS_PER_SECOND * 15)
                .withStartPeriod(NANOSECONDS_PER_SECOND * 5 * 60)
                .withRetries(3); // try health checking 3 times before marking container as unhealthy

        return withCopyFileToContainer(forHostPath(healthCheckScript), "/usr/local/bin/health.sh")
                .withCreateContainerCmdModifier(command -> command.withHealthcheck(cmd));
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo)
    {
        super.containerIsStarting(containerInfo);
        this.listener.ifPresent(listener -> listener.containerStarting(this, containerInfo));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo)
    {
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
            executor.runAsync(copy).whenComplete((ignore, throwable) -> {
                if (throwable == null) {
                    log.info("Copied files into %s %s in %.1f s", this, containerPath, stopwatch.elapsed(MILLISECONDS) / 1000.);
                }
                else {
                    log.warn("Could not copy files into %s %s: %s", this, containerPath, getStackTraceAsString((Throwable) throwable));
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

    public void clearDependencies()
    {
        dependencies.clear();
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
            catch (Exception e) {
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

            ExecResult result = execInContainer("tar", "-cvf", containerLogsArchive, "-T", containerLogsListingFile);
            if (result.getExitCode() != 0) {
                throw new RuntimeException(format("Could not create logs tar: %s", result.getStderr()));
            }

            copyFileFromContainer(containerLogsArchive, hostPath.resolve(format("%s/logs.tar.gz", logicalName)));
        }
        catch (IOException e) {
            log.warn("Could not create temporary file: %s", e);
        }
        catch (Exception e) {
            log.warn("Could not copy logs archive from %s: %s", logicalName, getStackTraceAsString(e));
        }
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
        catch (Exception e) {
            log.warn("Could not copy file from %s to %s: %s", filename, targetPath, getStackTraceAsString(e));
        }
    }

    private List<String> listFilesInContainer(String path)
    {
        if (!isRunning()) {
            log.warn("Could not list files in %s for stopped container %s", path, logicalName);
            return ImmutableList.of();
        }

        try {
            ExecResult result = (ExecResult) executor.getAsync(() -> execInContainer("/usr/bin/find", path, "-type", "f", "-print")).get();

            if (result.getExitCode() == 0L) {
                return Splitter.on("\n")
                        .omitEmptyStrings()
                        .splitToList(result.getStdout());
            }

            log.warn("Could not list files in %s: %s", path, result.getStderr());
        }
        catch (Exception e) {
            log.warn("Could not list files in container '%s': %s", logicalName, e);
        }

        return ImmutableList.of();
    }

    public Optional<Statistics> getStats()
    {
        if (!isRunning()) {
            log.warn("Could not get statistics for stopped container %s", logicalName);
            return Optional.empty();
        }

        try (DockerClient client = DockerClientFactory.lazyClient(); AsyncResultCallback<Statistics> callback = new AsyncResultCallback<>()) {
            client.statsCmd(getContainerId()).exec(callback);
            return Optional.ofNullable(executor.get(callback::awaitResult))
                    .map(Statistics.class::cast);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (Exception e) {
            log.error("Could not fetch container %s statistics: %s", logicalName, getStackTraceAsString(e));
            return Optional.empty();
        }
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
        catch (Exception e) {
            log.warn("Could not stop container correctly: %s", getStackTraceAsString(e));
        }

        checkState(!isRunning(), "Container %s is still running", logicalName);
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
