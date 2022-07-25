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
package io.trino.plugin.deltalake.util;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.testing.containers.wait.strategy.SelectedPortWaitStrategy;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class TestingHadoop
        implements Closeable
{
    private static final Logger log = Logger.get(TestingHadoop.class);
    public static final String HADOOP_MASTER = "hadoop-master";

    public static final int METASTORE_PORT = 9083;
    public static final int PROXY_PORT = 1180;
    public static final int HIVE_SERVER_PORT = 10000;

    public static final Set<Integer> HIVE_PORTS = ImmutableSet.of(
            METASTORE_PORT,
            PROXY_PORT,
            HIVE_SERVER_PORT);

    private final DockerContainer container;
    private final Closer closer = Closer.create();

    private TestingHadoop(
            String image,
            Set<Integer> ports,
            Map<String, String> resourcesToMount,
            Map<String, String> filesToMount,
            Map<String, String> extraHosts,
            Optional<Network> network,
            int retryLimit)
    {
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxRetries(retryLimit)
                .onRetry(event -> log.warn(
                        "TestingHadoop initialization failed (attempt %s), will retry. Exception: %s",
                        event.getAttemptCount(),
                        event.getLastFailure().getMessage()));

        container = Failsafe.with(retryPolicy).get(() -> startHadoopContainer(image, ports, resourcesToMount, filesToMount, extraHosts, network));
        closer.register(container::stop);

        // HACK: even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
        // (e.g: in: NameNodeProxies.createProxy)
        // This adds a static resolution for hadoop-master to internal container ip address
        //noinspection deprecation
        NetUtils.addStaticResolution(HADOOP_MASTER, container.getContainerInfo().getNetworkSettings().getIpAddress());
    }

    private DockerContainer startHadoopContainer(
            String image,
            Set<Integer> ports,
            Map<String, String> resourcesToMount,
            Map<String, String> filesToMount,
            Map<String, String> extraHosts,
            Optional<Network> network)
    {
        DockerContainer container = new DockerContainer(image);
        for (int port : ports) {
            container.addExposedPort(port);
        }

        network.ifPresent(container::withNetwork);

        container.withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .withCreateContainerCmdModifier(setHostName(HADOOP_MASTER))
                .waitingFor(new SelectedPortWaitStrategy(ports)
                        // 5 minutes is plenty of a timeout as it does not limit the time which is spent on downloading images
                        // Actual container startup is sub-minute in typical case.
                        .withStartupTimeout(Duration.ofMinutes(5)));

        resourcesToMount.forEach((resourceName, dockerPath) -> container.withCopyFileToContainer(MountableFile.forClasspathResource(resourceName), dockerPath));
        filesToMount.forEach((filePath, dockerPath) -> container.withCopyFileToContainer(MountableFile.forHostPath(filePath), dockerPath));
        extraHosts.forEach(container::withExtraHost);
        container.withLogConsumer(new PrintingLogConsumer(format("%-20s| ", "hadoop")));
        container.start();
        return container;
    }

    public String getMetastoreAddress()
    {
        return "thrift://" + container.getHost() + ":" + getMetastorePort();
    }

    public String getJdbcAddress()
    {
        return "jdbc:hive2://" + container.getHost() + ":" + getExposedPort(HIVE_SERVER_PORT);
    }

    public int getMetastorePort()
    {
        return getExposedPort(METASTORE_PORT);
    }

    public int getProxyPort()
    {
        return getExposedPort(PROXY_PORT);
    }

    public int getExposedPort(int port)
    {
        return container.getMappedPort(port);
    }

    public void runCommandInContainerWithRetries(List<String> commandAndArgs, Runnable onFailureCallaback)
    {
        Failsafe.with(new RetryPolicy<>()
                        .withBackoff(5, 10, SECONDS)
                        .withMaxDuration(Duration.of(5, MINUTES))
                        .withMaxAttempts(10)
                        .onRetry(event -> log.info("Command failed fo the  %d time, retrying", event.getAttemptCount())))
                .onFailure(ignored -> onFailureCallaback.run())
                .run(() -> runCommandInContainer(commandAndArgs.toArray(new String[0])));
    }

    public void runCommandInContainer(String... commandAndArgs)
    {
        try {
            Container.ExecResult execResult = container.execInContainer(commandAndArgs);
            if (execResult.getExitCode() != 0) {
                String message = format("Command [%s] exited with %s", printableCommand(commandAndArgs), execResult.getExitCode());
                log.error("%s", message);
                log.error("stderr: %s", execResult.getStderr());
                log.error("stdout: %s", execResult.getStdout());
                throw new RuntimeException(message);
            }
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Exception while running command: " + printableCommand(commandAndArgs), e);
        }
    }

    public Container.ExecResult executeInContainer(String... commandAndArgs)
    {
        try {
            return container.execInContainer(commandAndArgs);
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Exception while running command: " + printableCommand(commandAndArgs), e);
        }
    }

    public void runOnHive(String query)
    {
        runCommandInContainer("beeline", "-u", "jdbc:hive2://localhost:10000/default", "-n", "hive", "-e", query);
    }

    private String printableCommand(String... commandAndArgs)
    {
        return String.join(" ", Arrays.asList(commandAndArgs));
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    private static Consumer<CreateContainerCmd> setHostName(String hostName)
    {
        return createContainerCmd -> createContainerCmd.withHostName(hostName);
    }

    public DockerContainer getContainer()
    {
        return container;
    }

    public static Builder builder(String image)
    {
        return new Builder(image);
    }

    public static final class Builder
    {
        private final String image;
        private Set<Integer> ports = HIVE_PORTS;
        private Map<String, String> resourcesToMount = ImmutableMap.of();
        private Map<String, String> filesToMount = ImmutableMap.of();
        private Map<String, String> extraHosts = ImmutableMap.of();
        private Optional<Network> network = Optional.empty();
        private int retryLimit = 3;

        public Builder(String image)
        {
            this.image = requireNonNull(image, "image is null");
        }

        public Builder setPorts(Set<Integer> ports)
        {
            this.ports = ImmutableSet.copyOf(requireNonNull(ports, "ports is null"));
            return this;
        }

        public Builder setResourcesToMount(Map<String, String> resourceFilesToMount)
        {
            this.resourcesToMount = ImmutableMap.copyOf(requireNonNull(resourceFilesToMount, "resourceFilesToMount is null"));
            return this;
        }

        public Builder setFilesToMount(Map<String, String> hostFilesToMount)
        {
            this.filesToMount = ImmutableMap.copyOf(requireNonNull(hostFilesToMount, "hostFilesToMount is null"));
            return this;
        }

        public Builder setExtraHosts(Map<String, String> extraHosts)
        {
            this.extraHosts = ImmutableMap.copyOf(requireNonNull(extraHosts, "extraHosts is null"));
            return this;
        }

        public Builder setNetwork(Network network)
        {
            this.network = Optional.of(requireNonNull(network, "network is null"));
            return this;
        }

        public Builder setRetryLimit(int retryLimit)
        {
            checkArgument(retryLimit >= 1, "retryLimit must be greater than or equal to 1");
            this.retryLimit = retryLimit;
            return this;
        }

        public TestingHadoop build()
        {
            return new TestingHadoop(image, ports, resourcesToMount, filesToMount, extraHosts, network, retryLimit);
        }
    }
}
