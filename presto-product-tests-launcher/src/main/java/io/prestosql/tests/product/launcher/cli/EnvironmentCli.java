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
package io.prestosql.tests.product.launcher.cli;

import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.docker.ContainerUtil;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.testcontainers.ExistingNetwork;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.killContainers;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.CLI;
import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@Command(
        name = "cli",
        description = "Start Presto cli for environment",
        usageHelpAutoWidth = true)
public final class EnvironmentCli
        implements Callable<Integer>
{
    private static final Logger log = Logger.get(EnvironmentCli.class);

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public EnvironmentCliOptions environmentCliOptions = new EnvironmentCliOptions();

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    private final Module additionalEnvironments;

    public EnvironmentCli(Extensions extensions)
    {
        this.additionalEnvironments = requireNonNull(extensions, "extensions is null").getAdditionalEnvironments();
    }

    @Override
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(environmentCliOptions.toModule())
                        .build(),
                EnvironmentCli.Execution.class);
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final EnvironmentFactory environmentFactory;
        private final EnvironmentCliOptions environmentCliOptions;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, EnvironmentCliOptions environmentCliOptions)
        {
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.environmentCliOptions = requireNonNull(environmentCliOptions, "environmentCliOptions is null");
        }

        @Override
        public Integer call()
        {
            log.info("Starting CLI container %s for environment %s", CLI, environmentCliOptions.environment);

            try (DockerClient dockerClient = DockerClientFactory.lazyClient()) {
                killContainers(
                        dockerClient,
                        listContainersCmd -> listContainersCmd.withNameFilter(singletonList(CLI)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            Environment.Builder environment = environmentFactory.get(environmentCliOptions.environment)
                    .setContainerOutputMode(DockerContainer.OutputMode.DISCARD);

            ImmutableList.Builder<String> containers = ImmutableList.builder();
            environment.configureContainers(container -> containers.add(container.getLogicalName()));

            Container<?> container = environment.build().getContainer(COORDINATOR);
            DockerContainer dockerContainer = new DockerContainer(container.getDockerImageName(), CLI)
                    .withCreateContainerCmdModifier(cmd -> cmd.withName(CLI));

            createCliContainer(dockerContainer, environmentCliOptions.cliPackage);
            killContainersReaperContainer();

            dockerContainer.start();

            try {
                attachToCli(dockerContainer);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return ExitCode.OK;
        }

        private DockerContainer createCliContainer(DockerContainer container, Path cliPackage)
        {
            container.setNetwork(new ExistingNetwork(Environment.PRODUCT_TEST_LAUNCHER_NETWORK));

            return container
                    .withCopyFileToContainer(forHostPath(cliPackage), "/docker/presto-cli.jar")
                    .withCommand("--server", "presto-master:8080", "--progress", "--debug")
                    .withCreateContainerCmdModifier(modifier -> modifier
                            .withStdinOpen(true)
                            .withEntrypoint("/docker/presto-cli.jar")
                            .withTty(false));
        }

        private void attachToCli(DockerContainer container)
                throws InterruptedException
        {
            log.info("Attaching to " + container.getContainerId());

            FrameConsumerResultCallback callback = new FrameConsumerResultCallback();

            callback.addConsumer(OutputFrame.OutputType.STDERR, frame -> {
                try {
                    System.err.write(frame.getBytes());
                    System.err.flush();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            callback.addConsumer(OutputFrame.OutputType.STDOUT, frame -> {
                try {
                    System.out.write(frame.getBytes());
                    System.out.flush();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            callback.addConsumer(OutputFrame.OutputType.END, frame -> System.exit(0));

            try (DockerClient client = DockerClientFactory.lazyClient()) {
                client.attachContainerCmd(container.getContainerId())
                        .withStdErr(true)
                        .withStdOut(true)
                        .withFollowStream(true)
                        .withLogs(true)
                        .withStdIn(System.in)
                        .exec(callback);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            callback.awaitCompletion();
        }

        private void killContainersReaperContainer()
        {
            try (DockerClient dockerClient = DockerClientFactory.lazyClient()) {
                log.info("Killing the testcontainers reaper container (Ryuk) so that CLI can stay alive");
                ContainerUtil.killContainersReaperContainer(dockerClient);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class EnvironmentCliOptions
    {
        private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

        @Option(names = "--environment", paramLabel = "<environment>", description = "Name of the environment to start", required = true)
        public String environment;

        @Option(names = "--cli-package", paramLabel = "<cli package>", description = "Path to Presto CLI package " + DEFAULT_VALUE, defaultValue = "presto-cli/target/presto-cli-${project.version}-executable.jar")
        public Path cliPackage;

        public Module toModule()
        {
            return binder -> binder.bind(EnvironmentCliOptions.class).toInstance(this);
        }
    }
}
