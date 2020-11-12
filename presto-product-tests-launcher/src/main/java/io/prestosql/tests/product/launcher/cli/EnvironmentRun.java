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
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.airlift.units.Duration;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.cli.EnvironmentUp.EnvironmentUpOptions;
import io.prestosql.tests.product.launcher.cli.TestRun.TestRunOptions;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import org.testcontainers.DockerClientFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.cli.DockerUtils.dockerRun;
import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_ENVIRONMENT_LABEL_NAME;
import static io.prestosql.tests.product.launcher.env.Environment.PRODUCT_TEST_LAUNCHER_NETWORK;
import static io.prestosql.tests.product.launcher.env.Environments.pruneEnvironment;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Mixin;
import static picocli.CommandLine.Option;

/**
 * Runs a command (typically {@code java ...}) against an environment, similar to {@code docker run} but for product test environments.
 * Used by IntelliJ product test plugin, and useful also for debugging.
 */
@Command(
        name = "run",
        description = "Run a command against environment",
        usageHelpAutoWidth = true)
public final class EnvironmentRun
        implements Callable<Integer>
{
    private static final DockerClient dockerClient = DockerClientFactory.lazyClient();
    private final Module additionalEnvironments;

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Mixin
    public EnvironmentUpOptions environmentUpOptions = new EnvironmentUpOptions();

    @Option(names = "--image", paramLabel = "<image>", description = "Image to use for tests container")
    public String image = firstNonNull(getenv("PT_IMAGE"), "azul/zulu-openjdk-alpine:11.0.8-11.41.23");

    @Option(names = "--reuse", description = "Reuse existing environment")
    public boolean reuseEnabled = "true".equalsIgnoreCase(getenv("TESTCONTAINERS_REUSE_ENABLE"));  // same environment variable name as in integration tests to enable reuse

    @Parameters(paramLabel = "<command>", description = "Command to run inside the environment")
    public List<String> command;

    public EnvironmentRun(Extensions extensions)
    {
        this.additionalEnvironments = requireNonNull(extensions, "extensions is null").getAdditionalEnvironments();
    }

    @Override
    public Integer call()
    {
        requireNonNull(environmentUpOptions.environment, "No environment provided; please set PT_ENVIRONMENT environment variable");

        environmentUpOptions.background = true;

        // too easy to break something in TestRun, for now let's reuse it by "patching"
        TestRunOptions testRunOptions = new TestRunOptions();
        testRunOptions.attach = true;
        String stubPath = "/tmp/unused";
        testRunOptions.testJar = new File(stubPath);
        testRunOptions.testArguments = ImmutableList.of(stubPath);
        testRunOptions.timeout = Duration.valueOf("1h");    // unused
        testRunOptions.reportsDir = Path.of(stubPath);
        testRunOptions.logsDirBase = Optional.empty();
        testRunOptions.environment = environmentUpOptions.environment;
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(binder -> binder.bind(EnvironmentRun.class).toInstance(this))
                        .add(environmentUpOptions.toModule())
                        .add(testRunOptions.toModule())
                        .add(binder -> binder.bind(EnvironmentUp.Execution.class))
                        .add(binder -> binder.bind(TestRun.Execution.class))
                        .add(binder -> binder.bind(EnvironmentRunProductTestConfigurer.class))
                        .build(),
                Execution.class, false);
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final EnvironmentRun environmentRun;
        private final EnvironmentUpOptions environmentUpOptions;
        private final EnvironmentUp.Execution environmentUpExecution;
        private final EnvironmentRunProductTestConfigurer productTestConfigurer;

        @Inject
        public Execution(EnvironmentRun environmentRun, EnvironmentUpOptions environmentUpOptions, EnvironmentUp.Execution environmentUpExecution,
                EnvironmentRunProductTestConfigurer productTestConfigurer)
        {
            this.environmentRun = environmentRun;
            this.environmentUpOptions = environmentUpOptions;
            this.environmentUpExecution = environmentUpExecution;
            this.productTestConfigurer = productTestConfigurer;
        }

        @Override
        public Integer call()
        {
            if (!canReuseEnvironment()) {
                environmentUpExecution.call();
            }

            try {
                return dockerRun(createCreateContainerCmd());
            }
            finally {
                if (!environmentRun.reuseEnabled) {
                    pruneEnvironment();
                }
            }
        }

        private boolean canReuseEnvironment()
        {
            if (!environmentRun.reuseEnabled) {
                return false;
            }
            if (dockerClient.listContainersCmd()
                    .withNameFilter(ImmutableList.of("ptl-presto-master"))
                    .exec().isEmpty()) {
                return false;   // no presto master
            }
            Network network;
            try {
                network = dockerClient
                        .inspectNetworkCmd()
                        .withNetworkId(PRODUCT_TEST_LAUNCHER_NETWORK)
                        .exec();
            }
            catch (NotFoundException e) {
                return false;   // wrong environment
            }
            return environmentUpOptions.environment.equals(network.getLabels().get(PRODUCT_TEST_LAUNCHER_ENVIRONMENT_LABEL_NAME));
        }

        private CreateContainerCmd createCreateContainerCmd()
        {
            String cwd = new File(".").getAbsolutePath();
            Set<String> alreadyMounted = new HashSet<>();
            List<Bind> binds = Streams.concat(Stream.of(cwd, getProperty("user.home")), environmentRun.command.stream())
                    .flatMap(p -> Stream.of(p.split("[:=]")))
                    .filter(p -> p.startsWith("/") && new File(p).exists())
                    .sorted()   // dirs first, then children (files, subdirs)
                    .filter(p -> alreadyMounted.stream().noneMatch(it -> p.startsWith(it + "/")) && alreadyMounted.add(p))  // remove duplicated mounts
                    .map(fileParameter ->
                            Bind.parse(format("%s:%s:ro", fileParameter, fileParameter)))
                    .collect(toImmutableList());
            CreateContainerCmd container = dockerClient.createContainerCmd(environmentRun.image)
                    .withCmd(environmentRun.command)
                    .withWorkingDir(cwd)
                    .withHostConfig(HostConfig.newHostConfig()
                            .withNetworkMode(PRODUCT_TEST_LAUNCHER_NETWORK)
                            .withBinds(binds));
            configureDebuggerIfNeeded(container);
            productTestConfigurer.configureIfNeeded(container);
            return container;
        }

        private void configureDebuggerIfNeeded(CreateContainerCmd container)
        {
            container.withCmd(Stream.of(requireNonNull(container.getCmd()))
                    .map(arg -> {
                        // patch string like -agentlib:jdwp=transport=dt_socket,address=127.0.0.1:50247,suspend=y,server=n
                        if (!arg.startsWith("-agentlib:jdwp")) {
                            return arg;
                        }
                        return arg.replace("127.0.0.1", "host.docker.internal");
                    })
                    .collect(toImmutableList()));
        }
    }
}
