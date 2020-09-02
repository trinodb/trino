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

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.Environments;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.testcontainers.ExistingNetwork;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.exposePort;
import static io.prestosql.tests.product.launcher.env.DockerContainer.cleanOrCreateHostPath;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.BindMode.READ_WRITE;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(
        name = "run",
        description = "Run a Presto product test",
        usageHelpAutoWidth = true)
public final class TestRun
        implements Runnable
{
    private static final Logger log = Logger.get(TestRun.class);

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Mixin
    public TestRunOptions testRunOptions = new TestRunOptions();

    private final Module additionalEnvironments;

    public TestRun(Extensions extensions)
    {
        this.additionalEnvironments = requireNonNull(extensions, "extensions is null").getAdditionalEnvironments();
    }

    @Override
    public void run()
    {
        runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(testRunOptions.toModule())
                        .build(),
                TestRun.Execution.class);
    }

    public static class TestRunOptions
    {
        private static final String TARGET = "presto-product-tests/target";

        private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

        @Option(names = "--test-jar", paramLabel = "<jar>", description = "Path to test JAR " + DEFAULT_VALUE)
        public File testJar = new File(TARGET + "/presto-product-tests-${project.version}-executable.jar");

        @Option(names = "--environment", paramLabel = "<environment>", description = "Name of the environment to start", required = true)
        public String environment;

        @Option(names = "--attach", description = "attach to an existing environment")
        public boolean attach;

        @Option(names = "--reports-dir", paramLabel = "<dir>", defaultValue = TARGET + "/reports", description = "Location of the reports directory " + DEFAULT_VALUE)
        public Path reportsDir;

        @Option(names = "--logs-dir", paramLabel = "<dir>", description = "Location of the exported logs directory " + DEFAULT_VALUE, converter = OptionalPathConverter.class, defaultValue = "")
        public Optional<Path> logsDirBase;

        @Option(names = "--startup-retries", paramLabel = "<retries>", defaultValue = "5", description = "Environment startup retries " + DEFAULT_VALUE)
        public int startupRetries;

        @Parameters(paramLabel = "<argument>", description = "Test arguments")
        public List<String> testArguments;

        public Module toModule()
        {
            return binder -> binder.bind(TestRunOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Runnable
    {
        private static final String CONTAINER_REPORTS_DIR = "/docker/test-reports";
        private final EnvironmentFactory environmentFactory;
        private final PathResolver pathResolver;
        private final boolean debug;
        private final File testJar;
        private final List<String> testArguments;
        private final String environment;
        private final boolean attach;
        private final int startupRetries;
        private final Path reportsDirBase;
        private final Optional<Path> logsDirBase;
        private final EnvironmentConfig environmentConfig;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, PathResolver pathResolver, EnvironmentOptions environmentOptions, EnvironmentConfig environmentConfig, TestRunOptions testRunOptions)
        {
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            requireNonNull(environmentOptions, "environmentOptions is null");
            this.debug = environmentOptions.debug;
            this.testJar = requireNonNull(testRunOptions.testJar, "testOptions.testJar is null");
            this.testArguments = ImmutableList.copyOf(requireNonNull(testRunOptions.testArguments, "testOptions.testArguments is null"));
            this.environment = requireNonNull(testRunOptions.environment, "testRunOptions.environment is null");
            this.attach = testRunOptions.attach;
            this.startupRetries = testRunOptions.startupRetries;
            this.reportsDirBase = requireNonNull(testRunOptions.reportsDir, "testRunOptions.reportsDirBase is empty");
            this.logsDirBase = requireNonNull(testRunOptions.logsDirBase, "testRunOptions.logsDirBase is empty");
            this.environmentConfig = requireNonNull(environmentConfig, "environmentConfig is null");
        }

        @Override
        public void run()
        {
            RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                    .withMaxRetries(startupRetries)
                    .onFailedAttempt(event -> log.warn("Could not start environment '%s': %s", environment, getStackTraceAsString(event.getLastFailure())))
                    .onRetry(event -> log.info("Trying to start environment '%s', %d failed attempt(s)", environment, event.getAttemptCount() + 1))
                    .onSuccess(event -> log.info("Environment '%s' started in %s, %d attempt(s)", environment, event.getElapsedTime(), event.getAttemptCount()))
                    .onFailure(event -> log.info("Environment '%s' failed to start in attempt(s): %d: %s", environment, event.getAttemptCount(), event.getFailure()));

            try (UncheckedCloseable ignore = this::cleanUp) {
                Environment environment = Failsafe.with(retryPolicy)
                        .get(() -> tryStartEnvironment());

                awaitTestsCompletion(environment);
            }
            catch (Throwable e) {
                // log failure (tersely) because cleanup may take some time
                log.error("Failure: %s", e);
                throw e;
            }
        }

        private Environment tryStartEnvironment()
        {
            Environment environment = getEnvironment();

            if (!attach) {
                log.info("Pruning old environment(s)");
                Environments.pruneEnvironment();

                log.info("Starting the environment '%s' with configuration %s", this.environment, environmentConfig);
                environment.start();
            }
            else {
                DockerContainer tests = (DockerContainer) environment.getContainer("tests");
                tests.clearDependencies();
                tests.setNetwork(new ExistingNetwork(Environment.PRODUCT_TEST_LAUNCHER_NETWORK));
                // TODO prune previous ptl-tests container
                tests.start();
            }

            return environment;
        }

        private void cleanUp()
        {
            if (!attach) {
                log.info("Done, cleaning up");
                Environments.pruneEnvironment();
            }
        }

        private Environment getEnvironment()
        {
            Environment.Builder environment = environmentFactory.get(this.environment)
                    .containerDependsOnRest("tests");

            if (debug) {
                environment.configureContainers(Standard::enablePrestoJavaDebugger);
            }

            environment.configureContainer("tests", this::mountReportsDir);
            environment.configureContainer("tests", container -> {
                List<String> temptoJavaOptions = Splitter.on(" ").omitEmptyStrings().splitToList(
                        container.getEnvMap().getOrDefault("TEMPTO_JAVA_OPTS", ""));

                if (debug) {
                    temptoJavaOptions = new ArrayList<>(temptoJavaOptions);
                    temptoJavaOptions.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5007");
                    exposePort(container, 5007); // debug port
                }

                container
                        // the test jar is hundreds MB and file system bind is much more efficient
                        .withFileSystemBind(pathResolver.resolvePlaceholders(testJar).getPath(), "/docker/test.jar", READ_ONLY)
                        .withCommand(ImmutableList.<String>builder()
                                .add(
                                        "/usr/lib/jvm/zulu-11/bin/java",
                                        "-Xmx1g",
                                        // Force Parallel GC to ensure MaxHeapFreeRatio is respected
                                        "-XX:+UseParallelGC",
                                        "-XX:MinHeapFreeRatio=10",
                                        "-XX:MaxHeapFreeRatio=10",
                                        "-Djava.util.logging.config.file=/docker/presto-product-tests/conf/tempto/logging.properties",
                                        "-Duser.timezone=Asia/Kathmandu")
                                .addAll(temptoJavaOptions)
                                .add(
                                        "-jar", "/docker/test.jar",
                                        "--config", String.join(",", ImmutableList.<String>builder()
                                                .add("tempto-configuration.yaml") // this comes from classpath
                                                .add("/docker/presto-product-tests/conf/tempto/tempto-configuration-for-docker-default.yaml")
                                                .add(CONTAINER_TEMPTO_PROFILE_CONFIG)
                                                .add(environmentConfig.getTemptoEnvironmentConfigFile())
                                                .add(container.getEnvMap().getOrDefault("TEMPTO_CONFIG_FILES", "/dev/null"))
                                                .build()))
                                .addAll(testArguments)
                                .addAll(reportsDirOptions(reportsDirBase))
                                .build().toArray(new String[0]))
                        // this message marks that environment has started and tests are running
                        .waitingFor(new LogMessageWaitStrategy().withRegEx(".*\\[TestNG] Running.*"));
            });

            environmentConfig.extendEnvironment(this.environment).ifPresent(extender -> extender.extendEnvironment(environment));

            logsDirBase.ifPresent(environment::exposeLogsInHostPath);
            return environment
                    .build();
        }

        private static Iterable<? extends String> reportsDirOptions(Path path)
        {
            if (isNullOrEmpty(path.toString())) {
                return ImmutableList.of();
            }

            return ImmutableList.of("--report-dir", CONTAINER_REPORTS_DIR);
        }

        private void mountReportsDir(Container container)
        {
            if (isNullOrEmpty(reportsDirBase.toString())) {
                return;
            }

            cleanOrCreateHostPath(reportsDirBase);
            container.withFileSystemBind(reportsDirBase.toString(), CONTAINER_REPORTS_DIR, READ_WRITE);
            log.info("Exposing tests report dir in host directory '%s'", reportsDirBase);
        }

        private void awaitTestsCompletion(Environment environment)
        {
            Container<?> container = environment.getContainer("tests");

            log.info("Waiting for test completion");
            try {
                while (container.isRunning()) {
                    Thread.sleep(1000);
                }

                InspectContainerResponse containerInfo = container.getCurrentContainerInfo();
                ContainerState containerState = containerInfo.getState();
                Long exitCode = containerState.getExitCodeLong();
                log.info("Test container %s is %s, with exitCode %s", containerInfo.getId(), containerState.getStatus(), exitCode);
                checkState(exitCode != null, "No exitCode for tests container %s in state %s", container, containerState);
                if (exitCode != 0L) {
                    throw new RuntimeException("Tests exited with " + exitCode);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }
    }

    private interface UncheckedCloseable
            extends AutoCloseable
    {
        @Override
        void close();
    }
}
