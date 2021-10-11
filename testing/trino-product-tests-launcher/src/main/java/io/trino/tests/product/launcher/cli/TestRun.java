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
package io.trino.tests.product.launcher.cli;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.env.SupportedTrinoJdk;
import io.trino.tests.product.launcher.testcontainers.ExistingNetwork;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Timeout;
import net.jodah.failsafe.TimeoutExceededException;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.launcher.cli.Commands.runCommand;
import static io.trino.tests.product.launcher.env.DockerContainer.cleanOrCreateHostPath;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentListener.getStandardListeners;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.trino.tests.product.launcher.testcontainers.PortBinder.unsafelyExposePort;
import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.BindMode.READ_WRITE;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(
        name = "run",
        description = "Run a Trino product test",
        usageHelpAutoWidth = true)
public final class TestRun
        implements Callable<Integer>
{
    private static final Logger log = Logger.get(TestRun.class);

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    @SuppressWarnings("unused")
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
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(testRunOptions.toModule())
                        .build(),
                TestRun.Execution.class);
    }

    public static class TestRunOptions
    {
        private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

        @Option(names = "--test-jar", paramLabel = "<jar>", description = "Path to test JAR " + DEFAULT_VALUE, defaultValue = "${product-tests.module}/target/${product-tests.name}-${project.version}-executable.jar")
        public File testJar;

        @Option(names = "--cli-executable", paramLabel = "<jar>", description = "Path to CLI executable " + DEFAULT_VALUE, defaultValue = "${cli.bin}")
        public File cliJar;

        @Option(names = "--environment", paramLabel = "<environment>", description = "Name of the environment to start", required = true)
        public String environment;

        @Option(names = "--option", paramLabel = "<option>", description = "Extra options to provide to environment (property can be used multiple times; format is key=value)")
        public Map<String, String> extraOptions = new HashMap<>();

        @Option(names = "--attach", description = "attach to an existing environment")
        public boolean attach;

        @Option(names = "--reports-dir", paramLabel = "<dir>", description = "Location of the reports directory " + DEFAULT_VALUE, defaultValue = "${product-tests.module}/target/reports")
        public Path reportsDir;

        @Option(names = "--logs-dir", paramLabel = "<dir>", description = "Location of the exported logs directory " + DEFAULT_VALUE)
        public Optional<Path> logsDirBase;

        @Option(names = "--startup-retries", paramLabel = "<retries>", description = "Environment startup retries " + DEFAULT_VALUE, defaultValue = "5")
        public Integer startupRetries = 5;

        @Option(names = "--timeout", paramLabel = "<timeout>", description = "Maximum duration of tests execution " + DEFAULT_VALUE, defaultValue = "999d")
        public Duration timeout;

        @Parameters(paramLabel = "<argument>", description = "Test arguments")
        public List<String> testArguments;

        public Module toModule()
        {
            return binder -> binder.bind(TestRunOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Callable<Integer>
    {
        private static final String CONTAINER_REPORTS_DIR = "/docker/test-reports";
        private final EnvironmentFactory environmentFactory;
        private final boolean debug;
        private final SupportedTrinoJdk jdkVersion;
        private final File testJar;
        private final File cliJar;
        private final List<String> testArguments;
        private final String environment;
        private final boolean attach;
        private final Duration timeout;
        private final DockerContainer.OutputMode outputMode;
        private final int startupRetries;
        private final Path reportsDirBase;
        private final Optional<Path> logsDirBase;
        private final EnvironmentConfig environmentConfig;
        private final Map<String, String> extraOptions;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, EnvironmentOptions environmentOptions, EnvironmentConfig environmentConfig, TestRunOptions testRunOptions)
        {
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            requireNonNull(environmentOptions, "environmentOptions is null");
            this.debug = environmentOptions.debug;
            this.jdkVersion = requireNonNull(environmentOptions.jdkVersion, "environmentOptions.jdkVersion is null");
            this.testJar = requireNonNull(testRunOptions.testJar, "testRunOptions.testJar is null");
            this.cliJar = requireNonNull(testRunOptions.cliJar, "testRunOptions.cliJar is null");
            this.testArguments = ImmutableList.copyOf(requireNonNull(testRunOptions.testArguments, "testRunOptions.testArguments is null"));
            this.environment = requireNonNull(testRunOptions.environment, "testRunOptions.environment is null");
            this.attach = testRunOptions.attach;
            this.timeout = requireNonNull(testRunOptions.timeout, "testRunOptions.timeout is null");
            this.outputMode = requireNonNull(environmentOptions.output, "environmentOptions.output is null");
            this.startupRetries = testRunOptions.startupRetries;
            this.reportsDirBase = requireNonNull(testRunOptions.reportsDir, "testRunOptions.reportsDirBase is empty");
            this.logsDirBase = requireNonNull(testRunOptions.logsDirBase, "testRunOptions.logsDirBase is empty");
            this.environmentConfig = requireNonNull(environmentConfig, "environmentConfig is null");
            this.extraOptions = ImmutableMap.copyOf(requireNonNull(testRunOptions.extraOptions, "testRunOptions.extraOptions is null"));
        }

        @Override
        public Integer call()
        {
            long timeoutMillis = timeout.toMillis();
            if (timeoutMillis == 0) {
                log.error("Timeout %s exhausted", timeout);
                return ExitCode.SOFTWARE;
            }

            try {
                int exitCode = Failsafe
                        .with(Timeout.of(java.time.Duration.ofMillis(timeoutMillis))
                                .withCancel(true))
                        .get(this::tryExecuteTests);

                log.info("Tests execution completed with code %d", exitCode);
                return exitCode;
            }
            catch (TimeoutExceededException ignored) {
                log.error("Test execution exceeded timeout of %s", timeout);
            }
            catch (Throwable e) {
                log.error(e, "Failure");
            }

            return ExitCode.SOFTWARE;
        }

        private Integer tryExecuteTests()
        {
            try (Environment environment = startEnvironment()) {
                return toIntExact(environment.awaitTestsCompletion());
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to execute tests");
                return ExitCode.SOFTWARE;
            }
        }

        private Environment startEnvironment()
        {
            Environment environment = getEnvironment();

            Collection<DockerContainer> allContainers = environment.getContainers();
            DockerContainer testsContainer = environment.getContainer(TESTS);

            if (!attach) {
                // Reestablish dependency on every startEnvironment attempt
                Collection<DockerContainer> environmentContainers = allContainers.stream()
                        .filter(container -> !container.equals(testsContainer))
                        .collect(toImmutableList());
                testsContainer.dependsOn(environmentContainers);

                log.info("Starting environment '%s' with config '%s' and options '%s'. Trino will be started using JAVA_HOME: %s.", this.environment, environmentConfig.getConfigName(), extraOptions, jdkVersion.getJavaHome());
                environment.start();
            }
            else {
                testsContainer.setNetwork(new ExistingNetwork(Environment.PRODUCT_TEST_LAUNCHER_NETWORK));
                // TODO prune previous ptl-tests container
                testsContainer.start();
            }

            return environment;
        }

        private Environment getEnvironment()
        {
            Environment.Builder builder = environmentFactory.get(environment, environmentConfig, extraOptions)
                    .setContainerOutputMode(outputMode)
                    .setStartupRetries(startupRetries)
                    .setLogsBaseDir(logsDirBase);

            builder.configureContainer(TESTS, this::mountReportsDir);
            builder.configureContainer(TESTS, container -> {
                List<String> temptoJavaOptions = Splitter.on(" ").omitEmptyStrings().splitToList(
                        container.getEnvMap().getOrDefault("TEMPTO_JAVA_OPTS", ""));

                if (debug) {
                    temptoJavaOptions = new ArrayList<>(temptoJavaOptions);
                    temptoJavaOptions.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5007");
                    unsafelyExposePort(container, 5007); // debug port
                }

                if (System.getenv("CONTINUOUS_INTEGRATION") != null) {
                    container.withEnv("CONTINUOUS_INTEGRATION", "true");
                }

                container
                        // the test jar is hundreds MB and file system bind is much more efficient
                        .withFileSystemBind(testJar.getPath(), "/docker/test.jar", READ_ONLY)
                        .withFileSystemBind(cliJar.getPath(), "/docker/trino-cli", READ_ONLY)
                        .withCopyFileToContainer(forClasspathResource("docker/presto-product-tests/common/standard/set-trino-cli.sh"), "/etc/profile.d/set-trino-cli.sh")
                        .withEnv("JAVA_HOME", jdkVersion.getJavaHome())
                        .withCommand(ImmutableList.<String>builder()
                                .add(
                                        jdkVersion.getJavaCommand(),
                                        "-Xmx1g",
                                        // Force Parallel GC to ensure MaxHeapFreeRatio is respected
                                        "-XX:+UseParallelGC",
                                        "-XX:MinHeapFreeRatio=10",
                                        "-XX:MaxHeapFreeRatio=10",
                                        "-Djava.util.logging.config.file=/docker/presto-product-tests/conf/tempto/logging.properties",
                                        "-Duser.timezone=Asia/Kathmandu",
                                        // Tempto has progress logging built in
                                        "-DProgressLoggingListener.enabled=false")
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
                                .build().toArray(new String[0]));
            });

            builder.setAttached(attach);

            return builder.build(getStandardListeners(logsDirBase));
        }

        private static Iterable<? extends String> reportsDirOptions(Path path)
        {
            if (isNullOrEmpty(path.toString())) {
                return ImmutableList.of();
            }

            return ImmutableList.of("--report-dir", CONTAINER_REPORTS_DIR);
        }

        private void mountReportsDir(DockerContainer container)
        {
            if (isNullOrEmpty(reportsDirBase.toString())) {
                return;
            }

            cleanOrCreateHostPath(reportsDirBase);
            container.withFileSystemBind(reportsDirBase.toString(), CONTAINER_REPORTS_DIR, READ_WRITE);
            log.info("Exposing tests report dir in host directory '%s'", reportsDirBase);
        }
    }
}
