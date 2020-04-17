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
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.Environments;
import org.testcontainers.containers.Container;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.exposePort;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@Command(name = "run", description = "Presto product test launcher")
public final class TestRun
        implements Runnable
{
    private static final Logger log = Logger.get(TestRun.class);

    @Inject
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Inject
    public TestRunOptions testRunOptions = new TestRunOptions();

    private Module additionalEnvironments;

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
                        .add(new EnvironmentModule(additionalEnvironments))
                        .add(environmentOptions.toModule())
                        .add(testRunOptions.toModule())
                        .build(),
                TestRun.Execution.class);
    }

    public static class TestRunOptions
    {
        @Option(name = "--test-jar", title = "test jar", description = "path to test jar")
        public File testJar = new File("presto-product-tests/target/presto-product-tests-${project.version}-executable.jar");

        @Option(name = "--environment", title = "environment", description = "the name of the environment to start", required = true)
        public String environment;

        @Arguments(description = "test arguments")
        public List<String> testArguments = new ArrayList<>();

        public Module toModule()
        {
            return binder -> {
                binder.bind(TestRunOptions.class).toInstance(this);
            };
        }
    }

    public static class Execution
            implements Runnable
    {
        private static final int TESTS_READY_PORT = 1970;

        private final EnvironmentFactory environmentFactory;
        private final PathResolver pathResolver;
        private final boolean debug;
        private final File testJar;
        private final List<String> testArguments;
        private final String environment;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, PathResolver pathResolver, EnvironmentOptions environmentOptions, TestRunOptions testRunOptions)
        {
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            requireNonNull(environmentOptions, "environmentOptions is null");
            this.debug = environmentOptions.debug;
            this.testJar = requireNonNull(testRunOptions.testJar, "testOptions.testJar is null");
            this.testArguments = ImmutableList.copyOf(requireNonNull(testRunOptions.testArguments, "testOptions.testArguments is null"));
            this.environment = requireNonNull(testRunOptions.environment, "testRunOptions.environment is null");
        }

        @Override
        public void run()
        {
            try (UncheckedCloseable ignore = this::cleanUp) {
                log.info("Pruning old environment(s)");
                Environments.pruneEnvironment();

                Environment environment = getEnvironment();

                log.info("Starting the environment '%s'", environment);
                environment.start();
                log.info("Environment '%s' started", environment);

                runTests(environment);
            }
            catch (Throwable e) {
                // log failure (tersely) because cleanup may take some time
                log.error("Failure: %s", e);
                throw e;
            }
        }

        private void cleanUp()
        {
            log.info("Done, cleaning up");
            Environments.pruneEnvironment();
        }

        private Environment getEnvironment()
        {
            Environment.Builder environment = environmentFactory.get(this.environment);

            environment.configureContainer("tests", container -> {
                container.addExposedPort(TESTS_READY_PORT);

                List<String> temptoJavaOptions = Splitter.on(" ").omitEmptyStrings().splitToList(
                        container.getEnvMap().getOrDefault("TEMPTO_JAVA_OPTS", ""));

                if (debug) {
                    temptoJavaOptions = new ArrayList<>(temptoJavaOptions);
                    temptoJavaOptions.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5007");
                    exposePort(container, 5007); // debug port
                }

                container
                        .withFileSystemBind(pathResolver.resolvePlaceholders(testJar).getPath(), "/docker/test.jar", READ_ONLY)
                        .withEnv("TESTS_HIVE_VERSION_MAJOR", System.getenv().getOrDefault("TESTS_HIVE_VERSION_MAJOR", "1"))
                        .withEnv("TESTS_HIVE_VERSION_MINOR", System.getenv().getOrDefault("TESTS_HIVE_VERSION_MINOR", "2"))
                        .withCommand(ImmutableList.<String>builder()
                                .add("bash", "-xeuc", "nc -l \"$1\" < /dev/null; shift; exec \"$@\"", "-")
                                .add(Integer.toString(TESTS_READY_PORT))
                                .add(
                                        "java",
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
                                                .add(System.getenv().getOrDefault("TEMPTO_ENVIRONMENT_CONFIG_FILE", "/dev/null"))
                                                .add(container.getEnvMap().getOrDefault("TEMPTO_CONFIG_FILES", "/dev/null"))
                                                .build()))
                                .addAll(testArguments)
                                .build().toArray(new String[0]));
            });

            return environment.build();
        }

        private void runTests(Environment environment)
        {
            log.info("Starting test execution");
            Container<?> container = environment.getContainer("tests");
            try {
                // Release waiter to let the tests run
                new Socket(container.getContainerIpAddress(), container.getMappedPort(TESTS_READY_PORT)).close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            log.info("Waiting for test completion");
            try {
                while (container.isRunning()) {
                    Thread.sleep(1000);
                }

                InspectContainerResponse containerInfo = container.getCurrentContainerInfo();
                ContainerState containerState = containerInfo.getState();
                Integer exitCode = containerState.getExitCode();
                log.info("Test container %s is %s, with exitCode %s", containerInfo.getId(), containerState.getStatus(), exitCode);
                checkState(exitCode != null, "No exitCode for tests container %s in state %s", container, containerState);
                if (exitCode != 0) {
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
