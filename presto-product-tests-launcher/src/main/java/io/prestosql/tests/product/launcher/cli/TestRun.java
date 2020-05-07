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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;

import javax.inject.Inject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.cli.EnvironmentCommandExecution.ENVIRONMENT_READY_PORT;
import static io.prestosql.tests.product.launcher.docker.ContainerUtil.exposePort;
import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_TEMPTO_PROFILE_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@Command(name = "run", description = "Presto product test launcher")
public final class TestRun
        implements Runnable
{
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

        @Option(name = "--reuse-existing-environment", description = "Should reuse existing running docker containers")
        public boolean reuse;

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

        private final PathResolver pathResolver;
        private final boolean debug;
        private final File testJar;
        private final List<String> testArguments;
        private EnvironmentCommandExecution execution;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, PathResolver pathResolver, EnvironmentOptions environmentOptions, TestRunOptions testRunOptions)
        {
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            requireNonNull(environmentOptions, "environmentOptions is null");
            this.debug = environmentOptions.debug;
            this.testJar = requireNonNull(testRunOptions.testJar, "testOptions.testJar is null");
            this.testArguments = ImmutableList.copyOf(requireNonNull(testRunOptions.testArguments, "testOptions.testArguments is null"));
            execution = new EnvironmentCommandExecution(environmentFactory, testRunOptions.environment, testRunOptions.reuse, this::extendEnvironment, "tests");
        }

        @Override
        public void run()
        {
            execution.run();
        }

        private void extendEnvironment(Environment.Builder environment)
        {
            environment.configureContainer("tests", container -> {
                container.addExposedPort(ENVIRONMENT_READY_PORT);

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
        }
    }
}
