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
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

@Command(name = "presto", description = "Presto CLI")
public final class PrestoCli
        implements Runnable
{
    @Inject
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Inject
    public PrestoCliOptions prestoCliOptions = new PrestoCliOptions();

    private Module additionalEnvironments;

    public PrestoCli(Extensions extensions)
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
                        .add(prestoCliOptions.toModule())
                        .build(),
                PrestoCli.Execution.class);
    }

    public static class PrestoCliOptions
    {
        @Option(name = "--cli-jar", title = "test jar", description = "path to cli jar")
        public File cliJar = new File("presto-product-tests/target/presto-cli-${project.version}-executable.jar");

        @Option(name = "--environment", title = "environment", description = "the name of the environment to start", required = true)
        public String environment;

        @Option(name = "--reuse-existing-environment", description = "Should reuse existing running docker containers")
        public boolean reuse;

        @Arguments(description = "cli arguments")
        public List<String> cliArguments = new ArrayList<>();

        public Module toModule()
        {
            return binder -> {
                binder.bind(PrestoCliOptions.class).toInstance(this);
            };
        }
    }

    public static class Execution
            implements Runnable
    {
        private static final int TESTS_READY_PORT = 1970;

        private final PathResolver pathResolver;
        private final boolean debug;
        private final File cliJar;
        private final List<String> cliArguments;
        private EnvironmentCommandExecution execution;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, PathResolver pathResolver, EnvironmentOptions environmentOptions, PrestoCliOptions prestoCliOptions)
        {
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            requireNonNull(environmentOptions, "environmentOptions is null");
            this.debug = environmentOptions.debug;
            this.cliJar = requireNonNull(prestoCliOptions.cliJar, "testOptions.cliJar is null");
            this.cliArguments = ImmutableList.copyOf(requireNonNull(prestoCliOptions.cliArguments, "prestoCliOptions.cliArguments is null"));
            execution = new EnvironmentCommandExecution(environmentFactory, prestoCliOptions.environment, prestoCliOptions.reuse, this::extendEnvironment, "cli");
        }

        @Override
        public void run()
        {
            execution.run();
        }

        private void extendEnvironment(Environment.Builder environment)
        {
            environment.configureContainer("cli", container -> {
                container.addExposedPort(ENVIRONMENT_READY_PORT);

                ImmutableList.Builder<String> jvmOptions = ImmutableList.builder();

                if (debug) {
                    jvmOptions.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5007");
                    exposePort(container, 5007); // debug port
                }

                container
                        .withFileSystemBind(pathResolver.resolvePlaceholders(cliJar).getPath(), "/docker/cli.jar", READ_ONLY)
                        .withCommand(ImmutableList.<String>builder()
                                .add("bash", "-xeuc", "nc -l \"$1\" < /dev/null; shift; exec \"$@\"", "-")
                                .add(Integer.toString(TESTS_READY_PORT))
                                .add("java")
                                .addAll(jvmOptions.build())
                                .addAll(cliArguments)
                                .build().toArray(new String[0]));
            });
        }
    }
}
