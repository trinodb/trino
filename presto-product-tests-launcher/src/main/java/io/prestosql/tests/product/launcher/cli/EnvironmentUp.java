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
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentConfig;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.Environments;
import io.prestosql.tests.product.launcher.env.common.Standard;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import picocli.CommandLine.Command;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Mixin;
import static picocli.CommandLine.Option;

@Command(
        name = "up",
        description = "Start an environment",
        usageHelpAutoWidth = true)
public final class EnvironmentUp
        implements Runnable
{
    private static final Logger log = Logger.get(EnvironmentUp.class);
    private static final String LOGS_DIR = "logs/";

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Mixin
    public EnvironmentUpOptions environmentUpOptions = new EnvironmentUpOptions();

    private final Module additionalEnvironments;

    public EnvironmentUp(Extensions extensions)
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
                        .add(environmentUpOptions.toModule())
                        .build(),
                EnvironmentUp.Execution.class);
    }

    public static class EnvironmentUpOptions
    {
        @Option(names = "--background", description = "Keep containers running in the background once they are started")
        public boolean background;

        @Option(names = "--environment", paramLabel = "<environment>", description = "Name of the environment to start", required = true)
        public String environment;

        @Option(names = "--logs-dir", paramLabel = "<dir>", description = "Location of the exported logs directory", converter = OptionalPathConverter.class, defaultValue = "")
        public Optional<Path> logsDirBase;

        public Module toModule()
        {
            return binder -> binder.bind(EnvironmentUpOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Runnable
    {
        private final EnvironmentFactory environmentFactory;
        private final boolean withoutPrestoMaster;
        private final boolean background;
        private final String environment;
        private final boolean debug;
        private final EnvironmentConfig environmentConfig;
        private final Optional<Path> logsDirBase;

        @Inject
        public Execution(EnvironmentFactory environmentFactory, EnvironmentConfig environmentConfig, EnvironmentOptions options, EnvironmentUpOptions environmentUpOptions)
        {
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.environmentConfig = requireNonNull(environmentConfig, "environmentConfig is null");
            this.withoutPrestoMaster = options.withoutPrestoMaster;
            this.background = environmentUpOptions.background;
            this.environment = environmentUpOptions.environment;
            this.debug = options.debug;
            this.logsDirBase = requireNonNull(environmentUpOptions.logsDirBase, "environmentUpOptions.logsDirBase is null");
        }

        @Override
        public void run()
        {
            log.info("Pruning old environment(s)");
            Environments.pruneEnvironment();

            Environment.Builder builder = environmentFactory.get(environment)
                    .removeContainer("tests");

            if (withoutPrestoMaster) {
                builder.removeContainer("presto-master");
            }

            log.info("Creating environment '%s' with configuration %s", environment, environmentConfig);
            if (debug) {
                builder.configureContainers(Standard::enablePrestoJavaDebugger);
            }

            Optional<Path> environmentLogPath = logsDirBase.map(dir -> dir.resolve(environment));
            environmentLogPath.ifPresent(builder::exposeLogsInHostPath);

            Environment environment = builder.build();
            log.info("Starting the environment '%s'", this.environment);
            environment.start();
            log.info("Environment '%s' started", this.environment);

            if (background) {
                killContainersReaperContainer();
                return;
            }

            wait(environment.getContainers());
            log.info("Exiting, the containers will exit too");
        }

        private void killContainersReaperContainer()
        {
            try (DockerClient dockerClient = DockerClientFactory.lazyClient()) {
                log.info("Killing the testcontainers reaper container (Ryuk) so that environment can stay alive");
                ContainerUtil.killContainersReaperContainer(dockerClient);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void wait(Collection<Container<?>> containers)
        {
            try {
                while (containers.stream().anyMatch(ContainerState::isRunning)) {
                    Thread.sleep(1_000);
                }
                throw new RuntimeException("All containers have been stopped");
            }
            catch (InterruptedException e) {
                log.info("Interrupted");
                // It's OK not to restore interrupt flag here. When we return we're exiting the process.
            }
        }
    }
}
