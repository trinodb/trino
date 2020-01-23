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
import io.airlift.airline.Command;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentModule;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.Environments;
import io.prestosql.tests.product.launcher.env.SelectedEnvironmentProvider;
import org.testcontainers.DockerClientFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.testcontainers.TestcontainersUtil.killContainersReaperContainer;
import static java.util.Objects.requireNonNull;

@Command(name = "up", description = "start an environment")
public final class EnvironmentUp
        implements Runnable
{
    private static final Logger log = Logger.get(EnvironmentUp.class);

    @Inject
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    private Module additionalEnvironments;

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
                        .add(new EnvironmentModule(additionalEnvironments))
                        .add(environmentOptions.toModule())
                        .build(),
                EnvironmentUp.Execution.class);
    }

    public static class Execution
            implements Runnable
    {
        private final SelectedEnvironmentProvider selectedEnvironmentProvider;

        @Inject
        public Execution(SelectedEnvironmentProvider selectedEnvironmentProvider)
        {
            this.selectedEnvironmentProvider = requireNonNull(selectedEnvironmentProvider, "selectedEnvironmentProvider is null");
        }

        @Override
        public void run()
        {
            log.info("Pruning old environment(s)");
            Environments.pruneEnvironment();

            Environment environment = selectedEnvironmentProvider.getEnvironment()
                    .removeContainer("tests")
                    .build();

            log.info("Starting the environment '%s'", selectedEnvironmentProvider.getEnvironmentName());
            environment.start();
            log.info("Environment '%s' started", selectedEnvironmentProvider.getEnvironmentName());

            try (DockerClient dockerClient = DockerClientFactory.lazyClient()) {
                log.info("Killing the testcontainers reaper container (Ryuk) so that environment can stay alive");
                killContainersReaperContainer(dockerClient);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
