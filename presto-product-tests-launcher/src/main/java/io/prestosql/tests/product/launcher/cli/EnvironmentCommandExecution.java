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
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentFactory;
import io.prestosql.tests.product.launcher.env.Environments;
import io.prestosql.tests.product.launcher.env.common.EnvironmentExtender;
import org.testcontainers.containers.Container;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EnvironmentCommandExecution
{
    private static final Logger log = Logger.get(EnvironmentCommandExecution.class);

    public static final int ENVIRONMENT_READY_PORT = 1970;

    private final EnvironmentFactory environmentFactory;
    private final String environment;
    private final boolean reuse;
    private final EnvironmentExtender environmentExtender;
    private final String commandContainerName;

    @Inject
    public EnvironmentCommandExecution(
            EnvironmentFactory environmentFactory,
            String environment,
            boolean reuse,
            EnvironmentExtender environmentExtender,
            String commandContainerName)
    {
        this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.reuse = reuse;
        this.environmentExtender = requireNonNull(environmentExtender, "environmentExtender is null");
        this.commandContainerName = requireNonNull(commandContainerName, "commandContainerName is null");
    }

    public void run()
    {
        if (reuse) {
            log.info("Trying to reuse the environment '%s'", environment);
            Environment environment = getEnvironment();
            environment.reuse();
            environment.start();
            run(environment);
            return;
        }

        try (UncheckedCloseable ignore = this::cleanUp) {
            log.info("Pruning old environment(s)");
            Environments.pruneEnvironment();

            Environment environment = getEnvironment();

            log.info("Starting the environment '%s'", environment);
            environment.start();
            log.info("Environment '%s' started", environment);

            run(environment);
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
        environmentExtender.extendEnvironment(environment);
        return environment.build();
    }

    private void run(Environment environment)
    {
        log.info("Starting execution");
        Container<?> container = environment.getContainer(commandContainerName);
        try {
            // Release waiter to let the tests run
            new Socket(container.getContainerIpAddress(), container.getMappedPort(ENVIRONMENT_READY_PORT)).close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        log.info("Waiting for command completion");
        try {
            while (container.isRunning()) {
                Thread.sleep(1000);
            }

            InspectContainerResponse containerInfo = container.getCurrentContainerInfo();
            InspectContainerResponse.ContainerState containerState = containerInfo.getState();
            Integer exitCode = containerState.getExitCode();
            log.info("Command container %s is %s, with exitCode %s", containerInfo.getId(), containerState.getStatus(), exitCode);
            checkState(exitCode != null, "No exitCode for command container %s in state %s", container, containerState);
            if (exitCode != 0) {
                throw new RuntimeException("Command exited with " + exitCode);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }

    private interface UncheckedCloseable
            extends AutoCloseable
    {
        @Override
        void close();
    }
}
