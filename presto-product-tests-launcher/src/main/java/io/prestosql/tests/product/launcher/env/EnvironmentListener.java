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
package io.prestosql.tests.product.launcher.env;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.InspectContainerResponse;
import io.airlift.log.Logger;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public interface EnvironmentListener
{
    Logger log = Logger.get(EnvironmentListener.class);
    ObjectMapper mapper = new ObjectMapper();

    default void environmentStarting(Environment environment)
    {
    }

    default void environmentStarted(Environment environment)
    {
    }

    default void environmentStopped(Environment environment)
    {
    }

    default void environmentStopping(Environment environment)
    {
    }

    default void containerStarting(DockerContainer container, InspectContainerResponse response)
    {
    }

    default void containerStarted(DockerContainer container, InspectContainerResponse containerInfo)
    {
    }

    default void containerStopping(DockerContainer container, InspectContainerResponse response)
    {
    }

    default void containerStopped(DockerContainer container, InspectContainerResponse response)
    {
    }

    static void tryInvokeListener(Consumer<EnvironmentListener> call, EnvironmentListener... listeners)
    {
        Arrays.stream(listeners).forEach(listener -> {
            try {
                call.accept(listener);
            }
            catch (Exception e) {
                log.error("Could not invoke listener %s due to %s", listener.getClass().getSimpleName(), getStackTraceAsString(e));
            }
        });
    }

    static EnvironmentListener compose(EnvironmentListener... listeners)
    {
        return new EnvironmentListener()
        {
            @Override
            public void environmentStarting(Environment environment)
            {
                tryInvokeListener(listener -> listener.environmentStarting(environment), listeners);
            }

            @Override
            public void environmentStarted(Environment environment)
            {
                tryInvokeListener(listener -> listener.environmentStarted(environment), listeners);
            }

            @Override
            public void environmentStopping(Environment environment)
            {
                tryInvokeListener(listener -> listener.environmentStopping(environment), listeners);
            }

            @Override
            public void environmentStopped(Environment environment)
            {
                tryInvokeListener(listener -> listener.environmentStopped(environment), listeners);
            }

            @Override
            public void containerStarting(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(listener -> listener.containerStarting(container, response), listeners);
            }

            @Override
            public void containerStarted(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(listener -> listener.containerStarted(container, response), listeners);
            }

            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(listener -> listener.containerStopping(container, response), listeners);
            }

            @Override
            public void containerStopped(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(listener -> listener.containerStopped(container, response), listeners);
            }
        };
    }

    static EnvironmentListener loggingListener()
    {
        return new EnvironmentListener()
        {
            @Override
            public void environmentStarting(Environment environment)
            {
                log.info("Environment starting: %s", environment);
            }

            @Override
            public void environmentStarted(Environment environment)
            {
                log.info("Environment started: %s", environment);
            }

            @Override
            public void environmentStopping(Environment environment)
            {
                log.info("Environment stopping: %s", environment);
            }

            @Override
            public void environmentStopped(Environment environment)
            {
                log.info("Environment stopped: %s", environment);
            }

            @Override
            public void containerStarting(DockerContainer container, InspectContainerResponse response)
            {
                log.info("Container starting: %s", container);
            }

            @Override
            public void containerStarted(DockerContainer container, InspectContainerResponse response)
            {
                log.info("Container started: %s", container);
            }

            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                log.info("Container stopping: %s", container);
            }

            @Override
            public void containerStopped(DockerContainer container, InspectContainerResponse response)
            {
                log.info("Container stopped: %s", container);
            }
        };
    }

    static EnvironmentListener logCopyingListener(Path logBaseDir)
    {
        requireNonNull(logBaseDir, "logBaseDir is null");
        return new EnvironmentListener() {
            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                container.copyLogsToHostPath(logBaseDir);
            }
        };
    }

    static EnvironmentListener statsPrintingListener()
    {
        return new EnvironmentListener()
        {
            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                container.getStats().ifPresent(statistics -> {
                    try {
                        log.info("Container %s stats: %s", container, mapper.writeValueAsString(statistics));
                    }
                    catch (JsonProcessingException e) {
                        log.warn("Could not display container %s stats: %s", container, e);
                    }
                });
            }
        };
    }

    static EnvironmentListener getStandardListeners(Optional<Path> logsDirBase)
    {
        EnvironmentListener listener = compose(loggingListener(), statsPrintingListener());

        if (logsDirBase.isPresent()) {
            return compose(listener, logCopyingListener(logsDirBase.get()));
        }

        return listener;
    }
}
