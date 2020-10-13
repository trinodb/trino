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

import com.github.dockerjava.api.command.InspectContainerResponse;
import io.airlift.log.Logger;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeExecutor;
import net.jodah.failsafe.Timeout;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public interface EnvironmentListener
{
    Logger log = Logger.get(EnvironmentListener.class);

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

    static void tryInvokeListener(FailsafeExecutor executor, Consumer<EnvironmentListener> call, EnvironmentListener... listeners)
    {
        Arrays.stream(listeners).forEach(listener -> {
            try {
                executor.runAsync(() -> call.accept(listener)).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException | RuntimeException e) {
                log.error("Could not invoke listener %s due to %s", listener.getClass().getSimpleName(), getStackTraceAsString(e));
            }
        });
    }

    static EnvironmentListener compose(EnvironmentListener... listeners)
    {
        return new EnvironmentListener()
        {
            private FailsafeExecutor executor = Failsafe
                    .with(Timeout.of(ofMinutes(5)).withCancel(true))
                    .with(newCachedThreadPool(daemonThreadsNamed("environment-listener-%d")));

            @Override
            public void environmentStarting(Environment environment)
            {
                tryInvokeListener(executor, listener -> listener.environmentStarting(environment), listeners);
            }

            @Override
            public void environmentStarted(Environment environment)
            {
                tryInvokeListener(executor, listener -> listener.environmentStarted(environment), listeners);
            }

            @Override
            public void environmentStopping(Environment environment)
            {
                tryInvokeListener(executor, listener -> listener.environmentStopping(environment), listeners);
            }

            @Override
            public void environmentStopped(Environment environment)
            {
                tryInvokeListener(executor, listener -> listener.environmentStopped(environment), listeners);
            }

            @Override
            public void containerStarting(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(executor, listener -> listener.containerStarting(container, response), listeners);
            }

            @Override
            public void containerStarted(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(executor, listener -> listener.containerStarted(container, response), listeners);
            }

            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(executor, listener -> listener.containerStopping(container, response), listeners);
            }

            @Override
            public void containerStopped(DockerContainer container, InspectContainerResponse response)
            {
                tryInvokeListener(executor, listener -> listener.containerStopped(container, response), listeners);
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
        return new EnvironmentListener()
        {
            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                container.copyLogsToHostPath(logBaseDir);
            }
        };
    }

    static EnvironmentListener statsPrintingListener()
    {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2, daemonThreadsNamed("container-stats-%d"));
        List<ScheduledFuture<?>> futures = new ArrayList<>();

        return new EnvironmentListener()
        {
            @Override
            public void containerStarting(DockerContainer container, InspectContainerResponse response)
            {
                // Print stats every 30 seconds
                futures.add(executorService.scheduleWithFixedDelay(() ->
                {
                    StatisticsFetcher.Stats stats = container.getStats();
                    if (stats.areCalculated()) {
                        log.info("%s - %s", container.getLogicalName(), container.getStats());
                    }
                }, 5 * 1000L, 30 * 1000L, MILLISECONDS));
            }

            @Override
            public void containerStopping(DockerContainer container, InspectContainerResponse response)
            {
                log.info("Container %s final statistics - %s", container, container.getStats());
            }

            @Override
            public void containerStarted(DockerContainer container, InspectContainerResponse containerInfo)
            {
                // Force fetching of stats so CPU usage can be calculated from delta
                container.getStats();
            }

            @Override
            public void environmentStopping(Environment environment)
            {
                futures.forEach(future -> future.cancel(true));
            }

            @Override
            public void environmentStopped(Environment environment)
            {
                executorService.shutdown();
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
