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
package io.trino.tests.product.launcher.env;

import com.github.dockerjava.api.command.InspectContainerResponse;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Timeout;
import io.airlift.log.Logger;
import io.trino.tests.product.launcher.util.ConsoleTable;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.tests.product.launcher.env.StatisticsFetcher.Stats.HEADER;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public interface EnvironmentListener
        extends ContainerListener
{
    Logger log = Logger.get(EnvironmentListener.class);

    EnvironmentListener NOOP = new EnvironmentListener()
    {
    };

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

    static void tryInvokeListener(FailsafeExecutor<?> executor, Consumer<EnvironmentListener> call, EnvironmentListener... listeners)
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
                log.error(e, "Could not invoke listener %s", listener.getClass().getSimpleName());
            }
        });
    }

    static EnvironmentListener compose(EnvironmentListener... listeners)
    {
        return new EnvironmentListener()
        {
            private FailsafeExecutor<?> executor = Failsafe
                    .with(Timeout.builder(ofMinutes(5)).withInterrupt().build())
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
        Map<String, StatisticsFetcher> fetchers = new TreeMap<>(); // keep map sorted

        return new EnvironmentListener()
        {
            @Override
            public void containerStarted(DockerContainer container, InspectContainerResponse response)
            {
                // Start listening on statistics stream
                fetcher(container).start();
            }

            @Override
            public void environmentStopping(Environment environment)
            {
                fetchers.values().forEach(StatisticsFetcher::close);
                executorService.shutdownNow();
                printContainerStats();
            }

            @Override
            public void environmentStarted(Environment environment)
            {
                // Print stats for all containers every 30s after environment is started
                executorService.scheduleWithFixedDelay(() ->
                {
                    printContainerStats();
                }, 5 * 1000L, 30 * 1000L, MILLISECONDS);
            }

            @Override
            public void environmentStopped(Environment environment)
            {
                executorService.shutdown();
            }

            private StatisticsFetcher fetcher(DockerContainer container)
            {
                return fetchers.computeIfAbsent(container.getLogicalName(), key -> StatisticsFetcher.create(container));
            }

            private void printContainerStats()
            {
                ConsoleTable statistics = new ConsoleTable();
                statistics.addHeader(HEADER.toArray());
                fetchers.entrySet().forEach(entry -> statistics.addRow(entry.getValue().get().toRow(entry.getKey())));
                statistics.addSeparator();

                try {
                    log.info("Container stats:\n%s", statistics.render());
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
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
