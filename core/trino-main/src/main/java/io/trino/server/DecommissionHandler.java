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
package io.trino.server;

import io.airlift.log.Logger;
import io.trino.execution.TaskManager;
import io.trino.metadata.NodeState;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DecommissionHandler
{
    private static final Logger log = Logger.get(DecommissionHandler.class);

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(
            threadsNamed("decommission-handler-%s"));
    private final TaskManager sqlTaskManager;
    private final boolean isCoordinator;

    @Inject
    public DecommissionHandler(TaskManager sqlTaskManager, ServerConfig serverConfig)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.isCoordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    public synchronized void requestDecommission(ServerInfoResource sir)
    {
        log.info("enter requestDecommission " + sir.getServerState());
        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot decommission coordinator");
        }

        // The decommission is normally initiated by the coordinator.
        // Here we wait a short grace period of 10 seconds for coordinator to no longer
        // assign new tasks to this worker node, before wait active tasks to finish.
        executor.schedule(new Runnable() {
            @Override
            public void run()
            {
                GracefulShutdownHandler.waitActiveTasksToFinish(sqlTaskManager);
                log.info("complete waitActiveTasksToFinish " + sir.getServerState());
                NodeState state = sir.onDecommissioned();
                log.info("onDecommissioned " + state);
            }
        }, 10000, MILLISECONDS);
    }
}
