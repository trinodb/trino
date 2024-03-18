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
package io.trino.plugin.warp.extension.execution.health;

import com.google.inject.Inject;
import io.trino.plugin.varada.WorkerNodeManager;
import io.trino.plugin.varada.api.health.HealthResult;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.lang.management.ManagementFactory;

import static io.trino.plugin.warp.extension.execution.health.HealthTask.HEALTH_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false, shouldCheckExecutionAllowed = false)
//@Api(value = "Node Health", tags = HEALTH_TAG)
@Path(HEALTH_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class HealthTask
        implements TaskResource
{
    public static final String HEALTH_TAG = "Health";
    public static final String HEALTH_PATH = "health";
    public static final String TASK_NAME = "node-health";

    private static final long MEGABYTE = 1024L * 1024L;

    private final WorkerNodeManager workerNodeManager;
    private final WorkerCapacityManager workerCapacityManager;
    private final StorageEngineConstants storageEngineConstants;

    @Inject
    public HealthTask(WorkerNodeManager workerNodeManager,
            WorkerCapacityManager workerCapacityManager,
            StorageEngineConstants storageEngineConstants)
    {
        this.workerNodeManager = requireNonNull(workerNodeManager);
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
    }

    @GET
    @Path(TASK_NAME)
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "node health", nickname = "getNodeHealth", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "PRODUCTION"))})
    public HealthResult get()
    {
        return new HealthResult(workerNodeManager.isWorkerReady(),
                workerNodeManager.getCurrentNodeHttpUri(),
                ManagementFactory.getRuntimeMXBean().getStartTime(),
                (workerCapacityManager.getTotalCapacity() * storageEngineConstants.getPageSize()) / MEGABYTE);
    }
}
