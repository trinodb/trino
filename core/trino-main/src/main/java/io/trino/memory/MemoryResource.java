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
package io.trino.memory;

import com.google.common.collect.ListMultimap;
import io.trino.TaskMemoryInfo;
import io.trino.execution.SqlTask;
import io.trino.execution.SqlTaskManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static java.util.Objects.requireNonNull;

/**
 * Manages memory pools on this worker node
 */
@Path("/v1/memory")
public class MemoryResource
{
    private final LocalMemoryManager memoryManager;
    private final SqlTaskManager taskManager;

    @Inject
    public MemoryResource(LocalMemoryManager memoryManager, SqlTaskManager taskManager)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
    }

    @ResourceSecurity(INTERNAL_ONLY)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MemoryInfo getMemoryInfo()
    {
        return memoryManager.getInfo().withTasksMemoryInfo(buildTasksMemoryInfo());
    }

    private ListMultimap<QueryId, TaskMemoryInfo> buildTasksMemoryInfo()
    {
        List<SqlTask> tasks = taskManager.getAllTasks();
        return tasks.stream()
                .filter(task -> !task.getTaskState().isDone())
                // we are providing task memory information only for queries which are run with task-level retries.
                // task memory information is consumed by low memory killer and it does not make sense to kill individual tasks
                // for queries which does not allow task retries.
                .filter(task -> task.getTaskContext().map(context -> getRetryPolicy(context.getSession()) == TASK).orElse(false))
                .collect(toImmutableListMultimap(
                        task -> task.getTaskId().getQueryId(),
                        task -> new TaskMemoryInfo(
                                task.getTaskId(),
                                task.getTaskContext()
                                        .map(taskContext -> taskContext.getMemoryReservation().toBytes())
                                        // task context may no longer be available if task completes
                                        .orElse(0L))));
    }

    private Response toSuccessfulResponse(MemoryPoolInfo memoryInfo)
    {
        return Response.ok()
                .entity(memoryInfo)
                .build();
    }
}
