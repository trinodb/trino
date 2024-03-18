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
package io.trino.plugin.warp.extension.execution.warmup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;

@TaskResourceMarker(coordinator = false)
@Singleton
//@Api(value = "Warmup", tags = WarmingResource.WARMING_SWAGGER_TAG)
@Path(WarmupRuleService.WARMUP_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerWarmupRulesChangedTask
        implements TaskResource
{
    public static final String TASK_NAME = "worker-warmup-rules-changed";

    private final WorkerWarmupRuleService workerWarmupRuleService;

    @Inject
    public WorkerWarmupRulesChangedTask(WorkerWarmupRuleService workerWarmupRuleService)
    {
        this.workerWarmupRuleService = requireNonNull(workerWarmupRuleService);
    }

    @POST
    @Path(TASK_NAME)
    @Audit
    //@ApiOperation(value = "rules changed", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public void rulesChanged()
    {
        workerWarmupRuleService.fetchRulesFromCoordinator();
    }
}
