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
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
@Path(WarmingResource.WARMING)
//@Api(value = "Worker Warming", tags = WarmingResource.WARMING_SWAGGER_TAG)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@TaskResourceMarker
public class WorkerWarmingResource
        implements TaskResource
{
    public static final String STATUS = "worker-status";
    private final MetricsManager metricsManager;

    @Inject
    public WorkerWarmingResource(MetricsManager metricsManager)
    {
        this.metricsManager = requireNonNull(metricsManager);
    }

    @GET
    @Path(STATUS)
    //@ApiOperation(value = "worker warming status", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public WorkerWarmingStatusData getStatus()
    {
        VaradaStatsWarmingService varadaStatsWarmingService = (VaradaStatsWarmingService) metricsManager.get(VaradaStatsWarmingService.createKey(WARMING_SERVICE_STAT_GROUP));
        return new WorkerWarmingStatusData(varadaStatsWarmingService.getwarm_started(), varadaStatsWarmingService.getwarm_accomplished());
    }
}
