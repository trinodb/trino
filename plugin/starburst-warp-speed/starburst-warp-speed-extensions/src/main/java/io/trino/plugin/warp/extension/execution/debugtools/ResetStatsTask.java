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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.inject.Inject;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.metrics.VaradaStatsBase;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Map;
import java.util.regex.Pattern;

@TaskResourceMarker
@Path(ResetStatsTask.RESET_STAT)
public class ResetStatsTask
        implements TaskResource
{
    public static final String RESET_STAT = "reset-stats";
    private final MetricsManager metricsManager;

    @Inject
    public ResetStatsTask(MetricsManager metricsManager)
    {
        this.metricsManager = metricsManager;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void resetStats(ResetStatsData resetStatsData)
    {
        Map<String, VaradaStatsBase> all = metricsManager.getAll();
        Pattern pattern = Pattern.compile(resetStatsData.getStatsKey());
        all.entrySet().stream().filter((entry) -> pattern.matcher(entry.getKey()).matches()).forEach((entry) -> entry.getValue().reset());
    }
}
