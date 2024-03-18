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
package io.trino.plugin.varada.storage.flows;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.stats.VaradaStatsFlowSequencer;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

@Singleton
public class FlowsSequencer
{
    public static final String STATS_GROUP_NAME = "flowsequencer";

    private final FlowPriorityQueue flowPriorityQueue;
    private final MetricsManager metricsManager;

    @Inject
    public FlowsSequencer(MetricsManager metricsManager)
    {
        this.metricsManager = requireNonNull(metricsManager);
        flowPriorityQueue = new FlowPriorityQueue();
        initMetrics();
    }

    private void initMetrics()
    {
        for (FlowType flowType : FlowType.values()) {
            metricsManager.registerMetric(new VaradaStatsFlowSequencer(STATS_GROUP_NAME, flowType.name()));
        }
    }

    public CompletableFuture<Boolean> tryRunningFlow(FlowType flowType, long flowId, Optional<String> additionalInfo)
    {
        VaradaStatsFlowSequencer stats = (VaradaStatsFlowSequencer) metricsManager.get(VaradaStatsFlowSequencer.createKey(STATS_GROUP_NAME, flowType.name()));
        stats.incflow_started();
        return flowPriorityQueue.addFlow(flowType, flowId, additionalInfo);
    }

    public void flowFinished(FlowType flowType, long flowId, boolean force)
    {
        flowPriorityQueue.removeFlow(flowType, flowId, force);
        VaradaStatsFlowSequencer stats = (VaradaStatsFlowSequencer) metricsManager.get(VaradaStatsFlowSequencer.createKey(STATS_GROUP_NAME, flowType.name()));
        stats.incflow_finished();
    }

    @VisibleForTesting
    public Map getRunningFlows()
    {
        return flowPriorityQueue.getRunningFlows();
    }

    @VisibleForTesting
    public Map getPendingFlows()
    {
        return flowPriorityQueue.getPendingFlows();
    }
}
