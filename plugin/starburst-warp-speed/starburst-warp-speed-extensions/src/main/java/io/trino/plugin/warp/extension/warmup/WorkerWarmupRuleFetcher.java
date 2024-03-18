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
package io.trino.plugin.warp.extension.warmup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.varada.WorkerNodeManager;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.warmup.WarmupRuleApiMapper;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.plugin.varada.dispatcher.warmup.WorkerWarmingService.WARMING_SERVICE_STAT_GROUP;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerWarmupRuleFetcher
        implements WarmupRuleFetcher
{
    private static final Logger logger = Logger.get(WorkerWarmupRuleFetcher.class);

    private static final JsonCodec<List<WarmupColRuleData>> WARMUP_RULES_CODEC = JsonCodec.listJsonCodec(WarmupColRuleData.class);

    private final WorkerNodeManager workerNodeManager;
    private final VaradaClient varadaClient;
    private final MetricsManager metricsManager;

    @Inject
    public WorkerWarmupRuleFetcher(
            WorkerNodeManager workerNodeManager,
            VaradaClient varadaClient,
            MetricsManager metricsManager)
    {
        this.workerNodeManager = requireNonNull(workerNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
        this.metricsManager = requireNonNull(metricsManager);
    }

    @Override
    public List<WarmupRule> getWarmupRules(boolean force)
    {
        List<WarmupColRuleData> res = List.of();
        try {
            HttpUriBuilder restEndpointBuilder = varadaClient.getRestEndpoint(workerNodeManager.getCoordinatorNodeHttpUri());
            URI uri = restEndpointBuilder.appendPath(WarmupRuleService.WARMUP_PATH).appendPath(WarmupRuleService.TASK_NAME_GET).build();
            Request request = prepareGet()
                    .setUri(uri)
                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                    .build();
            res = varadaClient.sendWithRetry(request, createFullJsonResponseHandler(WARMUP_RULES_CODEC));
        }
        catch (Exception e) {
            VaradaStatsWarmingService warmingStats = (VaradaStatsWarmingService) metricsManager.get(VaradaStatsWarmingService.createKey(WARMING_SERVICE_STAT_GROUP));
            warmingStats.incfailed_fetching_rules();
            warmingStats.incwarm_skipped_due_key_conflict(); // HACK HACK HACk until test will support the previous counter
            logger.warn("failed getting rules from coordinator");
        }
        return res.stream().map(WarmupRuleApiMapper::toModel).collect(Collectors.toList());
    }

    @Override
    public List<WarmupRule> getWarmupRules()
    {
        return getWarmupRules(true);
    }
}
