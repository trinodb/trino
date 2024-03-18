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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.api.warmup.RuleResultDTO;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleRejectionData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupDefaultRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.varada.dispatcher.warmup.events.WarmRulesChangedEvent;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.varada.warmup.WarmupRuleApiMapper;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.varada.warmup.model.WarmupRuleResult;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.Node;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.plugin.varada.execution.VaradaClient.VOID_RESULTS_CODEC;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path(WarmupRuleService.WARMUP_PATH)
//@Api(value = "Warmup", tags = WarmingResource.WARMING_SWAGGER_TAG)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WarmupTask
        implements TaskResource
{
    private static final Logger logger = Logger.get(WarmupTask.class);

    public static final String TASK_NAME_SET = "warmup-rule-set";
    public static final String TASK_NAME_REPLACE = "warmup-rule-replace";
    public static final String TASK_NAME_FETCH = "run-fetcher";
    public static final String TASK_NAME_DELETE = "warmup-rule-delete";
    public static final String TASK_NAME_GET_USAGE = "warmup-rule-get-usage";
    public static final String TASK_NAME_VALIDATE = "warmup-rule-validate";

    private static final JsonCodec<WarmupRulesUsageData> USAGE_RESULT_CODEC = JsonCodec.jsonCodec(WarmupRulesUsageData.class);

    private final WarmupRuleService warmupRuleService;
    private final WarmupRuleFetcher warmupRuleFetcher;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final VaradaClient varadaClient;

    @Inject
    public WarmupTask(WarmupRuleService warmupRuleService,
            WarmupRuleFetcher warmupRuleFetcher,
            CoordinatorNodeManager coordinatorNodeManager,
            VaradaClient varadaClient,
            EventBus eventBus)
    {
        this.warmupRuleService = requireNonNull(warmupRuleService);
        this.warmupRuleFetcher = requireNonNull(warmupRuleFetcher);
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.varadaClient = requireNonNull(varadaClient);
        requireNonNull(eventBus).register(this);
    }

    @Path(WarmupRuleService.TASK_NAME_GET)
    @GET
    @Audit
    public List<WarmupColRuleData> warmupRuleGet()
    {
        return warmupRuleService.getAll()
                .stream()
                .map(WarmupRuleApiMapper::fromModel)
                .collect(Collectors.toList());
    }

    @Path(TASK_NAME_SET)
    @POST
    @Audit
    public RuleResultDTO save(@JsonProperty("warmupColRuleDataList") List<WarmupColRuleData> warmupColRuleDataList)
    {
        List<WarmupRule> collect = warmupColRuleDataList.stream()
                .map(WarmupRuleApiMapper::toModel)
                .collect(Collectors.toList());
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(collect);
        ImmutableList<WarmupColRuleData> appliedRules = warmupRuleResult.appliedRules().stream().map(WarmupRuleApiMapper::fromModel).collect(toImmutableList());
        ImmutableList<WarmupColRuleRejectionData> rejectedRules = warmupRuleResult.rejectedRules().entrySet().stream()
                .map(e -> new WarmupColRuleRejectionData(WarmupRuleApiMapper.fromModel(e.getKey()), ImmutableSet.copyOf(e.getValue()))).collect(toImmutableList());
        return new RuleResultDTO(appliedRules, rejectedRules);
    }

    @Path(TASK_NAME_REPLACE)
    @POST
    @Audit
    public RuleResultDTO replace(@JsonProperty("warmupColRuleDataList") List<WarmupColRuleData> warmupColRuleDataList)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.replaceAll(warmupColRuleDataList.stream().map(WarmupRuleApiMapper::toModel).collect(Collectors.toList()));
        ImmutableList<WarmupColRuleData> appliedRules = warmupRuleResult.appliedRules().stream().map(WarmupRuleApiMapper::fromModel).collect(toImmutableList());
        ImmutableList<WarmupColRuleRejectionData> rejectedRules = warmupRuleResult.rejectedRules().entrySet().stream()
                .map(e -> new WarmupColRuleRejectionData(WarmupRuleApiMapper.fromModel(e.getKey()), ImmutableSet.copyOf(e.getValue()))).collect(toImmutableList());
        return new RuleResultDTO(appliedRules, rejectedRules);
    }

    @Path(TASK_NAME_FETCH)
    @GET
    @Audit
    public void fetch()
    {
        warmupRuleFetcher.getWarmupRules();
    }

    @Path(TASK_NAME_DELETE)
    @Consumes(MediaType.APPLICATION_JSON)
    @DELETE
    @Audit
    public void delete(@JsonProperty("ids") List<Integer> ids)
    {
        warmupRuleService.delete(ids);
    }

    @SuppressWarnings({"UnstableApiUsage", "deprecation"})
    @Path(TASK_NAME_GET_USAGE)
    @GET
    @Audit
    public WarmupRulesUsageData getWithUsage()
    {
        Map<Integer, WarmupColRuleUsageData.Builder> hashToWarmupColRuleUsageDataMapBuilder = new ConcurrentHashMap<>();
        Map<WarmUpType, WarmupDefaultRuleUsageData> warmupTypeToWarmupDefaultRuleUsageDataMap = new ConcurrentHashMap<>();
        List<Node> workers = coordinatorNodeManager.getWorkerNodes();
        workers.stream()
                .parallel()
                .map(this::getWorkersWarmupRulesUsageData)
                .forEach(warmupRulesUsageData -> {
                    updateWarmupColRuleUsageData(warmupRulesUsageData, hashToWarmupColRuleUsageDataMapBuilder);
                    updateWarmupDefaultRuleUsageData(warmupRulesUsageData, warmupTypeToWarmupDefaultRuleUsageDataMap);
                });

        ImmutableCollection<WarmupColRuleUsageData> colRulesUsage = hashToWarmupColRuleUsageDataMapBuilder.values().stream().map(WarmupColRuleUsageData.Builder::build).collect(toImmutableList());
        return new WarmupRulesUsageData(colRulesUsage, warmupTypeToWarmupDefaultRuleUsageDataMap.values().stream().collect(toImmutableList()));
    }

    private WarmupRulesUsageData getWorkersWarmupRulesUsageData(Node node)
    {
        HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(node));
        uriBuilder.appendPath(WorkerWarmupTask.WORKER_WARMUP_PATH).appendPath(WorkerWarmupTask.TASK_NAME_GET);

        Request request = prepareGet()
                .setUri(uriBuilder.build())
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return varadaClient.sendWithRetry(request, createFullJsonResponseHandler(USAGE_RESULT_CODEC));
    }

    private void updateWarmupColRuleUsageData(WarmupRulesUsageData warmupRulesUsageData, Map<Integer, WarmupColRuleUsageData.Builder> hashToWarmupColRuleUsageDataMapBuilder)
    {
        warmupRulesUsageData.warmupColRuleUsageDataList()
                .forEach(warmupColRuleUsageData -> hashToWarmupColRuleUsageDataMapBuilder.compute(
                        warmupColRuleDataHashWithoutUsage(warmupColRuleUsageData),
                        (key, oldValue) -> {
                            if (Objects.isNull(oldValue)) {
                                return WarmupColRuleUsageData.builder(warmupColRuleUsageData);
                            }
                            return oldValue.increaseStorageKB(warmupColRuleUsageData.getUsedStorageKB());
                        }));
    }

    private void updateWarmupDefaultRuleUsageData(WarmupRulesUsageData warmupRulesUsageData, Map<WarmUpType, WarmupDefaultRuleUsageData> warmupTypeToWarmupDefaultRuleUsageDataMap)
    {
        warmupRulesUsageData.warmupDefaultRuleUsageDataList()
                .forEach(warmupDefaultRuleUsageData -> warmupTypeToWarmupDefaultRuleUsageDataMap.compute(
                        warmupDefaultRuleUsageData.warmUpType(),
                        (key, oldValue) -> {
                            if (Objects.isNull(oldValue)) {
                                return warmupDefaultRuleUsageData;
                            }
                            return new WarmupDefaultRuleUsageData(oldValue.usedStorageKB() + warmupDefaultRuleUsageData.usedStorageKB(),
                                    warmupDefaultRuleUsageData.warmUpType());
                        }));
    }

    @Path(TASK_NAME_VALIDATE)
    @POST
    //@ApiOperation(value = "validate", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public List<WarmupColRuleRejectionData> validate(@JsonProperty("warmupColRuleDataList") List<WarmupColRuleData> warmupColRuleDataList)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.validate(Collections.emptyList(),
                warmupColRuleDataList.stream().map(WarmupRuleApiMapper::toModel).collect(Collectors.toList()));
        return warmupRuleResult.rejectedRules()
                .entrySet()
                .stream()
                .map(e -> new WarmupColRuleRejectionData(WarmupRuleApiMapper.fromModel(e.getKey()), ImmutableSet.copyOf(e.getValue())))
                .collect(Collectors.toList());
    }

    private int warmupColRuleDataHashWithoutUsage(WarmupColRuleData warmupColRuleData)
    {
        return Objects.hash(warmupColRuleData.getSchema(),
                warmupColRuleData.getTable(),
                warmupColRuleData.getColumn(),
                warmupColRuleData.getPredicates(),
                warmupColRuleData.getWarmUpType(),
                warmupColRuleData.getPriority(),
                warmupColRuleData.getTtl());
    }

    @SuppressWarnings("unused")
    @Subscribe
    private void handleWarmRulesChangedEvent(WarmRulesChangedEvent warmRulesChangedEvent)
    {
        coordinatorNodeManager.getWorkerNodes().forEach(workerNode -> {
            HttpUriBuilder uriBuilder = varadaClient.getRestEndpoint(UriUtils.getHttpUri(workerNode));
            uriBuilder.appendPath(WarmupRuleService.WARMUP_PATH).appendPath(WorkerWarmupRulesChangedTask.TASK_NAME);
            Request request = preparePost()
                    .setUri(uriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .build();
            try {
                varadaClient.sendWithRetry(request, createFullJsonResponseHandler(VOID_RESULTS_CODEC));
            }
            catch (Exception e) {
                logger.warn("failed sending rule change notification to node %s", workerNode.getHost());
            }
        });
    }
}
