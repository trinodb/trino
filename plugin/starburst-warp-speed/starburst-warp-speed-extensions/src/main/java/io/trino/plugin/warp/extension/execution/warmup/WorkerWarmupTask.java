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

import com.google.common.collect.ImmutableCollection;
import com.google.inject.Inject;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupDefaultRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.warmup.WarmupRuleApiMapper;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.spi.connector.SchemaTableName;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.warp.extension.execution.warmup.WorkerWarmupTask.WORKER_WARMUP_PATH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

@TaskResourceMarker(coordinator = false)
@Path(WORKER_WARMUP_PATH)
//@Api(value = "Worker Warmup", tags = WarmingResource.WARMING_SWAGGER_TAG)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerWarmupTask
        implements TaskResource
{
    public static final String WORKER_WARMUP_PATH = "worker-warmup";
    public static final String TASK_NAME_GET = "worker-warmup-rule-get-usage";
    public static final long KILOBYTE = 1024L;
    public static final long MEGABYTE = KILOBYTE * 1024L;

    private final RowGroupDataService rowGroupDataService;
    private final WarmupDemoterService warmupDemoterService;
    private final WorkerWarmupRuleService workerWarmupRuleService;

    @Inject
    public WorkerWarmupTask(RowGroupDataService rowGroupDataService,
            WarmupDemoterService warmupDemoterService,
            WorkerWarmupRuleService workerWarmupRuleService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.workerWarmupRuleService = requireNonNull(workerWarmupRuleService);
    }

    @Path(TASK_NAME_GET)
    @GET
    //@ApiOperation(value = "get", nickname = "workerWarmupGet", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public WarmupRulesUsageData get()
    {
        List<WarmupRule> warmupRules = workerWarmupRuleService.fetchRulesFromCoordinator();
        Map<Integer, AtomicLong> warmupIdUsageMap = new HashMap<>();
        Map<WarmUpType, AtomicLong> defaultRulesMap = new HashMap<>();

        getRowGroupDataUsage(warmupRules, warmupIdUsageMap, defaultRulesMap);

        ImmutableCollection<WarmupColRuleUsageData> colRuleUsageDataList = warmupRules.stream()
                .map(warmupRule -> {
                    WarmupColRuleData data = WarmupRuleApiMapper.fromModel(warmupRule);
                    return new WarmupColRuleUsageData(warmupRule.getId(),
                            data.getSchema(),
                            data.getTable(),
                            data.getColumn(),
                            data.getWarmUpType(),
                            data.getPriority(),
                            data.getTtl(),
                            data.getPredicates(),
                            warmupIdUsageMap.getOrDefault(warmupRule.getId(), new AtomicLong(0)).get() / KILOBYTE);
                })
                .collect(toImmutableList());
        ImmutableCollection<WarmupDefaultRuleUsageData> defaultRuleUsageDataList = defaultRulesMap.entrySet()
                .stream()
                .map(e -> new WarmupDefaultRuleUsageData(e.getValue().get() / KILOBYTE, e.getKey()))
                .collect(toImmutableList());
        return new WarmupRulesUsageData(colRuleUsageDataList, defaultRuleUsageDataList);
    }

    private void getRowGroupDataUsage(List<WarmupRule> warmupRules, Map<Integer, AtomicLong> warmupIdUsageMap, Map<WarmUpType, AtomicLong> defaultRulesMap)
    {
        List<RowGroupData> rowGroupDataList = rowGroupDataService.getAll();
        Map<SchemaTableColumn, List<WarmupRule>> schemaTableColumnToRulesMap = warmupRules.stream()
                .collect(groupingBy(warmupRule -> new SchemaTableColumn(new SchemaTableName(warmupRule.getSchema(), warmupRule.getTable()), warmupRule.getVaradaColumn())));
        for (RowGroupData rowGroupData : rowGroupDataList) {
            for (WarmUpElement warmUpElement : rowGroupData.getWarmUpElements()) {
                SchemaTableColumn schemaTableColumn = new SchemaTableColumn(
                        new SchemaTableName(rowGroupData.getRowGroupKey().schema(), rowGroupData.getRowGroupKey().table()), warmUpElement.getVaradaColumn());

                Optional<WarmupRule> optionalWarmupRule = warmupDemoterService.findMostRelevantRuleForWarmupElement(rowGroupData,
                        warmUpElement, schemaTableColumnToRulesMap.get(schemaTableColumn));
                long sizeInBytes = 0;
                optionalWarmupRule.ifPresentOrElse(warmupRule -> {
                    AtomicLong currentUsage = warmupIdUsageMap.computeIfAbsent(warmupRule.getId(), key -> new AtomicLong(0));
                    currentUsage.getAndAdd(sizeInBytes);
                }, () -> {
                    AtomicLong currentUsage = defaultRulesMap.computeIfAbsent(WarmupRuleApiMapper.convertModelWarmUpType(warmUpElement.getWarmUpType()), key -> new AtomicLong(0));
                    currentUsage.getAndAdd(sizeInBytes);
                });
            }
        }
    }
}
