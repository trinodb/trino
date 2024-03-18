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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleRejectionData;
import io.trino.plugin.varada.api.warmup.WarmupColRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupDefaultRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.NodeUtils;
import io.trino.plugin.varada.warmup.WarmupRuleApiMapper;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.varada.warmup.model.WarmupRuleResult;
import io.trino.spi.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmupTaskTest
{
    private WarmupRuleService warmupRuleService;
    private WarmupRuleFetcher warmupRuleFetcher;
    private CoordinatorNodeManager coordinatorNodeManager;
    private VaradaClient varadaClient;
    private WarmupTask task;

    @BeforeEach
    public void before()
    {
        warmupRuleService = mock(WarmupRuleService.class);
        warmupRuleFetcher = mock(WarmupRuleFetcher.class);
        coordinatorNodeManager = mock(CoordinatorNodeManager.class);
        varadaClient = mock(VaradaClient.class);

        task = new WarmupTask(warmupRuleService,
                warmupRuleFetcher,
                coordinatorNodeManager,
                varadaClient,
                mock(EventBus.class));
    }

    @Test
    public void testSaveGet()
    {
        assertThat(task.warmupRuleGet()).isEmpty();

        WarmupRule warmupRule = WarmupRule.builder()
                .varadaColumn(new RegularColumn("col1"))
                .schema("schema")
                .table("table")
                .warmUpType(io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE)
                .priority(5)
                .ttl(10)
                .predicates(Set.of(new io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule("col2", "2")))
                .build();

        when(warmupRuleService.getAll()).thenReturn(List.of(warmupRule));
        when(warmupRuleService.save(anyList())).thenReturn(new WarmupRuleResult(Collections.singletonList(warmupRule), Collections.emptyMap()));
        WarmupColRuleData warmupColRuleData = WarmupRuleApiMapper.fromModel(warmupRule);

        task.save(List.of(warmupColRuleData));

        assertThat(task.warmupRuleGet()).containsExactly(warmupColRuleData);
    }

    @Test
    public void testGetWithUsage()
    {
        assertThat(task.getWithUsage().warmupColRuleUsageDataList()).isEmpty();

        List<WarmupRule> warmupRules = List.of(WarmupRule.builder()
                .varadaColumn(new RegularColumn("col1"))
                .schema("schema")
                .table("table")
                .warmUpType(io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE)
                .priority(5)
                .ttl(10)
                .predicates(Set.of(new io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule("col2", "2")))
                .build());

        List<Node> workers = IntStream.range(1, 10)
                .mapToObj(i -> NodeUtils.node(String.valueOf(i), true))
                .collect(Collectors.toList());
        when(coordinatorNodeManager.getWorkerNodes()).thenReturn(workers);

        ImmutableList<WarmupColRuleUsageData> workerWarmupColRuleDataList = warmupRules.stream()
                .map(WarmupRuleApiMapper::fromModel)
                .map(warmupColRuleData -> new WarmupColRuleUsageData(warmupColRuleData.getId(),
                        warmupColRuleData.getSchema(),
                        warmupColRuleData.getTable(),
                        warmupColRuleData.getColumn(),
                        warmupColRuleData.getWarmUpType(),
                        warmupColRuleData.getPriority(),
                        warmupColRuleData.getTtl(),
                        warmupColRuleData.getPredicates(),
                        10 * WorkerWarmupTask.MEGABYTE))
                .collect(toImmutableList());
        ImmutableList<WarmupDefaultRuleUsageData> defaultRuleUsageData = ImmutableList.of(new WarmupDefaultRuleUsageData(8 * WorkerWarmupTask.MEGABYTE, WarmUpType.WARM_UP_TYPE_DATA),
                new WarmupDefaultRuleUsageData(4 * WorkerWarmupTask.MEGABYTE, WarmUpType.WARM_UP_TYPE_BASIC));
        WarmupRulesUsageData warmupRulesUsageData = new WarmupRulesUsageData(workerWarmupColRuleDataList, defaultRuleUsageData);
        when(varadaClient.getRestEndpoint(any())).thenReturn(HttpUriBuilder.uriBuilderFrom(URI.create("http://aaa.com")));
        when(varadaClient.sendWithRetry(any(Request.class), any(FullJsonResponseHandler.class))).thenReturn(warmupRulesUsageData);

        WarmupRulesUsageData result = task.getWithUsage();

        result.warmupColRuleUsageDataList().forEach(warmupColRuleData -> assertThat(warmupColRuleData.getUsedStorageKB())
                .isGreaterThan(0L)
                .isEqualTo(workerWarmupColRuleDataList.stream()
                        .map(WarmupColRuleUsageData::getUsedStorageKB)
                        .reduce(0L, Long::sum) * workers.size()));
        result.warmupDefaultRuleUsageDataList().forEach(warmupDefaultRuleUsageData -> {
            if (warmupDefaultRuleUsageData.warmUpType().equals(WarmUpType.WARM_UP_TYPE_BASIC)) {
                assertThat(warmupDefaultRuleUsageData.usedStorageKB()).isEqualTo(9 * 4 * WorkerWarmupTask.MEGABYTE);
            }
            else {
                assertThat(warmupDefaultRuleUsageData.warmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_DATA);
                assertThat(warmupDefaultRuleUsageData.usedStorageKB()).isEqualTo(9 * 8 * WorkerWarmupTask.MEGABYTE);
            }
        });
    }

    @Test
    public void testValidate()
    {
        WarmupRule warmupRule = WarmupRule.builder()
                .varadaColumn(new RegularColumn("col1"))
                .schema("schema")
                .table("table")
                .warmUpType(io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE)
                .priority(5)
                .ttl(10)
                .predicates(Set.of(new io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule("col2", "2")))
                .build();

        when(warmupRuleService.validate(eq(ImmutableList.of()), eq(List.of(warmupRule))))
                .thenReturn(new WarmupRuleResult(List.of(warmupRule), Map.of(warmupRule, Set.of("error1"))));

        List<WarmupColRuleRejectionData> ruleRejectionDataList = task.validate(List.of(WarmupRuleApiMapper.fromModel(warmupRule)));

        assertThat(ruleRejectionDataList).containsOnly(new WarmupColRuleRejectionData(WarmupRuleApiMapper.fromModel(warmupRule), ImmutableSet.of("error1")));
    }

    @Test
    public void testFetch()
    {
        task.fetch();
        verify(warmupRuleFetcher, times(1)).getWarmupRules();
    }
}
