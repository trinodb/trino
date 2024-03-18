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

import io.trino.plugin.varada.api.warmup.WarmupDefaultRuleUsageData;
import io.trino.plugin.varada.api.warmup.WarmupRulesUsageData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerWarmupRuleService;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.SchemaTableName;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerWarmupTaskTest
{
    private RowGroupDataService rowGroupDataService;
    private WarmupDemoterService warmupDemoterService;
    private WorkerWarmupRuleService workerWarmupRuleService;
    private WorkerWarmupTask task;

    @BeforeEach
    public void before()
    {
        rowGroupDataService = mock(RowGroupDataService.class);
        warmupDemoterService = mock(WarmupDemoterService.class);
        workerWarmupRuleService = mock(WorkerWarmupRuleService.class);
        task = new WorkerWarmupTask(rowGroupDataService, warmupDemoterService, workerWarmupRuleService);
    }

    @Test
    public void testGet()
    {
        assertThat(task.get().warmupColRuleUsageDataList()).isEmpty();

        Map<SchemaTableColumn, WarmupRule> warmupRulesMap = new HashMap<>();
        Map<Pair<SchemaTableColumn, WarmUpType>, WarmUpElement> warmUpElementsMap = new HashMap<>();

        String schema = "schema1";
        String table = "table1";
        IntStream.range(1, 5).forEach(index -> {
            String column = "c" + index;
            SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName(schema, table), column);
            warmupRulesMap.put(schemaTableColumn,
                    WarmupRule.builder()
                            .schema(schema)
                            .table(table)
                            .varadaColumn(new RegularColumn(column))
                            .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                            .priority(1D)
                            .ttl(2)
                            .predicates(Set.of())
                            .build());
            long lastUsed = Instant.now().toEpochMilli();
            warmUpElementsMap.put(Pair.of(schemaTableColumn, WarmUpType.WARM_UP_TYPE_BASIC),
                    WarmUpElement.builder()
                            .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                            .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                            .recTypeLength(4)
                            .colName(column)
                            .lastUsedTimestamp(lastUsed)
                            .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                            .build());

            warmUpElementsMap.put(Pair.of(schemaTableColumn, WarmUpType.WARM_UP_TYPE_DATA),
                    WarmUpElement.builder()
                            .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                            .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                            .recTypeLength(4)
                            .colName(column)
                            .lastUsedTimestamp(lastUsed)
                            .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                            .build());
        });
        when(workerWarmupRuleService.fetchRulesFromCoordinator()).thenReturn(new ArrayList<>(warmupRulesMap.values()));

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey("schema1", "table1", "file_path", 0, 1L, 0, "", ""))
                .warmUpElements(warmUpElementsMap.values())
                .partitionKeys(Map.of())
                .build();

        when(rowGroupDataService.getAll()).thenReturn(List.of(rowGroupData));

        warmUpElementsMap.forEach((key, warmUpElement) -> {
            WarmupRule warmupRule = warmupRulesMap.get(key.getLeft());
            Optional<WarmupRule> result = warmupRule.getWarmUpType().equals(warmUpElement.getWarmUpType())
                    ? Optional.of(warmupRule) : Optional.empty();
            when(warmupDemoterService.findMostRelevantRuleForWarmupElement(rowGroupData,
                    warmUpElement,
                    Collections.singletonList(warmupRule)))
                    .thenReturn(result);
        });
        when(warmupDemoterService.findMostRelevantRuleForWarmupElement(any(), any(), eq(Collections.emptyList()))).thenReturn(Optional.empty());
        WarmupRulesUsageData warmupRulesUsageData = task.get();

        Collection<WarmupDefaultRuleUsageData> defaultRules = warmupRulesUsageData.warmupDefaultRuleUsageDataList();
        assertThat(defaultRules.size()).isEqualTo(1);
        assertThat(defaultRules.stream().findFirst().orElseThrow().warmUpType()).isEqualTo(io.trino.plugin.varada.api.warmup.WarmUpType.WARM_UP_TYPE_DATA);
    }
}
