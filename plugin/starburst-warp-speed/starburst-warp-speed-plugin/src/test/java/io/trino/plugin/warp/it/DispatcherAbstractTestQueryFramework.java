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
package io.trino.plugin.warp.it;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.WarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.WarmupPropertiesData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import io.trino.plugin.varada.api.warmup.column.TransformedColumnData;
import io.trino.plugin.varada.api.warmup.column.VaradaColumnData;
import io.trino.plugin.varada.api.warmup.expression.TransformFunctionData;
import io.trino.plugin.varada.it.smoke.VaradaAbstractTestQueryFramework;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import jakarta.ws.rs.HttpMethod;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.fail;

public abstract class DispatcherAbstractTestQueryFramework
        extends VaradaAbstractTestQueryFramework
{
    protected static final String DEFAULT_SCHEMA = "schema";
    protected static final int DEFAULT_PRIORITY = 0;
    protected static final int NEVER_PRIORITY = -10;
    protected static final Duration DEFAULT_TTL = Duration.ofSeconds(5);

    protected void createWarmupRules(String schema, String table, Map<String, Set<WarmupPropertiesData>> warmupMap)
            throws IOException
    {
        createWarmupRules(schema, table, warmupMap, ImmutableSet.of());
    }

    protected void createWarmupRules(String schema, String table, Map<String, Set<WarmupPropertiesData>> warmupMap, ImmutableSet<WarmupPredicateRule> predicates)
            throws IOException
    {
        List<WarmupColRuleData> rules = new ArrayList<>();
        VaradaColumnData column;

        for (Entry<String, Set<WarmupPropertiesData>> warmups : warmupMap.entrySet()) {
            for (WarmupPropertiesData warmupPropertiesData : warmups.getValue()) {
                if (warmupPropertiesData.transformFunction().equals(new TransformFunctionData(TransformFunctionData.TransformType.NONE))) {
                    column = new RegularColumnData(warmups.getKey());
                }
                else {
                    column = new TransformedColumnData(warmups.getKey(), warmupPropertiesData.transformFunction());
                }
                rules.add(new WarmupColRuleData(0,
                        schema,
                        table,
                        column,
                        warmupPropertiesData.warmUpType(),
                        warmupPropertiesData.priority(),
                        Duration.ofSeconds(warmupPropertiesData.ttl().getSeconds()),
                        predicates));
            }
        }
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, rules, HttpMethod.POST, HttpURLConnection.HTTP_OK);
    }

    protected void cleanWarmupRules()
    {
        try {
            List<WarmupColRuleData> result = objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                    .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupRuleService.TASK_NAME_GET, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));
            List<Integer> ruleIds = result.stream().map(WarmupColRuleData::getId).collect(Collectors.toList());

            executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_DELETE, ruleIds, HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);
        }
        catch (Exception e) {
            fail("failed to clean warmup rule", e);
        }
    }

    protected RowGroupCountResult getRowGroupCount()
            throws IOException
    {
        String s = executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        return objectMapper.readerFor(RowGroupCountResult.class).readValue(s);
    }

    protected List<WarmupColRuleData> getWarmupRules()
    {
        try {
            return objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {})
                    .readValue(executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupRuleService.TASK_NAME_GET, null, HttpMethod.GET, HttpURLConnection.HTTP_OK));
        }
        catch (Exception e) {
            fail("failed to clean warmup rule", e);
            return Collections.emptyList();
        }
    }
}
