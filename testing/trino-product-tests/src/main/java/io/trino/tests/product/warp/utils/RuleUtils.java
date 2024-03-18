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
package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import io.airlift.log.Logger;
import io.trino.plugin.varada.api.warmup.RuleResultDTO;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import jakarta.ws.rs.HttpMethod;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.WarmTypeForStrings.lucene_data_basic;
import static io.trino.tests.product.warp.utils.WarmTypeForStrings.lucene_data_only;
import static io.trino.tests.product.warp.utils.WarmTypeForStrings.no_lucene;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class RuleUtils
{
    protected static final int DEFAULT_PRIORITY = 8;
    public static final String TASK_NAME_SET = "warmup-rule-set";
    public static final String WARMUP_PATH = "warmup";
    public static final String TASK_NAME_GET = "warmup-rule-get";
    public static final String TASK_NAME_DELETE = "warmup-rule-delete";
    protected static final Duration DEFAULT_TTL = Duration.ofMinutes(10);
    private static final Logger logger = Logger.get(RuleUtils.class);

    private final RestUtils restUtils = new RestUtils();

    public RuleUtils() {}

    public void resetAllRules()
            throws IOException
    {
        List<WarmupColRuleData> existingRules = getExistingRules();
        List<Integer> rulesId = existingRules.stream()
                .map(WarmupColRuleData::getId)
                .toList();
        deleteRules(rulesId);
        assertThat(getExistingRules()).isEmpty();
    }

    public void resetTableRules(String schema, TestFormat testFormat)
            throws IOException
    {
        List<WarmupColRuleData> existingRules = getExistingRules();
        List<Integer> rulesId = existingRules.stream().filter(x -> x.getSchema().equalsIgnoreCase(schema) &&
                        x.getTable().equalsIgnoreCase(testFormat.name()))
                .map(WarmupColRuleData::getId)
                .toList();
        deleteRules(rulesId);
    }

    private void deleteRules(List<Integer> rulesId)
            throws IOException
    {
        restUtils.executeDeleteCommand(WARMUP_PATH, TASK_NAME_DELETE, rulesId);
        logger.info("deleted %s rules", rulesId.size());
    }

    public List<WarmupColRuleData> getExistingRules()
            throws IOException
    {
        String rules = restUtils.executeGetCommand(WARMUP_PATH, TASK_NAME_GET);
        return objectMapper.readerFor(new TypeReference<List<WarmupColRuleData>>() {}).readValue(rules);
    }

    private List<WarmUpType> calcWarmupTypesForColumn(TestFormat.Column column, WarmTypeForStrings warmTypeForStrings)
    {
        List<WarmUpType> columnWarmupTypes = new ArrayList<>();
        String columnType = column.type();
        if (columnType.equals("map") || columnType.equals("row")) {
            return columnWarmupTypes;
        }
        columnWarmupTypes.add(WarmUpType.WARM_UP_TYPE_DATA);
        if (columnType.equals("array")) {
            return columnWarmupTypes;
        }
        boolean isString = columnType.equalsIgnoreCase("varchar") ||
                columnType.equalsIgnoreCase("char");
        if (!isString || warmTypeForStrings == no_lucene || warmTypeForStrings == lucene_data_basic) {
            columnWarmupTypes.add(WarmUpType.WARM_UP_TYPE_BASIC);
        }
        if (isString && (warmTypeForStrings == lucene_data_only || warmTypeForStrings == lucene_data_basic)) {
            columnWarmupTypes.add(WarmUpType.WARM_UP_TYPE_LUCENE);
        }
        return columnWarmupTypes;
    }

    public void createWarmupRules(String schema, TestFormat testFormat)
            throws IOException
    {
        List<TestFormat.WarmupRule> warmupRules = testFormat.warmup_rules();
        Set<WarmupColRuleData> rules = new HashSet<>();
        WarmTypeForStrings warmTypeForStrings = testFormat.warm_type_for_strings() != null ?
                testFormat.warm_type_for_strings() :
                lucene_data_basic;
        if (warmupRules == null) {
            rules = createRulesFromStructure(schema, testFormat, warmTypeForStrings);
        }
        else {
            for (TestFormat.WarmupRule warmupRule : warmupRules) {
                List<WarmUpType> warmUpTypes = warmupRule.warmUpTypes();
                if (warmUpTypes == null) {
                    TestFormat.Column column = testFormat.structure().stream().filter(x -> x.name().equalsIgnoreCase(warmupRule.colNameId())).findFirst().orElseThrow();
                    warmUpTypes = calcWarmupTypesForColumn(column, warmTypeForStrings);
                }
                for (WarmUpType warmUpType : warmUpTypes) {
                    WarmupColRuleData rule = new WarmupColRuleData(0,
                            schema,
                            testFormat.name(),
                            new RegularColumnData(warmupRule.colNameId()),
                            warmUpType,
                            DEFAULT_PRIORITY,
                            DEFAULT_TTL,
                            ImmutableSet.of());
                    rules.add(rule);
                }
            }
        }
        if (rules.isEmpty()) {
            logger.info("no rules for test %s", testFormat.name());
            return;
        }
        String result = restUtils.executePostCommandWithReturnValue(WARMUP_PATH, TASK_NAME_SET, rules);
        RuleResultDTO res = objectMapper.readerFor(new TypeReference<RuleResultDTO>() {}).readValue(result);
        assertThat(res.rejectedRules().isEmpty() && !res.appliedRules().isEmpty())
                .as("some rules are rejected. %s", res.rejectedRules())
                .isTrue();
        logger.debug("created %s rules for schemaTable=%s.%s", res.appliedRules(), schema, testFormat.name());
    }

    public SetMultimap<String, Object> getCustomStats(String queryId, String summaryType)
    {
        String prefix = format("/v1/query/%s?", queryId);

        SetMultimap<String, Object> metricsMultiMap = HashMultimap.create();
        try {
            String pretty = restUtils.executeTrinoCommand(prefix, "pretty", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
            List<JsonNode> customMetrics = objectMapper.readTree(pretty).get("queryStats").get("operatorSummaries").findValues(summaryType).stream().filter(x -> !x.isEmpty()).toList();

            for (JsonNode jsonNode : customMetrics) {
                Map<String, Object> customMetricsAsMap = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(jsonNode);
                customMetricsAsMap.entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith("dispatcher"))
                        .forEach(entry -> metricsMultiMap.put(
                                entry.getKey().substring(entry.getKey().indexOf(":") + 1),
                                entry.getValue()));
            }
        }
        catch (Exception e) {
            logger.error(e, "error");
        }
        return metricsMultiMap;
    }

    public void validateLoadByCacheDataOperator(String queryId)
            throws IOException
    {
        String prefix = format("/v1/query/%s?", queryId);
        String pretty = restUtils.executeTrinoCommand(prefix, "pretty", null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        List<String> operatorTypes = objectMapper.readTree(pretty).get("queryStats").get("operatorSummaries").findValues("operatorType").stream().map(JsonNode::asText).toList();
        assertThat(operatorTypes.contains("LoadCachedDataOperator")).isTrue().describedAs("validate that LoadCachedDataOperator stage was applied");
        assertThat(operatorTypes.contains("ScanFilterAndProjectOperator")).isFalse().describedAs("validate that ScanFilterAndProjectOperator stage was not applied");
    }

    public Set<WarmupColRuleData> createRulesFromStructure(String schema,
            TestFormat testFormat,
            WarmTypeForStrings warmTypeForStrings)
    {
        List<TestFormat.Column> structure = testFormat.structure();
        String tableName = testFormat.name();
        Set<WarmupColRuleData> rulesFromStructure = new HashSet<>();
        for (TestFormat.Column column : structure) {
            List<WarmUpType> columnWarmupTypes = calcWarmupTypesForColumn(column, warmTypeForStrings);
            for (WarmUpType warmUpType : columnWarmupTypes) {
                WarmupColRuleData rule = new WarmupColRuleData(0,
                        schema,
                        tableName,
                        new RegularColumnData(column.name()),
                        warmUpType,
                        DEFAULT_PRIORITY,
                        DEFAULT_TTL,
                        ImmutableSet.of());
                rulesFromStructure.add(rule);
            }
        }
        return rulesFromStructure;
    }
}
