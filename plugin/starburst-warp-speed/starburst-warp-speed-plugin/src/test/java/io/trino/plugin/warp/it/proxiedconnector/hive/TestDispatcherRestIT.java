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

package io.trino.plugin.warp.it.proxiedconnector.hive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.varada.api.health.HealthResult;
import io.trino.plugin.varada.api.warmup.DateRangeSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.api.warmup.RuleResultDTO;
import io.trino.plugin.varada.api.warmup.WarmUpType;
import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.api.warmup.column.RegularColumnData;
import io.trino.plugin.varada.api.warmup.column.TransformedColumnData;
import io.trino.plugin.varada.api.warmup.expression.TransformFunctionData;
import io.trino.plugin.varada.api.warmup.expression.VaradaExpressionData;
import io.trino.plugin.varada.api.warmup.expression.VaradaPrimitiveConstantData;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.varada.warmup.WarmupRuleService;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.VaradaStubsStorageEngineModule;
import io.trino.plugin.warp.extension.execution.callhome.CallAllHomesResource;
import io.trino.plugin.warp.extension.execution.callhome.CallHomeData;
import io.trino.plugin.warp.extension.execution.callhome.CallHomeResource;
import io.trino.plugin.warp.extension.execution.debugtools.NativeStorageStateResource;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupCountResult;
import io.trino.plugin.warp.extension.execution.debugtools.RowGroupTask;
import io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNoteVersionData;
import io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNotesRequestData;
import io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNotesRequestData.ReleaseNoteContentType;
import io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNotesResource;
import io.trino.plugin.warp.extension.execution.health.ClusterHealthTask;
import io.trino.plugin.warp.extension.execution.warmup.WarmingResource;
import io.trino.plugin.warp.extension.execution.warmup.WarmingStatusData;
import io.trino.plugin.warp.extension.execution.warmup.WarmupTask;
import io.trino.plugin.warp.it.DispatcherAbstractTestQueryFramework;
import io.trino.plugin.warp.it.DispatcherQueryRunner;
import io.trino.testing.QueryRunner;
import jakarta.ws.rs.HttpMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME;
import static io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration.USE_HTTP_SERVER_PORT;
import static io.trino.plugin.warp.extension.execution.health.HealthTask.HEALTH_PATH;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDispatcherRestIT
        extends DispatcherAbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "rest";

    @AfterEach
    public void afterTest()
    {
        cleanWarmupRules();
        cleanModel();
    }

    @BeforeEach
    @Override
    public void beforeMethod(TestInfo testInfo)
    {
        super.beforeMethod(testInfo);
        initializeWorkers(CATALOG_NAME);
    }

    private void cleanModel()
    {
        createdTables.forEach((tableName) -> assertUpdate("DROP TABLE IF EXISTS " + tableName));
        createdSchemas.forEach((schemaName) -> assertUpdate("DROP SCHEMA IF EXISTS " + schemaName));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(new VaradaStubsStorageEngineModule(),
                Optional.empty(), 2,
                Map.of(),
                Map.of("http-server.log.enabled", "false",
                        WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT, "false",
                        "node.environment", "varada",
                        WARP_SPEED_PREFIX + PROXIED_CONNECTOR, HIVE_CONNECTOR_NAME),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG_NAME,
                new WarpPlugin(),
                Collections.emptyMap());
    }

    @Test
    public void testWarmupApi()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleDataLucene = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", "")));
        WarmupColRuleData warmupColRuleDataData = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_DATA,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", "")));

        WarmupColRuleData basicWarmupColRuleDataData = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_BASIC,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new DateRangeSlidingWindowWarmupPredicateRule("col2", 30, 15, "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataLucene, warmupColRuleDataData, basicWarmupColRuleDataData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        result = getWarmupRules();

        assertThat(result).hasSize(3);

        WarmupColRuleData warmupColRuleDataError = new WarmupColRuleData(10,
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of());
        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataError), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        RuleResultDTO ruleResultDTO = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);
        assertThat(ruleResultDTO.appliedRules().isEmpty()).isTrue();
        assertThat(ruleResultDTO.rejectedRules().isEmpty()).isFalse();
        result = getWarmupRules();

        assertThat(result).hasSize(3);

        WarmupColRuleData luceneWarmupColRuleDataResult = result.stream().filter(warmupColRuleData -> warmupColRuleData.getWarmUpType() == WarmUpType.WARM_UP_TYPE_LUCENE).findFirst().orElseThrow();
        assertThat(luceneWarmupColRuleDataResult.getId()).isGreaterThan(0);
        assertThat(luceneWarmupColRuleDataResult.getColumn()).isEqualTo(warmupColRuleDataLucene.getColumn());
        assertThat(luceneWarmupColRuleDataResult.getSchema()).isEqualTo(warmupColRuleDataLucene.getSchema());
        assertThat(luceneWarmupColRuleDataResult.getTable()).isEqualTo(warmupColRuleDataLucene.getTable());
        assertThat(luceneWarmupColRuleDataResult.getPredicates()).isEqualTo(warmupColRuleDataLucene.getPredicates());

        WarmupColRuleData basicWarmupColRuleDataResult = result.stream().filter(warmupColRuleData -> warmupColRuleData.getWarmUpType() == WarmUpType.WARM_UP_TYPE_BASIC).findFirst().orElseThrow();
        assertThat(basicWarmupColRuleDataResult.getId()).isGreaterThan(0);
        assertThat(basicWarmupColRuleDataResult.getColumn()).isEqualTo(warmupColRuleDataLucene.getColumn());
        assertThat(basicWarmupColRuleDataResult.getSchema()).isEqualTo(warmupColRuleDataLucene.getSchema());
        assertThat(basicWarmupColRuleDataResult.getTable()).isEqualTo(warmupColRuleDataLucene.getTable());
        assertThat(basicWarmupColRuleDataResult.getPredicates()).isEqualTo(basicWarmupColRuleDataData.getPredicates());

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_DELETE, result.stream().map(WarmupColRuleData::getId).collect(toList()), HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);
        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    private static Stream<Arguments> provideTransformedColumnDataTestParams()
    {
        return Stream.of(
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.LOWER))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.UPPER))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.DATE))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("index", VaradaExpressionData.Type.VARCHAR))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7, VaradaExpressionData.Type.INTEGER))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("7", VaradaExpressionData.Type.INTEGER))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7L, VaradaExpressionData.Type.BIGINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7, VaradaExpressionData.Type.BIGINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("7", VaradaExpressionData.Type.BIGINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData((short) 7, VaradaExpressionData.Type.SMALLINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7, VaradaExpressionData.Type.SMALLINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("7", VaradaExpressionData.Type.SMALLINT))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7.0, VaradaExpressionData.Type.DOUBLE))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7F, VaradaExpressionData.Type.DOUBLE))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("7.0", VaradaExpressionData.Type.DOUBLE))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7F, VaradaExpressionData.Type.REAL))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData(7.0, VaradaExpressionData.Type.REAL))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                                ImmutableList.of(new VaradaPrimitiveConstantData("7.0", VaradaExpressionData.Type.REAL))))),
                Arguments.arguments(new TransformedColumnData("col2",
                        new TransformFunctionData(TransformFunctionData.TransformType.JSON_EXTRACT_SCALAR,
                                ImmutableList.of(new VaradaPrimitiveConstantData("$.field", VaradaExpressionData.Type.VARCHAR))))));
    }

    @ParameterizedTest
    @MethodSource("provideTransformedColumnDataTestParams")
    void testWarmupApiTransformedColumn(TransformedColumnData transformedColumnData)
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData basicWarmupColRuleData = new WarmupColRuleData(0,
                "s1",
                "t1",
                transformedColumnData,
                WarmUpType.WARM_UP_TYPE_BASIC,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new DateRangeSlidingWindowWarmupPredicateRule("col2", 30, 15, "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(basicWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        result = getWarmupRules();

        assertThat(result).hasSize(1);

        WarmupColRuleData basicWarmupColRuleDataResult = result.stream().filter(warmupColRuleData -> warmupColRuleData.getWarmUpType() == WarmUpType.WARM_UP_TYPE_BASIC).findFirst().orElseThrow();
        assertThat(basicWarmupColRuleDataResult.getId()).isGreaterThan(0);
        assertThat(basicWarmupColRuleDataResult.getColumn()).isEqualTo(basicWarmupColRuleData.getColumn());
        assertThat(basicWarmupColRuleDataResult.getSchema()).isEqualTo(basicWarmupColRuleData.getSchema());
        assertThat(basicWarmupColRuleDataResult.getTable()).isEqualTo(basicWarmupColRuleData.getTable());
        assertThat(basicWarmupColRuleDataResult.getPredicates()).isEqualTo(basicWarmupColRuleData.getPredicates());

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_DELETE, result.stream().map(WarmupColRuleData::getId).collect(toList()), HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);
        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    @Test
    void testWarmupApiTransformedColumnUnknownTransformType()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData basicWarmupColRuleData = new WarmupColRuleData(0,
                "s1",
                "t1",
                new TransformedColumnData("col2", new TransformFunctionData(TransformFunctionData.TransformType.NONE)),
                WarmUpType.WARM_UP_TYPE_BASIC,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new DateRangeSlidingWindowWarmupPredicateRule("col2", 30, 15, "")));

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(basicWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_INTERNAL_ERROR);

        assertThat(restResult).contains("Unknown apiTransformType NONE");

        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    @Test
    void testWarmupApiTransformedColumnIllegalValueType()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData basicWarmupColRuleData = new WarmupColRuleData(0,
                "s1",
                "t1",
                new TransformedColumnData("col2", new TransformFunctionData(TransformFunctionData.TransformType.ELEMENT_AT,
                        ImmutableList.of(new VaradaPrimitiveConstantData(7, VaradaExpressionData.Type.VARCHAR)))),
                WarmUpType.WARM_UP_TYPE_BASIC,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new DateRangeSlidingWindowWarmupPredicateRule("col2", 30, 15, "")));

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(basicWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_INTERNAL_ERROR);

        assertThat(restResult).contains("Could not convert ");

        result = getWarmupRules();

        assertThat(result).isEmpty();
    }

    @Test
    public void testWarmupApiUpdatePriority()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleDataLucene = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataLucene), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        result = getWarmupRules();

        assertThat(result).hasSize(1);
        warmupColRuleDataLucene = new WarmupColRuleData(result.getFirst().getId(),
                "s1",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                10,
                Duration.ofMillis(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", "")));
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataLucene), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        result = getWarmupRules();
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.getFirst().getPriority()).isEqualTo(10);
    }

    @Test
    public void testWarmupApiValidations_BloomIllegalType()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s4", "t1", format("(%s tinyint)", "col1"));

        WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                "s4",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_BLOOM_HIGH,
                5,
                Duration.ofMillis(10),
                ImmutableSet.of());

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        RuleResultDTO ruleResultDTO = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);
        assertThat(ruleResultDTO.appliedRules().isEmpty()).isTrue();
        assertThat(ruleResultDTO.rejectedRules().isEmpty()).isFalse();
        assertThat(ruleResultDTO.rejectedRules().getFirst().errors().stream().findFirst())
                .contains("600: Bloom index can be applied only to types that use index length smaller than 4");
    }

    @Test
    public void testWarmupApiValidations_2BloomIndexShouldFail()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();
        createSchemaAndTable("s1", "t1", format("(%s integer)", "col1"));

        WarmupColRuleData warmupColRuleDataHigh = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_BLOOM_HIGH,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of());

        WarmupColRuleData warmupColRuleDataLow = new WarmupColRuleData(0,
                "s1",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_BLOOM_LOW,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of());

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleDataHigh, warmupColRuleDataLow), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        RuleResultDTO ruleResultDTO = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);
        assertThat(ruleResultDTO.rejectedRules().size()).isEqualTo(1);
        assertThat(ruleResultDTO.rejectedRules().getFirst().errors().toString().contains("Only single bloom index allowed")).isTrue();
        assertThat(ruleResultDTO.appliedRules().size()).isEqualTo(1);
    }

    @Test
    public void testWarmupApiDuplicateRuleShouldFail()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();

        createSchemaAndTable("s3", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col2", 30, "XXX", "")));

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleData, warmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        RuleResultDTO ruleResultDTO = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);
        assertThat(ruleResultDTO.appliedRules().size()).isEqualTo(1);
        assertThat(ruleResultDTO.rejectedRules().size()).isEqualTo(1);
        assertThat(ruleResultDTO.rejectedRules().getFirst().errors().toString().contains("can't add 2 rules with the same key")).isTrue();
    }

    @Test
    public void testWarmupApiInvalidWarmUpTypeShouldFail()
            throws IOException
    {
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result).isEmpty();

        createSchemaAndTable("s3", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col1", "2")));

        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(warmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        RuleResultDTO ruleResultDTO = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);
        assertThat(ruleResultDTO.rejectedRules().size()).isEqualTo(1);
        assertThat(ruleResultDTO.rejectedRules().getFirst().errors().toString().contains("Warmup type WARM_UP_TYPE_LUCENE doesn't support column type integer")).isTrue();
        assertThat(ruleResultDTO.appliedRules().size()).isZero();
    }

    @Test
    public void testWarmupApiReplace()
            throws IOException
    {
        createSchemaAndTable("s3", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col1", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col1", 30, "XXX", "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(warmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        WarmupColRuleData newWarmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_DATA,
                10,
                Duration.ofMinutes(10),
                ImmutableSet.of());
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(newWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        List<WarmupColRuleData> result = getWarmupRules();
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.getFirst().getPriority()).isEqualTo(10);
        assertThat(result.getFirst().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_DATA);

        WarmupColRuleData updateWarmupColRuleData = new WarmupColRuleData(result.getFirst().getId(),
                "s3",
                "t1",
                new RegularColumnData("col2"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                10,
                Duration.ofMinutes(10),
                ImmutableSet.of());
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_SET, List.of(updateWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        result = getWarmupRules();
        assertThat(result.size()).isEqualTo(1);
        assertThat(result.getFirst().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_LUCENE);

        // replace with empty list should delete all
        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(), HttpMethod.POST, HttpURLConnection.HTTP_OK);
        result = getWarmupRules();
        assertThat(result).isEmpty();
    }

    @Test
    public void testWarmupApiFailedReplaceShouldKeepPreviousRules()
            throws IOException
    {
        createSchemaAndTable("s3", "t1", format("(%s integer, %s varchar(20))", "col1", "col2"));

        WarmupColRuleData warmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_LUCENE,
                5,
                Duration.ofMinutes(10),
                ImmutableSet.of(new PartitionValueWarmupPredicateRule("col1", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col1", 30, "XXX", "")));

        executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(warmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        WarmupColRuleData newWarmupColRuleData = new WarmupColRuleData(0,
                "s3",
                "t1",
                new RegularColumnData("col1"),
                WarmUpType.WARM_UP_TYPE_DATA,
                10,
                Duration.ofMinutes(10),
                ImmutableSet.of());
        String restResult = executeRestCommand(WarmupRuleService.WARMUP_PATH, WarmupTask.TASK_NAME_REPLACE, List.of(newWarmupColRuleData, newWarmupColRuleData), HttpMethod.POST, HttpURLConnection.HTTP_OK);

        RuleResultDTO result = objectMapper.readerFor(RuleResultDTO.class).readValue(restResult);

        assertThat(result.appliedRules().size()).isEqualTo(1);
        assertThat(result.appliedRules().getFirst().getPriority()).isEqualTo(10);
        assertThat(result.rejectedRules().size()).isEqualTo(1);
        assertThat(result.rejectedRules().getFirst().errors().stream().findFirst().orElseThrow()).contains("can't add 2 rules with the same key");
    }

    @Test
    public void testRowGroupCount()
            throws IOException
    {
        RowGroupCountResult result = getRowGroupCount();
        assertThat(result.nodesWarmupElementsCount().size()).isEqualTo(1);

        String str = executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_COUNT_WITH_FILES_TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        result = objectMapper.readerFor(RowGroupCountResult.class).readValue(str);
        assertThat(result.nodesWarmupElementsCount()).hasSize(1);
        assertThat(result.rowGroupFilePathSet()).hasSize(1);
        executeRestCommand(RowGroupTask.ROW_GROUP_PATH, RowGroupTask.ROW_GROUP_RESET_TASK_NAME, null, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
    }

    @Test
    public void testCallHome()
            throws IOException
    {
        CallHomeData callHomeData = new CallHomeData("s3://stam-location", true, true);
        executeRestCommand(CallHomeResource.CALL_HOME_PATH, "", callHomeData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        executeRestCommand(CallAllHomesResource.CALL_ALL_HOMES_PATH, "", callHomeData, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
    }

    @Test
    public void testWarmingStatus()
            throws IOException
    {
        String str = executeRestCommand(WarmingResource.WARMING, WarmingResource.WARMING_STATUS, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        WarmingStatusData result = objectMapper.readerFor(WarmingStatusData.class).readValue(str);
        assertThat(result.nodesStatus()).isNotEmpty();
        assertThat(result.warming()).isFalse();
    }

    @Disabled
    @Test
    public void testReleaseNotes()
            throws IOException
    {
        ReleaseNotesRequestData requestData = new ReleaseNotesRequestData(ReleaseNoteContentType.LATEST);
        String str = executeRestCommand(ReleaseNotesResource.RELEASE_NOTES_PATH, "", requestData, HttpMethod.POST, HttpURLConnection.HTTP_OK);
        List<ReleaseNoteVersionData> result = objectMapper.readValue(str, new TypeReference<>() {});

        assertThat(result).isNotEmpty();
    }

    @Test
    public void testClusterHealth()
            throws IOException
    {
        String str = executeRestCommand(HEALTH_PATH, ClusterHealthTask.TASK_NAME, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
        HealthResult result = objectMapper.readerFor(HealthResult.class).readValue(str);

        assertThat(result).isNotNull();
        assertThat(result.getHealthNodes().size()).isEqualTo(1);
        assertThat(result.isReady()).isEqualTo(true);
        assertThat(result.getHealthNodes().getFirst().createEpochTime()).isGreaterThan(0);
    }

    @Test
    public void testNativeStorageState()
            throws IOException
    {
        String str = executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        NativeStorageStateResource.NativeStorageState state = objectMapper.readValue(str, NativeStorageStateResource.NativeStorageState.class);
        assertThat(state).isNotNull();
        assertThat(state.storagePermanentException()).isEqualTo(false);
        assertThat(state.storageTemporaryException()).isEqualTo(false);

        executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(false, false),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);

        str = executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        state = objectMapper.readValue(str, NativeStorageStateResource.NativeStorageState.class);
        assertThat(state).isNotNull();
        assertThat(state.storagePermanentException()).isEqualTo(false);
        assertThat(state.storageTemporaryException()).isEqualTo(false);

        executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(true, false),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);
        str = executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        state = objectMapper.readValue(str, NativeStorageStateResource.NativeStorageState.class);
        assertThat(state).isNotNull();
        assertThat(state.storagePermanentException()).isEqualTo(true);
        assertThat(state.storageTemporaryException()).isEqualTo(false);

        executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(false, true),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);
        str = executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        state = objectMapper.readValue(str, NativeStorageStateResource.NativeStorageState.class);
        assertThat(state).isNotNull();
        assertThat(state.storagePermanentException()).isEqualTo(false);
        assertThat(state.storageTemporaryException()).isEqualTo(true);

        executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                new NativeStorageStateResource.NativeStorageState(false, false),
                HttpMethod.POST,
                HttpURLConnection.HTTP_NO_CONTENT);
        str = executeRestCommand(
                NativeStorageStateResource.PATH,
                "",
                null,
                HttpMethod.GET,
                HttpURLConnection.HTTP_OK);
        state = objectMapper.readValue(str, NativeStorageStateResource.NativeStorageState.class);
        assertThat(state).isNotNull();
        assertThat(state.storagePermanentException()).isEqualTo(false);
        assertThat(state.storageTemporaryException()).isEqualTo(false);
    }
}
