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
package io.trino.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonMapperProvider;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.block.BlockJsonSerde;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.LanguageFunctionDefinition;
import io.trino.metadata.LanguageFunctionProvider.LanguageFunctionData;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.FunctionId;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SensitiveExpression;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmentRedactor;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.iterative.rule.test.PatternRecognitionBuilder;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.planprinter.GraphvizPrinter;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.TypeDescriptorKeyDeserializer;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.SENSITIVE_EXPRESSION_REDACTION_ENABLED;
import static io.trino.execution.StageState.RUNNING;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.PlanFragmentRedactor.redactConstraint;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTransactionHandle.create;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStageInfo
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider()
            .withModules(ImmutableSet.of(
                    HandleJsonModule.tableHandleModule(new HandleResolver()),
                    HandleJsonModule.columnHandleModule(new HandleResolver()),
                    HandleJsonModule.partitioningHandleModule(new HandleResolver()),
                    HandleJsonModule.transactionHandleModule(new HandleResolver())))
            .withKeyDeserializers(ImmutableMap.of(
                    TypeDescriptor.class, new TypeDescriptorKeyDeserializer(),
                    Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER)))
            .withJsonDeserializers(ImmutableMap.of(
                    Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                    Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
            .withJsonSerializers(ImmutableMap.of(
                    Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
            .get();
    private static final Symbol SYMBOL = new Symbol(BIGINT, "column");
    private static final Session SESSION = testSessionBuilder()
            .setSystemProperty(SENSITIVE_EXPRESSION_REDACTION_ENABLED, "true")
            .build();

    @Test
    public void testReportingFragmentRetainsSerializationShape()
            throws Exception
    {
        StageInfo stage = StageInfo.createInitial(new QueryId("query"), RUNNING, PlanFragmentRedactor.redact(createFragment(null, Optional.empty()), ImmutableMap.of(), true));

        String json = JSON_MAPPER.writeValueAsString(stage);
        JsonNode serializedPlan = JSON_MAPPER.readTree(json).get("plan");

        assertThat(serializedPlan.has("root")).isTrue();
        assertThat(serializedPlan.has("partitioning")).isTrue();
        assertThat(JSON_MAPPER.readValue(json, StageInfo.class).plan().fragment().getRoot().getId()).isEqualTo(stage.plan().fragment().getRoot().getId());
    }

    @Test
    public void testSensitivePlanDoesNotSerializeExecutableExpression()
            throws Exception
    {
        Expression predicate = new SensitiveExpression(comparison(
                EQUAL,
                SYMBOL.toSymbolReference(),
                new Constant(BIGINT, 987654321L)));
        StageInfo stage = StageInfo.createInitial(
                new QueryId("query"),
                RUNNING,
                PlanFragmentRedactor.redact(createFragment(predicate, Optional.of("{\"details\":[\"[REDACTED]\"]}")), ImmutableMap.of(), true));

        JsonNode serializedPlan = JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsString(stage)).get("plan");

        assertThat(serializedPlan.has("root")).isTrue();
        assertThat(serializedPlan.has("partitioning")).isTrue();
        assertThat(serializedPlan.get("root").get("id").asText()).isEqualTo("filter");
        assertThat(serializedPlan.get("root").get("source").get("id").asText()).isEqualTo("values");
        assertThat(serializedPlan.get("jsonRepresentation").asText()).contains(SensitiveExpression.REDACTED);
        assertThat(serializedPlan.toString()).doesNotContain("987654321");
        assertThat(((FilterNode) stage.plan().fragment().getRoot()).getPredicate())
                .isEqualTo(new SensitiveExpression(new Constant(predicate.type(), null)));
    }

    @Test
    public void testReportingFragmentIsCreatedOnceAndExecutionIsUnchanged()
    {
        Expression sensitive = new SensitiveExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression userPredicate = comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 123L));
        PlanFragment executable = createFragment(Logical.and(sensitive, userPredicate), Optional.empty());
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), true, Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());

        PlanFragment reporting = stateMachine.getStageInfo(ImmutableList::of).plan().fragment();
        assertThat(stateMachine.getStageInfo(ImmutableList::of).plan().fragment()).isSameAs(reporting);
        assertThat(stateMachine.getFragment()).isSameAs(executable);
        assertThat(((FilterNode) executable.getRoot()).getPredicate()).isEqualTo(Logical.and(sensitive, userPredicate));
        assertThat(((FilterNode) reporting.getRoot()).getPredicate())
                .isEqualTo(Logical.and(new SensitiveExpression(new Constant(sensitive.type(), null)), userPredicate));
    }

    @Test
    public void testReportingFragmentShowsOneMarkerPerLogicalExpression()
            throws Exception
    {
        // Pushdown leaves the policy and the domain derived from it as separate sensitive conjuncts unless they
        // coincide, so their number would reveal how the policy relates to the user's predicate
        Expression policy = new SensitiveExpression(comparison(LESS_THAN_OR_EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression derived = new SensitiveExpression(comparison(LESS_THAN, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression userPredicate = comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 123L));
        Expression marker = new SensitiveExpression(new Constant(policy.type(), null));

        Expression predicate = new Logical(AND, ImmutableList.of(derived, userPredicate, policy));
        PlanFragment executable = createFragment(predicate, Optional.empty());
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), true, Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());
        assertThat(((FilterNode) stateMachine.getStageInfo(ImmutableList::of).plan().fragment().getRoot()).getPredicate())
                .isEqualTo(Logical.and(marker, userPredicate));
        assertThat(((FilterNode) executable.getRoot()).getPredicate()).isEqualTo(predicate);
        JsonNode reportingJson = JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsString(stateMachine.getStageInfo(ImmutableList::of))).get("plan");
        PlanFragment reported = JSON_MAPPER.treeToValue(reportingJson, PlanFragment.class);
        assertThat(((FilterNode) reported.getRoot()).getPredicate()).isEqualTo(Logical.and(marker, userPredicate));
        PlanFragment worker = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(executable), PlanFragment.class);
        assertThat(((FilterNode) worker.getRoot()).getPredicate()).isEqualTo(predicate);
        assertThat(((FilterNode) StageInfo.createInitial(new QueryId("query"), RUNNING, PlanFragmentRedactor.redact(executable, ImmutableMap.of(), true)).plan().fragment().getRoot()).getPredicate())
                .isEqualTo(Logical.and(marker, userPredicate));

        executable = createFragment(Logical.and(derived, Logical.or(policy, derived)), Optional.empty());
        stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), true, Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());
        assertThat(((FilterNode) stateMachine.getStageInfo(ImmutableList::of).plan().fragment().getRoot()).getPredicate()).isEqualTo(marker);
    }

    @Test
    public void testReportingScanOmitsOpaquePolicyDataAndWorkerPlanKeepsIt()
            throws Exception
    {
        PlanNodeId scanId = new PlanNodeId("scan");
        TableScanNode scan = new TableScanNode(
                scanId,
                new TableHandle(TEST_CATALOG_HANDLE, new PolicyTableHandle(987654321L), create()),
                ImmutableList.of(SYMBOL),
                ImmutableMap.of(SYMBOL, new TestingColumnHandle("private_policy_column")),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        SensitiveExpression predicate = new SensitiveExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        PlanFragment executable = createFragmentForPlan(new FilterNode(new PlanNodeId("filter"), scan, predicate), Optional.empty());
        Map<PlanNodeId, TableInfo> tables = ImmutableMap.of(scanId, new TableInfo(Optional.of("test"), new QualifiedObjectName("test", "schema", "orders"), TupleDomain.all()));
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0),
                executable,
                tables,
                true,
                Runnable::run,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats());

        String reportingJson = JSON_MAPPER.writeValueAsString(stateMachine.getStageInfo(ImmutableList::of));
        assertThat(reportingJson).contains("orders", "filter", "scan").doesNotContain("987654321", "private_policy_column", "policyBound");

        String workerJson = JSON_MAPPER.writeValueAsString(executable);
        assertThat(workerJson).contains("987654321", "private_policy_column", "policyBound");
        PlanFragment workerFragment = JSON_MAPPER.readValue(workerJson, PlanFragment.class);
        assertThat(((FilterNode) workerFragment.getRoot()).getPredicate()).isEqualTo(predicate);
        assertThat(((TableScanNode) workerFragment.getRoot().getSources().getFirst()).getTable().connectorHandle())
                .isEqualTo(new PolicyTableHandle(987654321L));

        assertThat(GraphvizPrinter.printDistributed(new SubPlan(PlanFragmentRedactor.redact(executable, tables, true).fragment(), ImmutableList.of())))
                .contains("test.schema.orders", SensitiveExpression.REDACTED)
                .doesNotContain("987654321", "private_policy_column", "policyBound", "RedactedTableHandle");
    }

    public record PolicyTableHandle(long policyBound)
            implements ConnectorTableHandle {}

    @Test
    public void testSensitiveSpatialJoinFilterIsRedacted()
    {
        Symbol other = new Symbol(BIGINT, "other");
        PlanNode root = new SpatialJoinNode(
                new PlanNodeId("join"),
                SpatialJoinNode.Type.INNER,
                new ValuesNode(new PlanNodeId("left"), ImmutableList.of(SYMBOL), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L))))),
                new ValuesNode(new PlanNodeId("right"), ImmutableList.of(other), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 2L))))),
                ImmutableList.of(SYMBOL),
                new SensitiveExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanFragment reported = PlanFragmentRedactor.redact(createFragmentForPlan(root, Optional.empty()), ImmutableMap.of(), true).fragment();

        assertThat(((SpatialJoinNode) reported.getRoot()).getFilter()).isEqualTo(new SensitiveExpression(new Constant(BOOLEAN, null)));
    }

    @Test
    public void testSensitivePatternRecognitionExpressionsAreRedacted()
    {
        Symbol measure = new Symbol(BIGINT, "measure");
        Expression sensitive = new SensitiveExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        AggregationValuePointer countIf = new AggregationValuePointer(
                new TestingFunctionResolution().resolveFunction("count_if", fromTypes(BOOLEAN)),
                new AggregatedSetDescriptor(ImmutableSet.of(), true),
                ImmutableList.of(sensitive),
                Optional.empty(),
                Optional.empty());
        PlanNode root = new PatternRecognitionBuilder()
                .source(new ValuesNode(new PlanNodeId("values"), ImmutableList.of(SYMBOL), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L))))))
                .partitionBy(ImmutableList.of(SYMBOL))
                .addMeasure(measure, new Reference(BIGINT, "count"), ImmutableMap.of("count", countIf))
                .pattern(new IrLabel("A"))
                .addVariableDefinition(new IrLabel("A"), sensitive)
                .build(new PlanNodeIdAllocator());

        PatternRecognitionNode reported = (PatternRecognitionNode) PlanFragmentRedactor.redact(createFragmentForPlan(root, Optional.empty()), ImmutableMap.of(), true).fragment().getRoot();

        Expression marker = new SensitiveExpression(new Constant(BOOLEAN, null));
        ValuePointer pointer = reported.getMeasures().get(measure).getExpressionAndValuePointers().getAssignments().getFirst().valuePointer();
        assertThat(((AggregationValuePointer) pointer).getArguments()).containsExactly(marker);
        assertThat(reported.getVariableDefinitions().get(new IrLabel("A")).getExpression()).isEqualTo(marker);
    }

    @Test
    public void testReportingFragmentsOmitValueRangesAndLanguageFunctions()
    {
        // A value range derived from a sensitive expression reaches the statistics of the fragments that consume it
        PlanFragment source = createFragment(new SensitiveExpression(comparison(LESS_THAN, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L))), Optional.empty());
        PlanNode root = new ValuesNode(new PlanNodeId("values"), ImmutableList.of(SYMBOL), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L)))));
        StatsAndCosts statsAndCosts = new StatsAndCosts(
                ImmutableMap.of(root.getId(), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(SYMBOL, SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(987654321)
                                .setDistinctValuesCount(2)
                                .build())
                        .build()),
                ImmutableMap.of(root.getId(), new PlanCostEstimate(1, 3, 2, 4)));
        LanguageFunctionData policyFunction = LanguageFunctionData.ofDefinition(new LanguageFunctionDefinition("sql", BIGINT, ImmutableList.of(), "policy_body", ImmutableMap.of()), CharVarcharCoercion.LEGACY);
        Map<FunctionId, LanguageFunctionData> languageFunctions = ImmutableMap.of(new FunctionId("policy_function"), policyFunction);
        PlanFragment consumer = createFragmentForPlan(root, statsAndCosts, languageFunctions, Optional.empty());
        SubPlan plan = new SubPlan(consumer, ImmutableList.of(new SubPlan(source, ImmutableList.of())));
        assertThat(PlanFragmentRedactor.needsRedaction(SESSION, plan)).isTrue();
        assertThat(PlanFragmentRedactor.needsRedaction(TEST_SESSION, plan)).isFalse();
        // A sensitive expression the planner removed, like the mask of a column the query does not read, still resolved its language functions
        assertThat(PlanFragmentRedactor.needsRedaction(SESSION, new SubPlan(consumer, ImmutableList.of()))).isTrue();

        PlanFragment reported = PlanFragmentRedactor.redact(consumer, ImmutableMap.of(), true).fragment();

        assertThat(reported.getLanguageFunctions()).isEmpty();
        assertThat(consumer.getLanguageFunctions()).isEqualTo(languageFunctions);

        SymbolStatsEstimate statistics = reported.getStatsAndCosts().getStats().get(root.getId()).getSymbolStatistics(SYMBOL);
        assertThat(statistics.getLowValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(statistics.getHighValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(statistics.getDistinctValuesCount()).isEqualTo(2);
        assertThat(reported.getStatsAndCosts().getCosts()).isEqualTo(statsAndCosts.getCosts());
        assertThat(consumer.getStatsAndCosts()).isSameAs(statsAndCosts);
        assertThat(PlanFragmentRedactor.redact(consumer, ImmutableMap.of(), false).fragment()).isSameAs(consumer);
    }

    @Test
    public void testInitialPlanOfAdaptivePlanIsRedacted()
            throws Exception
    {
        // The initial plan is reported although it is not a source of the node
        PlanNode values = new ValuesNode(new PlanNodeId("values"), ImmutableList.of(SYMBOL), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L)))));
        Expression sensitive = new SensitiveExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        PlanNode root = new AdaptivePlanNode(new PlanNodeId("adaptive"), new FilterNode(new PlanNodeId("initial"), values, sensitive), ImmutableSet.of(SYMBOL), values);

        PlanFragment reported = PlanFragmentRedactor.redact(createFragmentForPlan(root, Optional.empty()), ImmutableMap.of(), true).fragment();

        assertThat(((FilterNode) ((AdaptivePlanNode) reported.getRoot()).getInitialPlan()).getPredicate())
                .isEqualTo(new SensitiveExpression(new Constant(BOOLEAN, null)));
        assertThat(JSON_MAPPER.writeValueAsString(reported)).doesNotContain("987654321");
    }

    @Test
    public void testRedactionFailsForUnknownPlanNode()
    {
        // OffsetNode never reaches a fragment, so it stands in for a node type the redactor does not know
        PlanFragment fragment = createFragmentForPlan(
                new OffsetNode(new PlanNodeId("offset"), new ValuesNode(new PlanNodeId("values"), ImmutableList.of(SYMBOL), ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L))))), 1),
                Optional.empty());
        SubPlan plan = new SubPlan(fragment, ImmutableList.of());

        assertThatThrownBy(() -> PlanFragmentRedactor.needsRedaction(SESSION, plan))
                .hasMessage("Cannot verify redaction of OffsetNode");
        assertThatThrownBy(() -> PlanFragmentRedactor.redact(fragment, ImmutableMap.of(), true))
                .hasMessage("Cannot verify redaction of OffsetNode");
        assertThat(PlanFragmentRedactor.needsRedaction(TEST_SESSION, plan)).isFalse();
    }

    @Test
    public void testRedactConstraint()
    {
        Symbol other = new Symbol(BIGINT, "other");
        ColumnHandle column = new TestingColumnHandle("column");
        ColumnHandle otherColumn = new TestingColumnHandle("other");
        TableScanNode scan = new TableScanNode(
                new PlanNodeId("scan"),
                new TableHandle(TEST_CATALOG_HANDLE, new PolicyTableHandle(1L), create()),
                ImmutableList.of(SYMBOL, other),
                ImmutableMap.of(SYMBOL, column, other, otherColumn),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        TupleDomain<ColumnHandle> domain = TupleDomain.withColumnDomains(ImmutableMap.of(
                column, singleValue(BIGINT, 1L),
                otherColumn, singleValue(BIGINT, 2L)));

        assertThat(redactConstraint(domain, scan, ImmutableSet.of(SYMBOL)))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(otherColumn, singleValue(BIGINT, 2L))));
        assertThat(redactConstraint(domain, scan, ImmutableSet.of(SYMBOL, other))).isEqualTo(TupleDomain.all());
        assertThat(redactConstraint(domain, scan, ImmutableSet.of(new Symbol(BIGINT, "unrelated")))).isSameAs(domain);
        assertThat(redactConstraint(TupleDomain.none(), scan, ImmutableSet.of(SYMBOL))).isEqualTo(TupleDomain.none());
    }

    private static PlanFragment createFragment(Expression predicate, Optional<String> jsonRepresentation)
    {
        PlanNode source = new ValuesNode(
                new PlanNodeId("values"),
                ImmutableList.of(SYMBOL),
                ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 1L)))));
        PlanNode root = predicate == null
                ? source
                : new FilterNode(new PlanNodeId("filter"), source, predicate);
        return createFragmentForPlan(root, jsonRepresentation);
    }

    private static PlanFragment createFragmentForPlan(PlanNode root, Optional<String> jsonRepresentation)
    {
        return createFragmentForPlan(root, StatsAndCosts.empty(), ImmutableMap.of(), jsonRepresentation);
    }

    private static PlanFragment createFragmentForPlan(PlanNode root, StatsAndCosts statsAndCosts, Map<FunctionId, LanguageFunctionData> languageFunctions, Optional<String> jsonRepresentation)
    {
        return new PlanFragment(
                new PlanFragmentId("0"),
                root,
                ImmutableSet.of(SYMBOL),
                SINGLE_DISTRIBUTION,
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                        ImmutableList.of(SYMBOL)),
                OptionalInt.empty(),
                statsAndCosts,
                ImmutableList.of(),
                languageFunctions,
                jsonRepresentation);
    }
}
