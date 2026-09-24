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
import io.trino.block.BlockJsonSerde;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.QueryId;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.type.TypeDescriptorKeyDeserializer;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.StageState.RUNNING;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTransactionHandle.create;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testOrdinaryPlanRetainsSerializationShape()
            throws Exception
    {
        StageInfo stage = StageInfo.createInitial(new QueryId("query"), RUNNING, createFragment(null, Optional.empty()));

        JsonNode serializedPlan = JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsString(stage)).get("plan");

        assertThat(serializedPlan.has("root")).isTrue();
        assertThat(serializedPlan.has("partitioning")).isTrue();
    }

    @Test
    public void testSecurePlanDoesNotSerializeExecutableExpression()
            throws Exception
    {
        Expression predicate = new SecureExpression(comparison(
                EQUAL,
                SYMBOL.toSymbolReference(),
                new Constant(BIGINT, 987654321L)));
        StageInfo stage = StageInfo.createInitial(
                new QueryId("query"),
                RUNNING,
                createFragment(predicate, Optional.of("{\"details\":[\"[REDACTED]\"]}")));

        JsonNode serializedPlan = JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsString(stage)).get("plan");

        assertThat(serializedPlan.has("root")).isTrue();
        assertThat(serializedPlan.has("partitioning")).isTrue();
        assertThat(serializedPlan.get("root").get("id").asText()).isEqualTo("filter");
        assertThat(serializedPlan.get("root").get("source").get("id").asText()).isEqualTo("values");
        assertThat(serializedPlan.get("jsonRepresentation").asText()).contains(SecureExpression.REDACTED);
        assertThat(serializedPlan.toString()).doesNotContain("987654321");
        assertThat(((FilterNode) stage.plan().getRoot()).getPredicate())
                .isEqualTo(new SecureExpression(new Constant(predicate.type(), null)));
    }

    @Test
    public void testReportingFragmentIsCreatedOnceAndExecutionIsUnchanged()
    {
        Expression secure = new SecureExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression userPredicate = comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 123L));
        PlanFragment executable = createFragment(Logical.and(secure, userPredicate), Optional.empty());
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());

        PlanFragment reporting = stateMachine.getStageInfo(ImmutableList::of).plan();
        assertThat(stateMachine.getStageInfo(ImmutableList::of).plan()).isSameAs(reporting);
        assertThat(stateMachine.getFragment()).isSameAs(executable);
        assertThat(((FilterNode) executable.getRoot()).getPredicate()).isEqualTo(Logical.and(secure, userPredicate));
        assertThat(((FilterNode) reporting.getRoot()).getPredicate())
                .isEqualTo(Logical.and(new SecureExpression(new Constant(secure.type(), null)), userPredicate));
    }

    @Test
    public void testReportingFragmentShowsOneMarkerPerLogicalExpression()
            throws Exception
    {
        // Pushdown leaves the policy and the domain derived from it as separate secure conjuncts unless they
        // coincide, so their number would reveal how the policy relates to the user's predicate
        Expression policy = new SecureExpression(comparison(LESS_THAN_OR_EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression derived = new SecureExpression(comparison(LESS_THAN, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        Expression userPredicate = comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 123L));
        Expression marker = new SecureExpression(new Constant(policy.type(), null));

        Expression predicate = new Logical(AND, ImmutableList.of(derived, userPredicate, policy));
        PlanFragment executable = createFragment(predicate, Optional.empty());
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());
        assertThat(((FilterNode) stateMachine.getStageInfo(ImmutableList::of).plan().getRoot()).getPredicate())
                .isEqualTo(Logical.and(marker, userPredicate));
        assertThat(((FilterNode) executable.getRoot()).getPredicate()).isEqualTo(predicate);
        JsonNode reportingJson = JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsString(stateMachine.getStageInfo(ImmutableList::of))).get("plan");
        PlanFragment reported = JSON_MAPPER.treeToValue(reportingJson, PlanFragment.class);
        assertThat(((FilterNode) reported.getRoot()).getPredicate()).isEqualTo(Logical.and(marker, userPredicate));
        PlanFragment worker = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(executable), PlanFragment.class);
        assertThat(((FilterNode) worker.getRoot()).getPredicate()).isEqualTo(predicate);
        assertThat(((FilterNode) StageInfo.createInitial(new QueryId("query"), RUNNING, executable).plan().getRoot()).getPredicate())
                .isEqualTo(Logical.and(marker, userPredicate));

        executable = createFragment(Logical.and(derived, Logical.or(policy, derived)), Optional.empty());
        stateMachine = new StageStateMachine(
                new StageId("query", 0), executable, ImmutableMap.of(), Runnable::run, noopTracer(), Span.getInvalid(), new SplitSchedulerStats());
        assertThat(((FilterNode) stateMachine.getStageInfo(ImmutableList::of).plan().getRoot()).getPredicate()).isEqualTo(marker);
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
        SecureExpression predicate = new SecureExpression(comparison(EQUAL, SYMBOL.toSymbolReference(), new Constant(BIGINT, 987654321L)));
        PlanFragment executable = createFragmentForPlan(new FilterNode(new PlanNodeId("filter"), scan, predicate), Optional.empty());
        StageStateMachine stateMachine = new StageStateMachine(
                new StageId("query", 0),
                executable,
                ImmutableMap.of(scanId, new TableInfo(Optional.of("test"), new QualifiedObjectName("test", "schema", "orders"), TupleDomain.all())),
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
    }

    public record PolicyTableHandle(long policyBound)
            implements ConnectorTableHandle {}

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
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                jsonRepresentation);
    }
}
