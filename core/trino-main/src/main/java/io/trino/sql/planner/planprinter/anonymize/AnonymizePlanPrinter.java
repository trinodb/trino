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

package io.trino.sql.planner.planprinter.anonymize;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import io.trino.execution.StageInfo;
import io.trino.execution.TableInfo;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.sql.planner.TypeProvider.getTypeProvider;
import static java.util.Objects.requireNonNull;

public class AnonymizePlanPrinter
{
    private static final JsonCodec<Map<PlanFragmentId, AnonymizedNodeRepresentation>> ANONYMIZED_PLAN_JSON_CODEC = jsonCodec(new TypeToken<>() {});

    private final PlanNode node;
    private final TypeProvider typeProvider;
    private final Function<TableScanNode, TableInfo> tableInfoSupplier;

    public AnonymizePlanPrinter(PlanNode node, TypeProvider typeProvider, Function<TableScanNode, TableInfo> tableInfoSupplier)
    {
        this.node = requireNonNull(node, "node is null");
        this.typeProvider = requireNonNull(typeProvider, "provider is null");
        this.tableInfoSupplier = requireNonNull(tableInfoSupplier, "tableInfoSupplier is null");
    }

    public static String anonymizedDistributedJsonPlan(Optional<StageInfo> outputStageInfo)
    {
        List<StageInfo> allStages = getAllStages(outputStageInfo);
        TypeProvider typeProvider = getTypeProvider(allStages.stream()
                .map(StageInfo::getPlan)
                .collect(toImmutableList()));
        Map<PlanNodeId, TableInfo> tableInfos = allStages.stream()
                .map(StageInfo::getTables)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<PlanFragmentId, AnonymizedNodeRepresentation> anonymizedPlan = allStages.stream()
                .map(StageInfo::getPlan)
                .filter(Objects::nonNull)
                .collect(toImmutableMap(
                        PlanFragment::getId,
                        plan -> new AnonymizePlanPrinter(
                                plan.getRoot(),
                                typeProvider,
                                tableScanNode -> tableInfos.get(tableScanNode.getId())).generatePlanRepresentation()));
        return ANONYMIZED_PLAN_JSON_CODEC.toJson(anonymizedPlan);
    }

    @VisibleForTesting
    public AnonymizedNodeRepresentation generatePlanRepresentation()
    {
        return node.accept(new Visitor(), null);
    }

    private class Visitor
            extends PlanVisitor<AnonymizedNodeRepresentation, Void>
    {
        @Override
        protected AnonymizedNodeRepresentation visitPlan(PlanNode node, Void context)
        {
            return DefaultNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitRemoteSource(RemoteSourceNode node, Void context)
        {
            return RemoteSourceNodeRepresentation.fromPlanNode(node, typeProvider);
        }

        @Override
        public AnonymizedNodeRepresentation visitAggregation(AggregationNode node, Void context)
        {
            return AggregationNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitJoin(JoinNode node, Void context)
        {
            return JoinNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitTableScan(TableScanNode node, Void context)
        {
            return TableScanNodeRepresentation.fromPlanNode(node, typeProvider, tableInfoSupplier.apply(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitExchange(ExchangeNode node, Void context)
        {
            return ExchangeNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitSemiJoin(SemiJoinNode node, Void context)
        {
            return SemiJoinNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitValues(ValuesNode node, Void context)
        {
            return ValuesNodeRepresentation.fromPlanNode(node, typeProvider);
        }

        @Override
        public AnonymizedNodeRepresentation visitFilter(FilterNode node, Void context)
        {
            return FilterNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitProject(ProjectNode node, Void context)
        {
            return ProjectNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitSort(SortNode node, Void context)
        {
            return SortNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitWindow(WindowNode node, Void context)
        {
            return WindowNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        @Override
        public AnonymizedNodeRepresentation visitTopN(TopNNode node, Void context)
        {
            return TopNNodeRepresentation.fromPlanNode(node, typeProvider, getSourceRepresentations(node));
        }

        private List<AnonymizedNodeRepresentation> getSourceRepresentations(PlanNode node)
        {
            return node.getSources().stream()
                    .map(source -> source.accept(this, null))
                    .collect(toImmutableList());
        }
    }
}
