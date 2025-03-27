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
package io.trino.sql.planner.planprinter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Partitioning.ArgumentBinding;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.planprinter.PlanPrinter.formatAggregation;
import static java.lang.String.format;

public final class GraphvizPrinter
{
    private enum NodeType
    {
        EXCHANGE,
        AGGREGATE,
        FILTER,
        PROJECT,
        TOPN,
        OUTPUT,
        LIMIT,
        TABLESCAN,
        VALUES,
        JOIN,
        SINK,
        WINDOW,
        UNION,
        SORT,
        SAMPLE,
        MARK_DISTINCT,
        TABLE_WRITER,
        TABLE_FINISH,
        INDEX_SOURCE,
        UNNEST,
        ANALYZE_FINISH,
        DYNAMIC_FILTER_SOURCE
    }

    private static final Map<NodeType, String> NODE_COLORS = immutableEnumMap(ImmutableMap.<NodeType, String>builder()
            .put(NodeType.EXCHANGE, "gold")
            .put(NodeType.AGGREGATE, "chartreuse3")
            .put(NodeType.FILTER, "yellow")
            .put(NodeType.PROJECT, "bisque")
            .put(NodeType.TOPN, "darksalmon")
            .put(NodeType.OUTPUT, "white")
            .put(NodeType.LIMIT, "gray83")
            .put(NodeType.TABLESCAN, "deepskyblue")
            .put(NodeType.VALUES, "deepskyblue")
            .put(NodeType.JOIN, "orange")
            .put(NodeType.SORT, "aliceblue")
            .put(NodeType.SINK, "indianred1")
            .put(NodeType.WINDOW, "darkolivegreen4")
            .put(NodeType.UNION, "turquoise4")
            .put(NodeType.MARK_DISTINCT, "violet")
            .put(NodeType.TABLE_WRITER, "cyan")
            .put(NodeType.TABLE_FINISH, "hotpink")
            .put(NodeType.INDEX_SOURCE, "dodgerblue3")
            .put(NodeType.UNNEST, "crimson")
            .put(NodeType.SAMPLE, "goldenrod4")
            .put(NodeType.ANALYZE_FINISH, "plum")
            .put(NodeType.DYNAMIC_FILTER_SOURCE, "magenta")
            .buildOrThrow());

    static {
        checkState(NODE_COLORS.size() == NodeType.values().length);
    }

    private GraphvizPrinter() {}

    public static String printLogical(List<PlanFragment> fragments)
    {
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph logical_plan {\n");

        for (PlanFragment fragment : fragments) {
            printFragmentNodes(output, fragment, idGenerator);
        }

        for (PlanFragment fragment : fragments) {
            fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);
        }

        output.append("}\n");

        return output.toString();
    }

    public static String printDistributed(SubPlan plan)
    {
        List<PlanFragment> fragments = plan.getAllFragments();
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph distributed_plan {\n");

        printSubPlan(plan, fragmentsById, idGenerator, output);

        output.append("}\n");

        return output.toString();
    }

    private static void printSubPlan(SubPlan plan, Map<PlanFragmentId, PlanFragment> fragmentsById, PlanNodeIdGenerator idGenerator, StringBuilder output)
    {
        PlanFragment fragment = plan.getFragment();
        printFragmentNodes(output, fragment, idGenerator);
        fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);

        for (SubPlan child : plan.getChildren()) {
            printSubPlan(child, fragmentsById, idGenerator, output);
        }
    }

    private static void printFragmentNodes(StringBuilder output, PlanFragment fragment, PlanNodeIdGenerator idGenerator)
    {
        String clusterId = "cluster_" + fragment.getId();
        output.append("subgraph ")
                .append(clusterId)
                .append(" {")
                .append('\n');

        output.append(format("label = \"%s\"", fragment.getPartitioning()))
                .append('\n');

        PlanNode plan = fragment.getRoot();
        plan.accept(new NodePrinter(output, idGenerator), null);

        output.append("}")
                .append('\n');
    }

    private static class NodePrinter
            extends PlanVisitor<Void, Void>
    {
        private static final int MAX_NAME_WIDTH = 100;
        private final StringBuilder output;
        private final PlanNodeIdGenerator idGenerator;

        public NodePrinter(StringBuilder output, PlanNodeIdGenerator idGenerator)
        {
            this.output = output;
            this.idGenerator = idGenerator;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException(format("Node %s does not have a Graphviz visitor", node.getClass().getName()));
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            List<String> columns = new ArrayList<>();
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                columns.add(node.getColumnNames().get(i) + " := " + node.getColumns().get(i));
            }
            printNode(node, format("TableWriter[%s]", Joiner.on(", ").join(columns)), NODE_COLORS.get(NodeType.TABLE_WRITER));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            printNode(node, format("StatisticsWriterNode[%s]", Joiner.on(", ").join(node.getOutputSymbols())), NODE_COLORS.get(NodeType.ANALYZE_FINISH));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            printNode(node, format("TableFinish[%s]", Joiner.on(", ").join(node.getOutputSymbols())), NODE_COLORS.get(NodeType.TABLE_FINISH));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            printNode(node, format("Sample[type=%s, ratio=%f]", node.getSampleType(), node.getSampleRatio()), NODE_COLORS.get(NodeType.SAMPLE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            printNode(node, format("Sort[%s]", Joiner.on(", ").join(node.getOrderingScheme().orderBy())), NODE_COLORS.get(NodeType.SORT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            printNode(node, format("MarkDistinct[%s]", node.getMarkerSymbol()), format("%s => %s", node.getDistinctSymbols(), node.getMarkerSymbol()), NODE_COLORS.get(NodeType.MARK_DISTINCT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            printNode(node, "Window", format("partition by = %s|order by = %s",
                    Joiner.on(", ").join(node.getPartitionBy()),
                    node.getOrderingScheme()
                            .map(orderingScheme -> Joiner.on(", ").join(orderingScheme.orderBy()))
                            .orElse("")),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitPatternRecognition(PatternRecognitionNode node, Void context)
        {
            printNode(node, "PatternRecognition", format("partition by = %s|order by = %s",
                    Joiner.on(", ").join(node.getPartitionBy()),
                    node.getOrderingScheme()
                            .map(orderingScheme -> Joiner.on(", ").join(orderingScheme.orderBy()))
                            .orElse("")),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Void context)
        {
            printNode(node,
                    "RowNumber",
                    format("partition by = %s", Joiner.on(", ").join(node.getPartitionBy())),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopNRanking(TopNRankingNode node, Void context)
        {
            printNode(node,
                    "TopNRanking",
                    format("type=%s|partition by = %s|order by = %s|n = %s",
                            node.getRankingType(),
                            Joiner.on(", ").join(node.getPartitionBy()),
                            Joiner.on(", ").join(node.getOrderingScheme().orderBy()),
                            node.getMaxRankingPerPartition()),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            printNode(node, "Union", NODE_COLORS.get(NodeType.UNION));

            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            printNode(node, (node.getOrderingScheme().isPresent() ? "Merge" : "Exchange") + " 1:N", NODE_COLORS.get(NodeType.EXCHANGE));
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            List<ArgumentBinding> symbols = node.getOutputSymbols().stream()
                    .map(Symbol::toSymbolReference)
                    .map(ArgumentBinding::expressionBinding)
                    .collect(toImmutableList());
            if (node.getType() == REPARTITION) {
                symbols = node.getPartitioningScheme().getPartitioning().getArguments();
            }
            String columns = Joiner.on(", ").join(symbols);
            printNode(node, format("ExchangeNode[%s]", node.getType()), columns, NODE_COLORS.get(NodeType.EXCHANGE));
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                builder.append(format("%s := %s\\n", entry.getKey(), formatAggregation(new NoOpAnonymizer(), entry.getValue())));
            }
            printNode(node, format("Aggregate[%s]", node.getStep()), builder.toString(), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Void context)
        {
            // grouping sets are easier to understand in terms of inputs
            List<String> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> "(" + Joiner.on(", ").join(set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList())) + ")")
                    .collect(Collectors.toList());

            printNode(node, "GroupId", Joiner.on(", ").join(inputGroupingSetSymbols), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            String expression = node.getPredicate().toString();
            printNode(node, "Filter", expression, NODE_COLORS.get(NodeType.FILTER));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                if ((entry.getValue() instanceof Reference) &&
                        ((Reference) entry.getValue()).name().equals(entry.getKey().name())) {
                    // skip identity assignments
                    continue;
                }
                builder.append(format("%s := %s\\n", entry.getKey(), entry.getValue()));
            }

            printNode(node, "Project", builder.toString(), NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUnnest(UnnestNode node, Void context)
        {
            StringBuilder label = new StringBuilder();
            if (node.getJoinType() == INNER) {
                if (node.getReplicateSymbols().isEmpty()) {
                    label.append("Unnest");
                }
                else {
                    label.append("CrossJoin Unnest");
                }
            }
            else {
                label.append(node.getJoinType().getJoinLabel())
                        .append(" Unnest on true");
            }

            List<Symbol> unnestInputs = node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .collect(toImmutableList());

            label.append(format(" [%s", unnestInputs))
                    .append(node.getOrdinalitySymbol().isPresent() ? " (ordinality)]" : "]");

            printNode(node, label.toString(), "", NODE_COLORS.get(NodeType.UNNEST));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            String keys = node.getOrderingScheme()
                    .orderBy().stream()
                    .map(input -> input + " " + node.getOrderingScheme().ordering(input))
                    .collect(Collectors.joining(", "));

            printNode(node, format("TopN[%s]", node.getCount()), keys, NODE_COLORS.get(NodeType.TOPN));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            String columns = getColumns(node);
            printNode(node, format("Output[%s]", columns), NODE_COLORS.get(NodeType.OUTPUT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            printNode(node, format("DistinctLimit[%s]", node.getLimit()), NODE_COLORS.get(NodeType.LIMIT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            printNode(node, format("Limit[%s]", node.getCount()), NODE_COLORS.get(NodeType.LIMIT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            printNode(node, format("TableScan[%s]", node.getTable()), NODE_COLORS.get(NodeType.TABLESCAN));
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            printNode(node, "Values", NODE_COLORS.get(NodeType.TABLESCAN));
            return null;
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            printNode(node, "Scalar", NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(clause.toExpression());
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            printNode(node, node.getType().getJoinLabel(), criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            printNode(node, "SemiJoin", format("%s = %s", node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol()), NODE_COLORS.get(NodeType.JOIN));

            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            printNode(node, node.getType().getJoinLabel(), node.getFilter().toString(), NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            String parameters = Joiner.on(",").join(node.getCorrelation());
            printNode(node, "Apply", parameters, NODE_COLORS.get(NodeType.JOIN));

            node.getInput().accept(this, context);
            node.getSubquery().accept(this, context);

            return null;
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            printNode(node, "AssignUniqueId", NODE_COLORS.get(NodeType.PROJECT));
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitCorrelatedJoin(CorrelatedJoinNode node, Void context)
        {
            String correlationSymbols = Joiner.on(",").join(node.getCorrelation());
            String filterExpression = "";
            if (!node.getFilter().equals(TRUE)) {
                filterExpression = " " + node.getFilter().toString();
            }

            printNode(node, "CorrelatedJoin", correlationSymbols + filterExpression, NODE_COLORS.get(NodeType.JOIN));

            node.getInput().accept(this, context);
            node.getSubquery().accept(this, context);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            printNode(node, format("IndexSource[%s]", node.getIndexHandle()), NODE_COLORS.get(NodeType.INDEX_SOURCE));
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new Comparison(Comparison.Operator.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            String joinLabel = format("%sIndexJoin", node.getType().getJoinLabel());
            printNode(node, joinLabel, criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitDynamicFilterSource(DynamicFilterSourceNode node, Void context)
        {
            printNode(node, "DynamicFilterSource", NODE_COLORS.get(NodeType.DYNAMIC_FILTER_SOURCE));
            return node.getSource().accept(this, context);
        }

        private void printNode(PlanNode node, String label, String color)
        {
            String nodeId = idGenerator.getNodeId(node);
            label = escapeSpecialCharacters(label);
            output.append(nodeId)
                    .append(format("[label=\"{%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, color))
                    .append(';')
                    .append('\n');
        }

        private void printNode(PlanNode node, String label, String details, String color)
        {
            if (details.isEmpty()) {
                printNode(node, label, color);
            }
            else {
                String nodeId = idGenerator.getNodeId(node);
                label = escapeSpecialCharacters(label);
                details = escapeSpecialCharacters(details);
                output.append(nodeId)
                        .append(format("[label=\"{%s|%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, details, color))
                        .append(';')
                        .append('\n');
            }
        }

        private static String getColumns(OutputNode node)
        {
            Iterator<String> columnNames = node.getColumnNames().iterator();
            String columns = "";
            int nameWidth = 0;
            while (columnNames.hasNext()) {
                String columnName = columnNames.next();
                columns += columnName;
                nameWidth += columnName.length();
                if (columnNames.hasNext()) {
                    columns += ", ";
                }
                if (nameWidth >= MAX_NAME_WIDTH) {
                    columns += "\\n";
                    nameWidth = 0;
                }
            }
            return columns;
        }

        /**
         * Escape characters that are special to graphviz.
         */
        private static String escapeSpecialCharacters(String label)
        {
            return label
                    .replace("<", "\\<")
                    .replace(">", "\\>")
                    .replace("\"", "\\\"");
        }
    }

    private static class EdgePrinter
            extends PlanVisitor<Void, Void>
    {
        private final StringBuilder output;
        private final Map<PlanFragmentId, PlanFragment> fragmentsById;
        private final PlanNodeIdGenerator idGenerator;

        public EdgePrinter(StringBuilder output, Map<PlanFragmentId, PlanFragment> fragmentsById, PlanNodeIdGenerator idGenerator)
        {
            this.output = output;
            this.fragmentsById = ImmutableMap.copyOf(fragmentsById);
            this.idGenerator = idGenerator;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                printEdge(node, child);

                child.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            for (PlanFragmentId planFragmentId : node.getSourceFragmentIds()) {
                PlanFragment target = fragmentsById.get(planFragmentId);
                printEdge(node, target.getRoot());
            }

            return null;
        }

        private void printEdge(PlanNode from, PlanNode to)
        {
            String fromId = idGenerator.getNodeId(from);
            String toId = idGenerator.getNodeId(to);

            output.append(fromId)
                    .append(" -> ")
                    .append(toId)
                    .append(';')
                    .append('\n');
        }
    }

    private static class PlanNodeIdGenerator
    {
        private final Map<PlanNode, Integer> planNodeIds;
        private int idCount;

        public PlanNodeIdGenerator()
        {
            planNodeIds = new HashMap<>();
        }

        public String getNodeId(PlanNode from)
        {
            int nodeId;

            if (planNodeIds.containsKey(from)) {
                nodeId = planNodeIds.get(from);
            }
            else {
                idCount++;
                planNodeIds.put(from, idCount);
                nodeId = idCount;
            }
            return "plannode_" + nodeId;
        }
    }
}
