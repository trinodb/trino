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
package io.trino.sql.planner.optimizations.joins;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.Objects.requireNonNull;

/**
 * JoinGraph represents sequence of Joins, where nodes in the graph
 * are PlanNodes that are being joined and edges are all equality join
 * conditions between pair of nodes.
 */
public class JoinGraph
{
    private final List<Expression> filters;
    private final List<PlanNode> nodes; // nodes in order of their appearance in tree plan (left, right, parent)
    private final Multimap<PlanNodeId, Edge> edges;
    private final PlanNodeId rootId;
    private final boolean containsCrossJoin;

    /**
     * Builds {@link JoinGraph} containing {@code plan} node.
     */
    public static JoinGraph buildFrom(
            Metadata metadata,
            PlanNode plan,
            Lookup lookup,
            PlanNodeIdAllocator planNodeIdAllocator,
            Session session,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types)
    {
        return plan.accept(new Builder(metadata, lookup, planNodeIdAllocator, session, typeAnalyzer, types), new Context());
    }

    public JoinGraph(PlanNode node)
    {
        this(ImmutableList.of(node), ImmutableMultimap.of(), node.getId(), ImmutableList.of(), false);
    }

    public JoinGraph(
            List<PlanNode> nodes,
            Multimap<PlanNodeId, Edge> edges,
            PlanNodeId rootId,
            List<Expression> filters,
            boolean containsCrossJoin)
    {
        this.nodes = nodes;
        this.edges = edges;
        this.rootId = rootId;
        this.filters = filters;
        this.containsCrossJoin = containsCrossJoin;
    }

    public JoinGraph withFilter(Expression expression)
    {
        ImmutableList.Builder<Expression> filters = ImmutableList.builder();
        filters.addAll(this.filters);
        filters.add(expression);

        return new JoinGraph(nodes, edges, rootId, filters.build(), containsCrossJoin);
    }

    public List<Expression> getFilters()
    {
        return filters;
    }

    public PlanNodeId getRootId()
    {
        return rootId;
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public int size()
    {
        return nodes.size();
    }

    public PlanNode getNode(int index)
    {
        return nodes.get(index);
    }

    public List<PlanNode> getNodes()
    {
        return nodes;
    }

    public Collection<Edge> getEdges(PlanNode node)
    {
        return ImmutableList.copyOf(edges.get(node.getId()));
    }

    public boolean isContainsCrossJoin()
    {
        return containsCrossJoin;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (PlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(" = ")
                    .append(nodeFrom.toString())
                    .append("\n");
        }
        for (PlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(":");
            for (Edge nodeTo : edges.get(nodeFrom.getId())) {
                builder.append(" ").append(nodeTo.getTargetNode().getId());
            }
            builder.append("\n");
        }

        return builder.toString();
    }

    private JoinGraph joinWith(JoinGraph other, List<JoinNode.EquiJoinClause> joinClauses, Context context, PlanNodeId newRoot, boolean containsCrossJoin)
    {
        for (PlanNode node : other.nodes) {
            checkState(!edges.containsKey(node.getId()), "Node [%s] appeared in two JoinGraphs", node);
        }

        List<PlanNode> nodes = ImmutableList.<PlanNode>builder()
                .addAll(this.nodes)
                .addAll(other.nodes)
                .build();

        ImmutableMultimap.Builder<PlanNodeId, Edge> edges = ImmutableMultimap.<PlanNodeId, Edge>builder()
                .putAll(this.edges)
                .putAll(other.edges);

        List<Expression> joinedFilters = ImmutableList.<Expression>builder()
                .addAll(this.filters)
                .addAll(other.filters)
                .build();

        for (JoinNode.EquiJoinClause edge : joinClauses) {
            Symbol leftSymbol = edge.getLeft();
            Symbol rightSymbol = edge.getRight();
            checkState(context.containsSymbol(leftSymbol));
            checkState(context.containsSymbol(rightSymbol));

            PlanNode left = context.getSymbolSource(leftSymbol);
            PlanNode right = context.getSymbolSource(rightSymbol);
            edges.put(left.getId(), new Edge(right, leftSymbol, rightSymbol));
            edges.put(right.getId(), new Edge(left, rightSymbol, leftSymbol));
        }

        return new JoinGraph(nodes, edges.build(), newRoot, joinedFilters, this.containsCrossJoin || containsCrossJoin);
    }

    private static class Builder
            extends PlanVisitor<JoinGraph, Context>
    {
        private final Metadata metadata;
        private final Lookup lookup;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final Session session;
        private final TypeAnalyzer typeAnalyzer;
        private final TypeProvider types;

        private Builder(Metadata metadata, Lookup lookup, PlanNodeIdAllocator planNodeIdAllocator, Session session, TypeAnalyzer typeAnalyzer, TypeProvider types)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.lookup = requireNonNull(lookup, "lookup cannot be null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.session = requireNonNull(session, "session is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected JoinGraph visitPlan(PlanNode node, Context context)
        {
            for (Symbol symbol : node.getOutputSymbols()) {
                context.setSymbolSource(symbol, node);
            }
            return new JoinGraph(node);
        }

        @Override
        public JoinGraph visitFilter(FilterNode node, Context context)
        {
            JoinGraph graph = node.getSource().accept(this, context);
            return graph.withFilter(node.getPredicate());
        }

        @Override
        public JoinGraph visitJoin(JoinNode node, Context context)
        {
            //TODO: add support for non inner joins
            if (node.getType() != INNER) {
                return visitPlan(node, context);
            }

            JoinGraph left = node.getLeft().accept(this, context);
            JoinGraph right = node.getRight().accept(this, context);

            JoinGraph graph = left.joinWith(right, node.getCriteria(), context, node.getId(), node.isCrossJoin());

            if (node.getFilter().isPresent()) {
                return graph.withFilter(node.getFilter().get());
            }
            return graph;
        }

        @Override
        public JoinGraph visitProject(ProjectNode node, Context context)
        {
            Optional<PlanNode> rewrittenNode = pushProjectionThroughJoin(metadata, node, lookup, planNodeIdAllocator, session, typeAnalyzer, types);
            if (rewrittenNode.isPresent()) {
                return rewrittenNode.get().accept(this, context);
            }

            return visitPlan(node, context);
        }

        @Override
        public JoinGraph visitGroupReference(GroupReference node, Context context)
        {
            PlanNode dereferenced = lookup.resolve(node);
            JoinGraph graph = dereferenced.accept(this, context);
            if (graph.nodes.size() == 1) {
                return replacementGraph(getOnlyElement(graph.nodes), node, context);
            }
            return graph;
        }

        private JoinGraph replacementGraph(PlanNode oldNode, PlanNode newNode, Context context)
        {
            // TODO optimize when idea is generally approved
            List<Symbol> symbols = context.symbolSources.entrySet().stream()
                    .filter(entry -> entry.getValue() == oldNode)
                    .map(Map.Entry::getKey)
                    .collect(toImmutableList());
            symbols.forEach(symbol -> context.symbolSources.put(symbol, newNode));

            return new JoinGraph(newNode);
        }
    }

    public static class Edge
    {
        private final PlanNode targetNode;
        private final Symbol sourceSymbol;
        private final Symbol targetSymbol;

        public Edge(PlanNode targetNode, Symbol sourceSymbol, Symbol targetSymbol)
        {
            this.targetNode = requireNonNull(targetNode, "targetNode is null");
            this.sourceSymbol = requireNonNull(sourceSymbol, "sourceSymbol is null");
            this.targetSymbol = requireNonNull(targetSymbol, "targetSymbol is null");
        }

        public PlanNode getTargetNode()
        {
            return targetNode;
        }

        public Symbol getSourceSymbol()
        {
            return sourceSymbol;
        }

        public Symbol getTargetSymbol()
        {
            return targetSymbol;
        }
    }

    private static class Context
    {
        private final Map<Symbol, PlanNode> symbolSources = new HashMap<>();

        public void setSymbolSource(Symbol symbol, PlanNode node)
        {
            symbolSources.put(symbol, node);
        }

        public boolean containsSymbol(Symbol symbol)
        {
            return symbolSources.containsKey(symbol);
        }

        public PlanNode getSymbolSource(Symbol symbol)
        {
            checkState(containsSymbol(symbol));
            return symbolSources.get(symbol);
        }
    }
}
