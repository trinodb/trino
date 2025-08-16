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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.joins.JoinGraph;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getJoinReorderingStrategy;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.AUTOMATIC;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class EliminateCrossJoins
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // we run this for cost-based reordering also for cases when some of the tables do not have statistics
        JoinReorderingStrategy joinReorderingStrategy = getJoinReorderingStrategy(session);
        return joinReorderingStrategy == ELIMINATE_CROSS_JOINS || joinReorderingStrategy == AUTOMATIC;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        JoinGraph joinGraph = JoinGraph.buildFrom(node, context.getLookup(), context.getIdAllocator());
        if (joinGraph.size() < 3 || !joinGraph.isContainsCrossJoin()) {
            return Result.empty();
        }

        List<Integer> joinOrder = getJoinOrder(joinGraph);
        if (isOriginalOrder(joinOrder)) {
            return Result.empty();
        }

        PlanNode replacement = buildJoinTree(node.getOutputSymbols(), joinGraph, joinOrder, context.getIdAllocator());
        return Result.ofPlanNode(replacement);
    }

    public static boolean isOriginalOrder(List<Integer> joinOrder)
    {
        for (int i = 0; i < joinOrder.size(); i++) {
            if (joinOrder.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given JoinGraph determine the order of joins between graph nodes
     * by traversing JoinGraph. Any graph traversal algorithm could be used
     * here (like BFS or DFS), but we use PriorityQueue to preserve
     * original JoinOrder as mush as it is possible. PriorityQueue returns
     * next nodes to join in order of their occurrence in original Plan.
     */
    public static List<Integer> getJoinOrder(JoinGraph graph)
    {
        ImmutableList.Builder<PlanNode> joinOrder = ImmutableList.builder();

        Map<PlanNodeId, Integer> priorities = new HashMap<>();
        for (int i = 0; i < graph.size(); i++) {
            priorities.put(graph.getNode(i).getId(), i);
        }

        PriorityQueue<PlanNode> nodesToVisit = new PriorityQueue<>(
                graph.size(),
                comparing(node -> priorities.get(node.getId())));
        Set<PlanNode> visited = new HashSet<>();

        nodesToVisit.add(graph.getNode(0));

        while (!nodesToVisit.isEmpty()) {
            PlanNode node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                joinOrder.add(node);
                for (JoinGraph.Edge edge : graph.getEdges(node)) {
                    nodesToVisit.add(edge.getTargetNode());
                }
            }

            if (nodesToVisit.isEmpty() && visited.size() < graph.size()) {
                // disconnected graph, find new starting point
                Optional<PlanNode> firstNotVisitedNode = graph.getNodes().stream()
                        .filter(graphNode -> !visited.contains(graphNode))
                        .findFirst();
                firstNotVisitedNode.ifPresent(nodesToVisit::add);
            }
        }

        checkState(visited.size() == graph.size());
        return joinOrder.build().stream()
                .map(node -> priorities.get(node.getId()))
                .collect(toImmutableList());
    }

    public static PlanNode buildJoinTree(List<Symbol> expectedOutputSymbols, JoinGraph graph, List<Integer> joinOrder, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(expectedOutputSymbols, "expectedOutputSymbols is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(graph, "graph is null");
        joinOrder = ImmutableList.copyOf(requireNonNull(joinOrder, "joinOrder is null"));
        checkArgument(joinOrder.size() >= 2);

        PlanNode result = graph.getNode(joinOrder.get(0));
        Set<PlanNodeId> alreadyJoinedNodes = new HashSet<>();
        alreadyJoinedNodes.add(result.getId());

        for (int i = 1; i < joinOrder.size(); i++) {
            PlanNode rightNode = graph.getNode(joinOrder.get(i));
            alreadyJoinedNodes.add(rightNode.getId());

            ImmutableList.Builder<JoinNode.EquiJoinClause> criteria = ImmutableList.builder();

            for (JoinGraph.Edge edge : graph.getEdges(rightNode)) {
                PlanNode targetNode = edge.getTargetNode();
                if (alreadyJoinedNodes.contains(targetNode.getId())) {
                    criteria.add(new JoinNode.EquiJoinClause(
                            edge.getTargetSymbol(),
                            edge.getSourceSymbol()));
                }
            }

            result = new JoinNode(
                    idAllocator.getNextId(),
                    JoinType.INNER,
                    result,
                    rightNode,
                    criteria.build(),
                    result.getOutputSymbols(),
                    rightNode.getOutputSymbols(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty());
        }

        List<Expression> filters = graph.getFilters();

        for (Expression filter : filters) {
            result = new FilterNode(
                    idAllocator.getNextId(),
                    result,
                    filter);
        }

        // If needed, introduce a projection to constrain the outputs to what was originally expected
        // Some nodes are sensitive to what's produced (e.g., DistinctLimit node)
        return restrictOutputs(idAllocator, result, ImmutableSet.copyOf(expectedOutputSymbols)).orElse(result);
    }
}
