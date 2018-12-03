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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Rule.Context;
import io.prestosql.sql.planner.optimizations.SymbolMapper;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.Join.type;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topN;
import static io.prestosql.sql.planner.plan.TopNNode.Step.PARTIAL;

/**
 * Transforms:
 * <pre>
 * - TopN
 *    - [Project]
 *       - Join
 *          - left source
 *          - right source
 * </pre>
 * Into:
 * <pre>
 * - TopN
 *    - [Project]
 *       - Join
 *          - TopN (present if Join is left or outer, not already limited, and orderBy symbols come from left source)
 *             - left source
 *          - TopN (present if Join is right or outer, not already limited, and orderBy symbols come from right source)
 *             - right source
 * </pre>
 */
public final class PushTopNThroughOuterJoinRuleSet
{
    private PushTopNThroughOuterJoinRuleSet() {}

    public static Set<Rule<?>> rules()
    {
        return ImmutableSet.of(withProject(), withoutProject());
    }

    @VisibleForTesting
    static Rule<?> withProject()
    {
        return new PushTopNThroughOuterJoinWithProject();
    }

    @VisibleForTesting
    static Rule<?> withoutProject()
    {
        return new PushTopNThroughOuterJoinWithoutProject();
    }

    private static class PushTopNThroughOuterJoinWithProject
            implements Rule<TopNNode>
    {
        private static final Capture<ProjectNode> PROJECT_CHILD = newCapture();
        private static final Capture<JoinNode> JOIN_CHILD = newCapture();

        private static final Pattern<TopNNode> PATTERN_WITH_PROJECT =
                topN().with(source().matching(
                        project().capturedAs(PROJECT_CHILD).with(source().matching(
                                join().capturedAs(JOIN_CHILD).with(type().matching(type -> isLeftOrFullOuter(type) || isRightOrFullOuter(type)))))));

        @Override
        public Pattern<TopNNode> getPattern()
        {
            return PATTERN_WITH_PROJECT;
        }

        @Override
        public Result apply(TopNNode parent, Captures captures, Context context)
        {
            ProjectNode projectNode = captures.get(PROJECT_CHILD);
            JoinNode joinNode = captures.get(JOIN_CHILD);

            Optional<SymbolMapper> symbolMapper = symbolMapper(parent.getOrderingScheme().getOrderBy(), projectNode.getAssignments());
            if (!symbolMapper.isPresent()) {
                return Result.empty();
            }

            TopNNode mappedTopN = symbolMapper.get().map(parent, joinNode, context.getIdAllocator().getNextId()).withStep(PARTIAL);

            Optional<JoinNode> joinNodeWithTopNSource = joinNodeWithTopNSource(mappedTopN, joinNode, context);
            return joinNodeWithTopNSource
                    .map(newJoinNode ->
                            Result.ofPlanNode(
                                    parent.replaceChildren(ImmutableList.of(
                                            projectNode.replaceChildren(ImmutableList.of(
                                                    newJoinNode))))))
                    .orElseGet(Result::empty);
        }

        private Optional<SymbolMapper> symbolMapper(List<Symbol> symbols, Assignments assignments)
        {
            SymbolMapper.Builder mapper = SymbolMapper.builder();
            for (Symbol symbol : symbols) {
                Expression expression = assignments.get(symbol);
                if (!(expression instanceof SymbolReference)) {
                    return Optional.empty();
                }
                mapper.put(symbol, Symbol.from(expression));
            }
            return Optional.of(mapper.build());
        }
    }

    private static class PushTopNThroughOuterJoinWithoutProject
            implements Rule<TopNNode>
    {
        private static final Capture<JoinNode> JOIN_CHILD = newCapture();

        private static final Pattern<TopNNode> PATTERN_WITHOUT_PROJECT =
                topN().with(source().matching(
                        join().capturedAs(JOIN_CHILD).with(type().matching(type -> isLeftOrFullOuter(type) || isRightOrFullOuter(type)))));

        @Override
        public Pattern<TopNNode> getPattern()
        {
            return PATTERN_WITHOUT_PROJECT;
        }

        @Override
        public Result apply(TopNNode parent, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(JOIN_CHILD);
            TopNNode pushdownTopN = new TopNNode(context.getIdAllocator().getNextId(), joinNode, parent.getCount(), parent.getOrderingScheme(), PARTIAL);

            Optional<JoinNode> joinNodeWithTopNSource = joinNodeWithTopNSource(pushdownTopN, joinNode, context);
            return joinNodeWithTopNSource
                    .map(newJoinNode ->
                            Result.ofPlanNode(
                                    parent.replaceChildren(ImmutableList.of(
                                            newJoinNode))))
                    .orElseGet(Result::empty);
        }
    }

    private static Optional<JoinNode> joinNodeWithTopNSource(TopNNode topNNode, JoinNode joinNode, Context context)
    {
        List<Symbol> orderBySymbols = topNNode.getOrderingScheme().getOrderBy();
        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();

        if (isLeftOrFullOuter(joinNode.getType())
                && ImmutableSet.copyOf(left.getOutputSymbols()).containsAll(orderBySymbols)
                && !isLimited(left, context.getLookup(), topNNode.getCount())) {
            return Optional.of(
                    joinNode.replaceChildren(ImmutableList.of(
                            topNNode.replaceChildren(ImmutableList.of(left)),
                            right)));
        }

        if (isRightOrFullOuter(joinNode.getType())
                && ImmutableSet.copyOf(right.getOutputSymbols()).containsAll(orderBySymbols)
                && !isLimited(right, context.getLookup(), topNNode.getCount())) {
            return Optional.of(
                    joinNode.replaceChildren(ImmutableList.of(
                            left,
                            topNNode.replaceChildren(ImmutableList.of(right)))));
        }

        return Optional.empty();
    }

    private static boolean isLimited(PlanNode node, Lookup lookup, long limit)
    {
        Range<Long> cardinality = extractCardinality(node, lookup);
        return cardinality.hasUpperBound() && cardinality.upperEndpoint() <= limit;
    }

    private static boolean isLeftOrFullOuter(JoinNode.Type type)
    {
        return type == LEFT || type == FULL;
    }

    private static boolean isRightOrFullOuter(JoinNode.Type type)
    {
        return type == RIGHT || type == FULL;
    }
}
