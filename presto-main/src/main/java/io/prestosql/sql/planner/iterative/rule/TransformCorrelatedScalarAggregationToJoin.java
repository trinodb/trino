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
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.ScalarAggregationToJoinRewriter;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.matching.Pattern.nonEmpty;
import static io.prestosql.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.prestosql.sql.planner.plan.Patterns.CorrelatedJoin.subquery;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.correlatedJoin;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * Scalar aggregation is aggregation with GROUP BY 'a constant' (or empty GROUP BY).
 * It always returns single row.
 * <p>
 * This optimizer rewrites correlated scalar aggregation subquery to left outer join in a way described here:
 * https://github.com/prestosql/presto/wiki/Correlated-subqueries
 * <p>
 * From:
 * <pre>
 * - CorrelatedJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Aggregation(GROUP BY (); functions: [sum(F), count(), ...]
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Aggregation(GROUP BY A, B, C, U; functions: [sum(F), count(non_null), ...]
 *   - Join(LEFT_OUTER, D = C)
 *     - AssignUniqueId(adds symbol U)
 *       - (input) plan which produces symbols: [A, B, C]
 *     - projection which adds non null symbol used for count() function
 *       - Filter(E > 5)
 *         - plan which produces symbols: [D, E, F]
 * </pre>
 * <p>
 * Note that only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedScalarAggregationToJoin
{
    private final Metadata metadata;

    public TransformCorrelatedScalarAggregationToJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new TransformCorrelatedScalarAggregationToJoin.TransformCorrelatedScalarAggregationWithProjection(metadata),
                new TransformCorrelatedScalarAggregationToJoin.TransformCorrelatedScalarAggregationWithoutProjection(metadata));
    }

    @VisibleForTesting
    static final class TransformCorrelatedScalarAggregationWithProjection
            implements Rule<CorrelatedJoinNode>
    {
        private static final Capture<ProjectNode> PROJECTION = newCapture();
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
                .with(nonEmpty(correlation()))
                .with(filter().equalTo(TRUE_LITERAL))
                .with(subquery().matching(project()
                        .capturedAs(PROJECTION)
                        .with(source().matching(aggregation()
                                .with(empty(groupingColumns()))
                                .capturedAs(AGGREGATION)))));

        private final Metadata metadata;

        @VisibleForTesting
        TransformCorrelatedScalarAggregationWithProjection(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Pattern<CorrelatedJoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
        {
            PlanNode rewrittenNode = new ScalarAggregationToJoinRewriter(metadata, context.getSymbolAllocator(), context.getIdAllocator(), context.getLookup())
                    .rewriteScalarAggregation(correlatedJoinNode, captures.get(AGGREGATION));

            if (rewrittenNode instanceof CorrelatedJoinNode) {
                // Failed to decorrelate subquery
                return Result.empty();
            }

            // Restrict outputs and apply projection
            Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
            List<Symbol> expectedAggregationOutputs = rewrittenNode.getOutputSymbols().stream()
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder()
                    .putIdentities(expectedAggregationOutputs)
                    .putAll(captures.get(PROJECTION).getAssignments())
                    .build();

            return Result.ofPlanNode(new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    rewrittenNode,
                    assignments));
        }
    }

    @VisibleForTesting
    static final class TransformCorrelatedScalarAggregationWithoutProjection
            implements Rule<CorrelatedJoinNode>
    {
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
                .with(nonEmpty(correlation()))
                .with(filter().equalTo(TRUE_LITERAL)) // todo non-trivial join filter: adding filter/project on top of aggregation
                .with(subquery().matching(aggregation()
                        .with(empty(groupingColumns()))
                        .capturedAs(AGGREGATION)));

        private final Metadata metadata;

        @VisibleForTesting
        TransformCorrelatedScalarAggregationWithoutProjection(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Pattern<CorrelatedJoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
        {
            PlanNode rewrittenNode = new ScalarAggregationToJoinRewriter(metadata, context.getSymbolAllocator(), context.getIdAllocator(), context.getLookup())
                    .rewriteScalarAggregation(correlatedJoinNode, captures.get(AGGREGATION));

            if (rewrittenNode instanceof CorrelatedJoinNode) {
                // Failed to decorrelate subquery
                return Result.empty();
            }

            // Restrict outputs
            Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
            List<Symbol> expectedAggregationOutputs = rewrittenNode.getOutputSymbols().stream()
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            return Result.ofPlanNode(new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    rewrittenNode,
                    Assignments.identity(expectedAggregationOutputs)));
        }
    }
}
