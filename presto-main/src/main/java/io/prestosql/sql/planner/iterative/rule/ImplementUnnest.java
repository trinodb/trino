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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.unnest.UnnestOperator;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.UnnestingNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FrameBound;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.WindowFrame;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * This rule transforms UnnestNode into a sequence of nodes, which implement Join involving unnest.
 * The implementation depends on the type of Join and Join condition.
 * <p>
 * 1. Trivial Join condition.
 * This case applies to CROSS / IMPLICIT Join which have no join filter
 * and to INNER, LEFT, RIGHT, FULL Join on true.
 * In this case, the rule transforms:
 * <pre>
 * - Unnest (joinType)
 *    - Source
 * </pre>
 * Into:
 * <pre>
 * - Unnesting (joinType, onTrue)
 *    - Source
 * </pre>
 * <p>
 * <p>
 * 2. INNER join with non-trivial condition.
 * <pre>
 * - Unnest (INNER, condition)
 *    - Source
 * </pre>
 * is transformed into:
 * <pre>
 * - Filter (condition)
 *    - Unnesting (INNER, onTrue)
 *       - Source
 * </pre>
 * <p>
 * <p>
 * 3. LEFT join with non-trivial condition.
 * <pre>
 * - Unnest (LEFT, condition)
 *    - Source
 * </pre>
 * is transformed into:
 * <pre>
 * - Project (prune: left_id, marker, max_marker)
 *    - Filter (max_marker = 0 OR marker = 2)
 *       - Window (max_marker := max(marker) partition by left_id)
 *          - Filter (marker = 0 OR condition)
 *             - Unnesting (LEFT, add: marker, onTrue: false)
 *                - AssignUniqueId (left_id)
 *                   - Source
 * </pre>
 * <p>
 * <p>
 * 4. RIGHT join with non-trivial condition.
 * <pre>
 * - Unnest (RIGHT, condition)
 *    - Source
 * </pre>
 * is transformed into:
 * <pre>
 * - Project (prune: right_id, marker, max_marker)
 *    - Filter (max_marker = 1 OR marker = 2)
 *       - Window (max_marker := max(marker) partition by right_id)
 *          - Filter (marker = 1 OR condition)
 *             - Unnesting (RIGHT, add: right_id, marker, onTrue: false)
 *                - Source
 * </pre>
 * <p>
 * <p>
 * 5. FULL join with non-trivial condition.
 * <pre>
 * - Unnest (FULL, condition)
 *    - Source
 * </pre>
 * is transformed into:
 * <pre>
 * - Project (prune: left_id, right_id, marker, max_marker)
 *    - Filter (max_marker = 0 OR max_marker = 1 OR marker = 2)
 *       - Window (max_marker := max(marker) partition by left_id)
 *          - Filter (count = 1 OR marker = 2)
 *             - Window (count := count() partition by right_id)
 *                - Filter (marker = 0 OR marker = 1 OR condition)
 *                   - Unnesting (FULL, add: right_id, marker, onTrue: false)
 *                      - AssignUniqueId (left_id)
 *                         - Source
 * </pre>
 * For the semantics of helper symbols: right_id and marker, see {@link UnnestOperator}
 */
public class ImplementUnnest
        implements Rule<UnnestNode>
{
    private static final Pattern<UnnestNode> PATTERN = unnest();

    private final Metadata metadata;

    public ImplementUnnest(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<UnnestNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(UnnestNode parent, Captures captures, Context context)
    {
        if (!parent.getFilter().isPresent() || parent.getFilter().get().equals(TRUE_LITERAL)) {
            return rewriteOnTrue(parent);
        }
        if (parent.getJoinType() == INNER) {
            return rewriteInner(parent, context);
        }
        if (parent.getJoinType() == LEFT) {
            return rewriteLeft(parent, context);
        }
        if (parent.getJoinType() == RIGHT) {
            return rewriteRight(parent, context);
        }
        return rewriteFull(parent, context);
    }

    private Result rewriteOnTrue(UnnestNode node)
    {
        return Result.ofPlanNode(simpleRewriteOnTrue(node));
    }

    private Result rewriteInner(UnnestNode node, Context context)
    {
        UnnestingNode rewritten = simpleRewriteOnTrue(node);
        FilterNode filter = new FilterNode(
                context.getIdAllocator().getNextId(),
                rewritten,
                node.getFilter().get());

        return Result.ofPlanNode(filter);
    }

    private UnnestingNode simpleRewriteOnTrue(UnnestNode node)
    {
        return new UnnestingNode(
                node.getId(),
                node.getSource(),
                node.getReplicateSymbols(),
                node.getUnnestSymbols(),
                node.getOrdinalitySymbol(),
                Optional.empty(),
                Optional.empty(),
                node.getJoinType(),
                true);
    }

    private Result rewriteLeft(UnnestNode node, Context context)
    {
        // assign unique id on source to track the origin of replicate rows
        Symbol leftId = context.getSymbolAllocator().newSymbol("left_id", BIGINT);
        AssignUniqueId uniqueId = new AssignUniqueId(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                leftId);
        ImmutableList.Builder<Symbol> replicateSymbols = ImmutableList.builder();
        replicateSymbols.addAll(node.getReplicateSymbols())
                .add(leftId);

        // unnesting node adding marker symbol
        // marker = 0 for a row of replicate values completed with nulls
        // marker = 2 for a row of replicate values joined with unnested values
        Symbol marker = context.getSymbolAllocator().newSymbol("marker", BIGINT);
        UnnestingNode unnesting = new UnnestingNode(
                node.getId(),
                uniqueId,
                replicateSymbols.build(),
                node.getUnnestSymbols(),
                node.getOrdinalitySymbol(),
                Optional.empty(),
                Optional.of(marker),
                node.getJoinType(),
                false);

        // filter predicate: (marker = 0 OR joinCondition)
        Expression filterPredicateForJoinCondition = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "0")),
                node.getFilter().get());
        FilterNode filterForJoinCondition = new FilterNode(
                context.getIdAllocator().getNextId(),
                unnesting,
                filterPredicateForJoinCondition);

        // for each original replicate row, find maximum marker value left after the filter
        Symbol maxMarker = context.getSymbolAllocator().newSymbol("max_marker", BIGINT);
        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        WindowNode.Function maxMarkerFunction = new WindowNode.Function(
                metadata.resolveFunction(QualifiedName.of("max"), fromTypes(BIGINT)),
                ImmutableList.of(marker.toSymbolReference()),
                frame,
                false);
        WindowNode window = new WindowNode(
                context.getIdAllocator().getNextId(),
                filterForJoinCondition,
                new WindowNode.Specification(ImmutableList.of(leftId), Optional.empty()),
                ImmutableMap.of(maxMarker, maxMarkerFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // filter to remove the null-completed row for the original replicate row,
        // if any row joined with unnested values passed the joinCondition
        Expression filterPredicateForExtraRows = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, maxMarker.toSymbolReference(), new GenericLiteral("BIGINT", "0")),
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "2")));
        FilterNode filterForExtraRows = new FilterNode(
                context.getIdAllocator().getNextId(),
                window,
                filterPredicateForExtraRows);

        // prune helper symbols: leftId, marker, maxMarker
        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterForExtraRows,
                Assignments.identity(node.getOutputSymbols()));

        return Result.ofPlanNode(project);
    }

    private Result rewriteRight(UnnestNode node, Context context)
    {
        // add unique id to each unnested row to track their origin
        Symbol rightId = context.getSymbolAllocator().newSymbol("right_id", BIGINT);
        // add marker symbol
        // marker = 1 for a row of unnested values completed with nulls
        // marker = 2 for a row of unnested values joined with replicate values
        Symbol marker = context.getSymbolAllocator().newSymbol("marker", BIGINT);
        UnnestingNode unnesting = new UnnestingNode(
                node.getId(),
                node.getSource(),
                node.getReplicateSymbols(),
                node.getUnnestSymbols(),
                node.getOrdinalitySymbol(),
                Optional.of(rightId),
                Optional.of(marker),
                node.getJoinType(),
                false);

        // filter predicate: (marker = 1 OR joinCondition)
        Expression filterPredicateForJoinCondition = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "1")),
                node.getFilter().get());
        FilterNode filterForJoinCondition = new FilterNode(
                context.getIdAllocator().getNextId(),
                unnesting,
                filterPredicateForJoinCondition);

        // for each original unnested row, find maximum marker value left after the filter
        Symbol maxMarker = context.getSymbolAllocator().newSymbol("max_marker", BIGINT);
        WindowNode.Frame frame = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        WindowNode.Function maxMarkerFunction = new WindowNode.Function(
                metadata.resolveFunction(QualifiedName.of("max"), fromTypes(BIGINT)),
                ImmutableList.of(marker.toSymbolReference()),
                frame,
                false);
        WindowNode window = new WindowNode(
                context.getIdAllocator().getNextId(),
                filterForJoinCondition,
                new WindowNode.Specification(ImmutableList.of(rightId), Optional.empty()),
                ImmutableMap.of(maxMarker, maxMarkerFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // filter to remove the null-completed row for the original unnested row,
        // if the row joined with replicate values passed the joinCondition
        Expression filterPredicateForExtraRows = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, maxMarker.toSymbolReference(), new GenericLiteral("BIGINT", "1")),
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "2")));
        FilterNode filterForExtraRows = new FilterNode(
                context.getIdAllocator().getNextId(),
                window,
                filterPredicateForExtraRows);

        // prune helper symbols: rightId, marker, maxMarker
        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterForExtraRows,
                Assignments.identity(node.getOutputSymbols()));

        return Result.ofPlanNode(project);
    }

    private Result rewriteFull(UnnestNode node, Context context)
    {
        // assign unique id on source to track the origin of replicate rows
        Symbol leftId = context.getSymbolAllocator().newSymbol("left_id", BIGINT);
        AssignUniqueId uniqueId = new AssignUniqueId(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                leftId);
        ImmutableList.Builder<Symbol> replicateSymbols = ImmutableList.builder();
        replicateSymbols.addAll(node.getReplicateSymbols())
                .add(leftId);

        // add unique id to each unnested row to track their origin
        Symbol rightId = context.getSymbolAllocator().newSymbol("right_id", BIGINT);
        // add marker symbol
        // marker = 0 for a row of replicate values completed with nulls
        // marker = 1 for a row of unnested values completed with nulls
        // marker = 2 for a row of unnested values joined with replicate values
        Symbol marker = context.getSymbolAllocator().newSymbol("marker", BIGINT);
        UnnestingNode unnesting = new UnnestingNode(
                node.getId(),
                uniqueId,
                replicateSymbols.build(),
                node.getUnnestSymbols(),
                node.getOrdinalitySymbol(),
                Optional.of(rightId),
                Optional.of(marker),
                node.getJoinType(),
                false);

        // filter predicate: (marker = 0 OR marker = 1 OR joinCondition)
        Expression filterPredicateForJoinCondition = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "0")),
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "1")),
                node.getFilter().get());
        FilterNode filterForJoinCondition = new FilterNode(
                context.getIdAllocator().getNextId(),
                unnesting,
                filterPredicateForJoinCondition);

        // for each original unnested row, count resulting rows left after the filter
        Symbol count = context.getSymbolAllocator().newSymbol("count", BIGINT);
        WindowNode.Frame frameForCount = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        WindowNode.Function countFunction = new WindowNode.Function(
                metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of()),
                ImmutableList.of(),
                frameForCount,
                false);
        WindowNode windowForCount = new WindowNode(
                context.getIdAllocator().getNextId(),
                filterForJoinCondition,
                new WindowNode.Specification(ImmutableList.of(rightId), Optional.empty()),
                ImmutableMap.of(count, countFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // filter to remove the null-completed row for the original unnested row,
        // if the row joined with replicate values passed the joinCondition
        Expression filterPredicateForExtraUnnestedRows = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, count.toSymbolReference(), new GenericLiteral("BIGINT", "1")),
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "2")));
        FilterNode filterForExtraUnnestedRows = new FilterNode(
                context.getIdAllocator().getNextId(),
                windowForCount,
                filterPredicateForExtraUnnestedRows);

        // for each original replicate row, find maximum marker value left after the filter
        // NOTE for any unnested row completed with nulls, leftId is also null
        Symbol maxMarker = context.getSymbolAllocator().newSymbol("max_marker", BIGINT);
        WindowNode.Frame frameForMax = new WindowNode.Frame(
                WindowFrame.Type.RANGE,
                FrameBound.Type.UNBOUNDED_PRECEDING,
                Optional.empty(),
                FrameBound.Type.CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        WindowNode.Function maxMarkerFunction = new WindowNode.Function(
                metadata.resolveFunction(QualifiedName.of("max"), fromTypes(BIGINT)),
                ImmutableList.of(marker.toSymbolReference()),
                frameForMax,
                false);
        WindowNode windowForMax = new WindowNode(
                context.getIdAllocator().getNextId(),
                filterForExtraUnnestedRows,
                new WindowNode.Specification(ImmutableList.of(leftId), Optional.empty()),
                ImmutableMap.of(maxMarker, maxMarkerFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        // filter to remove the null-completed row for the original replicate row,
        // if any row joined with unnested values passed the joinCondition
        Expression filterPredicateForExtraReplicateRows = ExpressionUtils.or(
                new ComparisonExpression(EQUAL, maxMarker.toSymbolReference(), new GenericLiteral("BIGINT", "0")),
                new ComparisonExpression(EQUAL, maxMarker.toSymbolReference(), new GenericLiteral("BIGINT", "1")),
                new ComparisonExpression(EQUAL, marker.toSymbolReference(), new GenericLiteral("BIGINT", "2")));
        FilterNode filterForExtraReplicateRows = new FilterNode(
                context.getIdAllocator().getNextId(),
                windowForMax,
                filterPredicateForExtraReplicateRows);

        // prune helper symbols: leftId, rightId, marker, maxMarker
        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterForExtraReplicateRows,
                Assignments.identity(node.getOutputSymbols()));

        return Result.ofPlanNode(project);
    }
}
