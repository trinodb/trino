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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.plan.JoinType.ASOF;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.Join.type;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites an ASOF join into:
 * AssignUniqueId(left) -> AssignUniqueId(right) -> LEFT JOIN (with original equi-criteria and inequality)
 * -> Window(row_number() PARTITION BY left_uid ORDER BY right_ts DESC NULLS LAST, right_uid DESC NULLS LAST)
 * -> Filter(row_number = 1)
 * -> Project(original outputs)
 */
public class RewriteAsofJoinToLeftJoinWithTop1
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().with(type().equalTo(ASOF));

    private final PlannerContext plannerContext;

    public RewriteAsofJoinToLeftJoinWithTop1(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        // Validate equi-criteria
        if (node.getCriteria().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "ASOF join requires at least one equi-join criterion");
        }

        // Extract and validate non-equi inequality predicate
        Comparison inequality = extractSupportedInequality(node)
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "ASOF join requires a single supported inequality predicate"));

        // Determine which side provides the right-side timestamp symbol and ensure orderable
        RightOrderingColumns orderingColumns = resolveOrderingColumns(node, inequality);
        if (!orderingColumns.rightTs.type().isOrderable()) {
            throw new TrinoException(NOT_SUPPORTED, "ASOF join inequality requires an orderable type on the build side");
        }

        // Assign unique ids for partitioning and deterministic tie-breaking
        Symbol leftUid = context.getSymbolAllocator().newSymbol("asof_left_uid", BIGINT);
        PlanNode leftWithUid = new AssignUniqueId(context.getIdAllocator().getNextId(), node.getLeft(), leftUid);

        Symbol rightUid = context.getSymbolAllocator().newSymbol("asof_right_uid", BIGINT);
        PlanNode rightWithUid = new AssignUniqueId(context.getIdAllocator().getNextId(), node.getRight(), rightUid);

        // Build LEFT join preserving original equi-criteria and filter
        List<Symbol> newLeftOutputs = ImmutableList.<Symbol>builder()
                .addAll(node.getLeftOutputSymbols())
                .add(leftUid)
                .build();

        // Ensure right outputs include the timestamp used for ordering and right uid
        ImmutableList.Builder<Symbol> rightOutputsBuilder = ImmutableList.<Symbol>builder()
                .addAll(node.getRightOutputSymbols());
        if (!node.getRightOutputSymbols().contains(orderingColumns.rightTs)) {
            rightOutputsBuilder.add(orderingColumns.rightTs);
        }
        rightOutputsBuilder.add(rightUid);
        List<Symbol> newRightOutputs = rightOutputsBuilder.build();

        JoinNode leftJoin = new JoinNode(
                node.getId(),
                LEFT,
                leftWithUid,
                rightWithUid,
                node.getCriteria(),
                newLeftOutputs,
                newRightOutputs,
                node.isMaySkipOutputDuplicates(),
                node.getFilter(), // keep original inequality filter as-is
                node.getDistributionType(),
                node.isSpillable(),
                node.getDynamicFilters(),
                node.getReorderJoinStatsAndCost());

        // Window: row_number over partition by leftUid ordered by rightTs desc nulls last, rightUid desc nulls last
        Symbol rankSymbol = context.getSymbolAllocator().newSymbol("asof_rank", BIGINT);
        WindowNode.Function rowNumber = new WindowNode.Function(
                plannerContext.getMetadata().resolveBuiltinFunction("row_number", ImmutableList.of()),
                ImmutableList.of(),
                Optional.empty(),
                WindowNode.Frame.DEFAULT_FRAME,
                false,
                false);

        OrderingScheme orderingScheme = new OrderingScheme(
                ImmutableList.of(orderingColumns.rightTs, rightUid),
                ImmutableMap.of(orderingColumns.rightTs, SortOrder.DESC_NULLS_LAST, rightUid, SortOrder.DESC_NULLS_LAST));

        WindowNode windowNode = new WindowNode(
                context.getIdAllocator().getNextId(),
                leftJoin,
                new DataOrganizationSpecification(ImmutableList.of(leftUid), Optional.of(orderingScheme)),
                ImmutableMap.of(rankSymbol, rowNumber),
                ImmutableSet.of(),
                0);

        // Filter: rank = 1
        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                windowNode,
                new Comparison(LESS_THAN_OR_EQUAL, rankSymbol.toSymbolReference(), new Constant(BIGINT, 1L)));

        // Project back to original outputs (drop helper symbols)
        List<Symbol> finalOutputs = ImmutableList.<Symbol>builder()
                .addAll(node.getLeftOutputSymbols())
                .addAll(node.getRightOutputSymbols())
                .build();
        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                Assignments.identity(finalOutputs));

        return Result.ofPlanNode(projectNode);
    }

    private Optional<Comparison> extractSupportedInequality(JoinNode node)
    {
        Optional<Expression> filter = node.getFilter();
        if (filter.isEmpty()) {
            return Optional.empty();
        }
        List<Expression> conjuncts = IrUtils.extractConjuncts(filter.get());
        List<Comparison> comparisons = conjuncts.stream()
                .filter(Comparison.class::isInstance)
                .map(Comparison.class::cast)
                .filter(c -> c.operator() == LESS_THAN || c.operator() == LESS_THAN_OR_EQUAL || c.operator() == GREATER_THAN || c.operator() == GREATER_THAN_OR_EQUAL)
                .collect(Collectors.toList());
        if (comparisons.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(comparisons.get(0));
    }

    private RightOrderingColumns resolveOrderingColumns(JoinNode node, Comparison inequality)
    {
        Set<Symbol> leftSymbols = ImmutableSet.copyOf(node.getLeft().getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(node.getRight().getOutputSymbols());

        Expression leftExpr = inequality.left();
        Expression rightExpr = inequality.right();

        boolean leftExprFromRight = referencesOnly(leftExpr, rightSymbols);
        boolean rightExprFromLeft = referencesOnly(rightExpr, leftSymbols);
        boolean leftExprFromLeft = referencesOnly(leftExpr, leftSymbols);
        boolean rightExprFromRight = referencesOnly(rightExpr, rightSymbols);

        // Supported forms:
        // 1) right.ts <= left.ts  (or <)
        if ((inequality.operator() == LESS_THAN || inequality.operator() == LESS_THAN_OR_EQUAL) && leftExprFromRight && rightExprFromLeft) {
            Symbol rightTs = symbolFromReference(leftExpr);
            return new RightOrderingColumns(rightTs);
        }
        // 2) left.ts >= right.ts  (or >)
        if ((inequality.operator() == GREATER_THAN || inequality.operator() == GREATER_THAN_OR_EQUAL) && leftExprFromLeft && rightExprFromRight) {
            Symbol rightTs = symbolFromReference(rightExpr);
            return new RightOrderingColumns(rightTs);
        }

        throw new TrinoException(NOT_SUPPORTED, "ASOF join inequality must compare build-side timestamp to probe-side timestamp with <=/< or >=/>");
    }

    private boolean referencesOnly(Expression expression, Set<Symbol> allowed)
    {
        return io.trino.sql.planner.SymbolsExtractor.extractUnique(expression).stream().allMatch(allowed::contains);
    }

    private Symbol symbolFromReference(Expression expression)
    {
        checkArgument(expression instanceof Reference, "Expected symbol reference, got: %s", expression);
        return Symbol.from(expression);
    }

    private record RightOrderingColumns(Symbol rightTs) {}
}
