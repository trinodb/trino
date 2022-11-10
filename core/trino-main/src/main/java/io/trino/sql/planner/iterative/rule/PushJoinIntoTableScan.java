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
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.ConnectorExpressionTranslator.ConnectorExpressionTranslation;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PushJoinIntoTableScan
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();

    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushJoinIntoTableScan(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.isCrossJoin()) {
            return Result.empty();
        }

        TableScanNode left = captures.get(LEFT_TABLE_SCAN);
        TableScanNode right = captures.get(RIGHT_TABLE_SCAN);

        verify(!left.isUpdateTarget() && !right.isUpdateTarget(), "Unexpected Join over for-update table scan");

        Expression effectiveFilter = getEffectiveFilter(joinNode);
        ConnectorExpressionTranslation translation = ConnectorExpressionTranslator.translateConjuncts(
                context.getSession(),
                effectiveFilter,
                context.getSymbolAllocator().getTypes(),
                plannerContext,
                typeAnalyzer);

        if (!translation.remainingExpression().equals(BooleanLiteral.TRUE_LITERAL)) {
            // TODO add extra filter node above join
            return Result.empty();
        }

        if (left.getEnforcedConstraint().isNone() || right.getEnforcedConstraint().isNone()) {
            // bailing out on one of the tables empty; this is not interesting case which makes handling
            // enforced constraint harder below.
            return Result.empty();
        }

        Map<String, ColumnHandle> leftAssignments = left.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Map<String, ColumnHandle> rightAssignments = right.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        /*
         * We are (lazily) computing estimated statistics for join node and left and right table
         * and passing those to connector via applyJoin.
         *
         * There are a couple reasons for this approach:
         * - the engine knows how to estimate join and connector may not
         * - the engine may have cached stats for the table scans (within context.getStatsProvider()), so can be able to provide information more inexpensively
         * - in the future, the engine may be able to provide stats for table scan even in case when connector no longer can (see https://github.com/trinodb/trino/issues/6998)
         * - the pushdown feasibility assessment logic may be different (or configured differently) for different connectors/catalogs.
         */
        JoinStatistics joinStatistics = getJoinStatistics(joinNode, left, right, context);

        Optional<JoinApplicationResult<TableHandle>> joinApplicationResult = plannerContext.getMetadata().applyJoin(
                context.getSession(),
                getJoinType(joinNode),
                left.getTable(),
                right.getTable(),
                translation.connectorExpression(),
                // TODO we could pass only subset of assignments here, those which are needed to resolve translation.getPushableConditions
                leftAssignments,
                rightAssignments,
                joinStatistics);

        if (joinApplicationResult.isEmpty()) {
            return Result.empty();
        }

        TableHandle handle = joinApplicationResult.get().getTableHandle();

        Map<ColumnHandle, ColumnHandle> leftColumnHandlesMapping = joinApplicationResult.get().getLeftColumnHandles();
        Map<ColumnHandle, ColumnHandle> rightColumnHandlesMapping = joinApplicationResult.get().getRightColumnHandles();

        ImmutableMap.Builder<Symbol, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        assignmentsBuilder.putAll(left.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> leftColumnHandlesMapping.get(entry.getValue()))));
        assignmentsBuilder.putAll(right.getAssignments().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                entry -> rightColumnHandlesMapping.get(entry.getValue()))));
        Map<Symbol, ColumnHandle> assignments = assignmentsBuilder.buildOrThrow();

        // convert enforced constraint
        JoinNode.Type joinType = joinNode.getType();
        TupleDomain<ColumnHandle> leftConstraint = deriveConstraint(left.getEnforcedConstraint(), leftColumnHandlesMapping, joinType == RIGHT || joinType == FULL);
        TupleDomain<ColumnHandle> rightConstraint = deriveConstraint(right.getEnforcedConstraint(), rightColumnHandlesMapping, joinType == LEFT || joinType == FULL);

        TupleDomain<ColumnHandle> newEnforcedConstraint = TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        // we are sure that domains map is present as we bailed out on isNone above
                        .putAll(leftConstraint.getDomains().orElseThrow())
                        .putAll(rightConstraint.getDomains().orElseThrow())
                        .buildOrThrow());

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                joinNode.getId(),
                                handle,
                                ImmutableList.copyOf(assignments.keySet()),
                                assignments,
                                newEnforcedConstraint,
                                deriveTableStatisticsForPushdown(context.getStatsProvider(), context.getSession(), joinApplicationResult.get().isPrecalculateStatistics(), joinNode),
                                false,
                                Optional.empty()),
                        Assignments.identity(joinNode.getOutputSymbols())));
    }

    private JoinStatistics getJoinStatistics(JoinNode join, TableScanNode left, TableScanNode right, Context context)
    {
        return new JoinStatistics()
        {
            @Override
            public Optional<BasicRelationStatistics> getLeftStatistics()
            {
                return getBasicRelationStats(left, left.getOutputSymbols(), context);
            }

            @Override
            public Optional<BasicRelationStatistics> getRightStatistics()
            {
                return getBasicRelationStats(right, right.getOutputSymbols(), context);
            }

            @Override
            public Optional<BasicRelationStatistics> getJoinStatistics()
            {
                return getBasicRelationStats(join, join.getOutputSymbols(), context);
            }

            private Optional<BasicRelationStatistics> getBasicRelationStats(PlanNode node, List<Symbol> outputSymbols, Context context)
            {
                PlanNodeStatsEstimate stats = context.getStatsProvider().getStats(node);
                TypeProvider types = context.getSymbolAllocator().getTypes();
                double outputRowCount = stats.getOutputRowCount();
                double outputSize = stats.getOutputSizeInBytes(outputSymbols, types);
                if (isNaN(outputRowCount) || isNaN(outputSize)) {
                    return Optional.empty();
                }
                return Optional.of(new BasicRelationStatistics((long) outputRowCount, (long) outputSize));
            }
        };
    }

    private TupleDomain<ColumnHandle> deriveConstraint(TupleDomain<ColumnHandle> sourceConstraint, Map<ColumnHandle, ColumnHandle> columnMapping, boolean nullable)
    {
        TupleDomain<ColumnHandle> constraint = sourceConstraint;
        if (nullable) {
            constraint = constraint.transformDomains((columnHandle, domain) -> domain.union(onlyNull(domain.getType())));
        }
        return constraint.transformKeys(columnMapping::get);
    }

    public Expression getEffectiveFilter(JoinNode node)
    {
        Expression effectiveFilter = and(node.getCriteria().stream().map(JoinNode.EquiJoinClause::toExpression).collect(toImmutableList()));
        if (node.getFilter().isPresent()) {
            effectiveFilter = and(effectiveFilter, node.getFilter().get());
        }
        return effectiveFilter;
    }

    private JoinType getJoinType(JoinNode joinNode)
    {
        return switch (joinNode.getType()) {
            case INNER -> JoinType.INNER;
            case LEFT -> JoinType.LEFT_OUTER;
            case RIGHT -> JoinType.RIGHT_OUTER;
            case FULL -> JoinType.FULL_OUTER;
        };
    }
}
