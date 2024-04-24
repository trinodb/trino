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
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.BasicRelationStatistics;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Comparison.Operator;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.ConnectorExpressionTranslator.ConnectorExpressionTranslation;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.iterative.rule.Rules.deriveTableStatisticsForPushdown;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PushJoinIntoTableScan
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();
    private static final Capture<ProjectNode> LEFT_PROJECT = newCapture();
    private static final Capture<ProjectNode> RIGHT_PROJECT = newCapture();
    private final PlannerContext plannerContext;
    private final ProjectSide projectSide;
    private final Pattern<JoinNode> pattern;

    public static Set<Rule<?>> rules(PlannerContext plannerContext)
    {
        return ImmutableSet.of(
                new PushJoinIntoTableScan(plannerContext, ProjectSide.LEFT),
                new PushJoinIntoTableScan(plannerContext, ProjectSide.RIGHT),
                new PushJoinIntoTableScan(plannerContext, ProjectSide.BOTH),
                new PushJoinIntoTableScan(plannerContext, ProjectSide.NONE));
    }

    PushJoinIntoTableScan(PlannerContext plannerContext, ProjectSide projectSide)
    {
        this.projectSide = requireNonNull(projectSide, "projectSide is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.pattern = switch (projectSide) {
            case LEFT -> Patterns.join()
                    .with(left().matching(project().capturedAs(LEFT_PROJECT).with(source().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));
            case RIGHT -> Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(project().capturedAs(RIGHT_PROJECT).with(source().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)))));
            case BOTH -> Patterns.join()
                    .with(left().matching(project().capturedAs(LEFT_PROJECT).with(source().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))))
                    .with(right().matching(project().capturedAs(RIGHT_PROJECT).with(source().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)))));
            case NONE -> Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));
        };
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        PlanNodes nodes = PlanNodes.create(captures, joinNode, projectSide);

        if (nodes.join.isCrossJoin()) {
            return Result.empty();
        }

        if (nodes.leftScan.isUpdateTarget() || nodes.rightScan.isUpdateTarget()) {
            // unexpected Join over for-update table scan
            return Result.empty();
        }

        Expression effectiveFilter = getJoinNodeEffectiveFilter(nodes.join, nodes.leftProject, nodes.rightProject);
        ConnectorExpressionTranslation translation = ConnectorExpressionTranslator.translateConjuncts(
                context.getSession(),
                effectiveFilter);

        if (!translation.remainingExpression().equals(Booleans.TRUE)) {
            return Result.empty();
        }

        if (nodes.leftScan.getEnforcedConstraint().isNone() || nodes.rightScan.getEnforcedConstraint().isNone()) {
            // bailing out on one of the tables empty; this is not interesting case which makes handling
            // enforced constraint harder below.
            return Result.empty();
        }

        Map<String, ColumnHandle> leftScanAssignments = nodes.leftScan.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().name(), Entry::getValue));

        Map<String, ColumnHandle> rightScanAssignments = nodes.rightScan.getAssignments().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().name(), Entry::getValue));

        /*
         * We are (lazily) computing estimated statistics for join node and left and right table
         * and passing those to connector via applyJoin.
         *
         * There are couple reasons for this approach:
         * - the engine knows how to estimate join and connector may not
         * - the engine may have cached stats for the table scans (within context.getStatsProvider()), so can be able to provide information more inexpensively
         * - in the future, the engine may be able to provide stats for table scan even in case when connector no longer can (see https://github.com/trinodb/trino/issues/6998)
         * - the pushdown feasibility assessment logic may be different (or configured differently) for different connectors/catalogs.
         */
        JoinStatistics joinStatistics = getJoinStatistics(nodes.join, nodes.leftScan, nodes.rightScan, context);

        Optional<JoinApplicationResult<TableHandle>> joinApplicationResult = plannerContext.getMetadata().applyJoin(
                context.getSession(),
                getJoinType(nodes.join),
                nodes.leftScan.getTable(),
                nodes.rightScan.getTable(),
                translation.connectorExpression(),
                leftScanAssignments,
                rightScanAssignments,
                joinStatistics);

        if (joinApplicationResult.isEmpty()) {
            return Result.empty();
        }

        JoinApplicationResult<TableHandle> applyJoinResult = joinApplicationResult.get();

        Map<ColumnHandle, ColumnHandle> leftColumnHandlesMapping = applyJoinResult.getLeftColumnHandles();
        Map<ColumnHandle, ColumnHandle> rightColumnHandlesMapping = applyJoinResult.getRightColumnHandles();
        ImmutableMap.Builder<Symbol, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();
        assignmentsBuilder.putAll(nodes.leftScan.getAssignments().entrySet().stream().collect(toImmutableMap(
                Entry::getKey,
                entry -> leftColumnHandlesMapping.get(entry.getValue()))));
        assignmentsBuilder.putAll(nodes.rightScan.getAssignments().entrySet().stream().collect(toImmutableMap(
                Entry::getKey,
                entry -> rightColumnHandlesMapping.get(entry.getValue()))));
        Map<Symbol, ColumnHandle> tableScanAssignments = assignmentsBuilder.buildOrThrow();

        TupleDomain<ColumnHandle> newEnforcedConstraint = calculateConstraint(
                nodes,
                leftColumnHandlesMapping,
                rightColumnHandlesMapping);

        Assignments projectAssignments = calculateProjectAssignments(nodes, tableScanAssignments);

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                nodes.join.getId(),
                                applyJoinResult.getTableHandle(),
                                ImmutableList.copyOf(tableScanAssignments.keySet()),
                                tableScanAssignments,
                                newEnforcedConstraint,
                                deriveTableStatisticsForPushdown(
                                        context.getStatsProvider(),
                                        context.getSession(),
                                        applyJoinResult.isPrecalculateStatistics(),
                                        nodes.join),
                                false,
                                Optional.empty()),
                        projectAssignments));
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
                double outputRowCount = stats.getOutputRowCount();
                double outputSize = stats.getOutputSizeInBytes(outputSymbols);
                if (isNaN(outputRowCount) || isNaN(outputSize)) {
                    return Optional.empty();
                }
                return Optional.of(new BasicRelationStatistics((long) outputRowCount, (long) outputSize));
            }
        };
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    private record PlanNodes(JoinNode join, TableScanNode leftScan, TableScanNode rightScan, Optional<ProjectNode> leftProject, Optional<ProjectNode> rightProject)
    {
        public static PlanNodes create(Captures captures, JoinNode joinNode, ProjectSide projectSide)
        {
            return new PlanNodes(
                    joinNode,
                    captures.get(LEFT_TABLE_SCAN),
                    captures.get(RIGHT_TABLE_SCAN),
                    projectSide == ProjectSide.LEFT || projectSide == ProjectSide.BOTH ? Optional.of(captures.get(LEFT_PROJECT)) : Optional.empty(),
                    projectSide == ProjectSide.RIGHT || projectSide == ProjectSide.BOTH ? Optional.of(captures.get(RIGHT_PROJECT)) : Optional.empty());
        }
    }

    private static TupleDomain<ColumnHandle> calculateConstraint(
            PlanNodes nodes,
            Map<ColumnHandle, ColumnHandle> leftColumnHandlesMapping,
            Map<ColumnHandle, ColumnHandle> rightColumnHandlesMapping)
    {
        JoinType joinType = nodes.join.getType();
        TupleDomain<ColumnHandle> leftConstraint = deriveConstraint(nodes.leftScan.getEnforcedConstraint(), leftColumnHandlesMapping, joinType == RIGHT || joinType == FULL);
        TupleDomain<ColumnHandle> rightConstraint = deriveConstraint(nodes.rightScan.getEnforcedConstraint(), rightColumnHandlesMapping, joinType == LEFT || joinType == FULL);
        return TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>builder()
                        .putAll(leftConstraint.getDomains().orElseThrow())
                        .putAll(rightConstraint.getDomains().orElseThrow())
                        .buildOrThrow());
    }

    private static Assignments calculateProjectAssignments(PlanNodes nodes, Map<Symbol, ColumnHandle> assignments)
    {
        Assignments.Builder builder = Assignments.builder();
        nodes.join.getOutputSymbols().forEach(output -> {
            if (assignments.containsKey(output)) {
                builder.putIdentity(output);
            }
            else {
                // look for missing output assignment in projections
                Expression expression = findAssignment(output, nodes.leftProject)
                        .or(() -> findAssignment(output, nodes.rightProject))
                        .orElseThrow(() -> new IllegalStateException("Output %s not found".formatted(output)));
                builder.put(output, expression);
            }
        });
        return builder.build();
    }

    private static Optional<Expression> findAssignment(Symbol output, Optional<ProjectNode> projectNode)
    {
        return projectNode.flatMap(node -> Optional.ofNullable(node.getAssignments().get(output)));
    }

    private static TupleDomain<ColumnHandle> deriveConstraint(TupleDomain<ColumnHandle> constraint, Map<ColumnHandle, ColumnHandle> columnMapping, boolean nullable)
    {
        if (nullable) {
            constraint = constraint.transformDomains((columnHandle, domain) -> domain.union(onlyNull(domain.getType())));
        }
        return constraint.transformKeys(columnMapping::get);
    }

    private static Expression getJoinNodeEffectiveFilter(JoinNode node, Optional<ProjectNode> leftProject, Optional<ProjectNode> rightProject)
    {
        List<Expression> equiJoinClausesExpressions = node.getCriteria().stream()
                .map(equiJoinClause -> getExpression(equiJoinClause, leftProject, rightProject))
                .collect(toImmutableList());

        Expression joinCriteriaEffectiveFilter = and(equiJoinClausesExpressions);
        if (node.getFilter().isPresent()) {
            return and(joinCriteriaEffectiveFilter, node.getFilter().get());
        }
        return joinCriteriaEffectiveFilter;
    }

    private static Expression getExpression(EquiJoinClause equiJoinClause, Optional<ProjectNode> leftProjectNode, Optional<ProjectNode> rightProjectNode)
    {
        Expression leftExpression = leftProjectNode.map(assignments -> assignments.getAssignments().get(Symbol.from(equiJoinClause.getLeft().toSymbolReference())))
                .orElse(equiJoinClause.getLeft().toSymbolReference());
        Expression rightExpression = rightProjectNode.map(assignments -> assignments.getAssignments().get(Symbol.from(equiJoinClause.getRight().toSymbolReference())))
                .orElse(equiJoinClause.getRight().toSymbolReference());

        return new Comparison(Operator.EQUAL, leftExpression, rightExpression);
    }

    private io.trino.spi.connector.JoinType getJoinType(JoinNode joinNode)
    {
        return switch (joinNode.getType()) {
            case INNER -> io.trino.spi.connector.JoinType.INNER;
            case LEFT -> io.trino.spi.connector.JoinType.LEFT_OUTER;
            case RIGHT -> io.trino.spi.connector.JoinType.RIGHT_OUTER;
            case FULL -> io.trino.spi.connector.JoinType.FULL_OUTER;
        };
    }

    enum ProjectSide
    {
        LEFT, RIGHT, BOTH, NONE
    }
}
