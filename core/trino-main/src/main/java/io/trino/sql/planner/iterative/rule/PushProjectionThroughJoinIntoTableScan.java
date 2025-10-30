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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Pushes projections that appear above a join down to the table scans on either side of the join.
 * This is particularly useful for cross-connector joins where the join cannot be pushed down, but
 * individual projections on each side can still be pushed to their respective connectors.
 *
 * <pre>
 * Transforms:
 *   Project(x := f(a), y := g(b))
 *     Join(a = b)
 *       TableScan(a, ...)
 *       TableScan(b, ...)
 *
 * Into:
 *   Project(x, y)  -- identity projections
 *     Join(a = b)
 *       Project(x := f(a), a)  -- pushed down
 *         TableScan(a, ...)
 *       Project(y := g(b), b)  -- pushed down
 *         TableScan(b, ...)
 * </pre>
 *
 * <p>The rule only applies when: - All projection expressions are deterministic - Each projection
 * expression references columns from only one side of the join - For outer joins, projections on
 * the non-preserved side are not pushed (to maintain correctness)
 */
public class PushProjectionThroughJoinIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project().with(source().matching(join().capturedAs(JOIN)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);

        // Only apply to deterministic projections
        if (!project.getAssignments().expressions().stream().allMatch(DeterminismEvaluator::isDeterministic)) {
            return Result.empty();
        }

        // Skip if all projections are identity - nothing to push down
        if (project.getAssignments().isIdentity()) {
            return Result.empty();
        }

        Set<Symbol> joinCriteriaSymbols = join.getCriteria().stream().flatMap(criteria -> Stream.of(criteria.getLeft(), criteria.getRight())).collect(toImmutableSet());
        Set<Symbol> joinFilterSymbols = join.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of());
        Set<Symbol> joinRequiredSymbols = ImmutableSet.<Symbol>builder().addAll(joinCriteriaSymbols).addAll(joinFilterSymbols).build();

        // Separate projections by which side of the join they reference
        Assignments.Builder leftProjections = Assignments.builder();
        Assignments.Builder rightProjections = Assignments.builder();
        Assignments.Builder remainingProjections = Assignments.builder();

        Set<Symbol> leftSymbols = ImmutableSet.copyOf(join.getLeft().getOutputSymbols());
        Set<Symbol> rightSymbols = ImmutableSet.copyOf(join.getRight().getOutputSymbols());

        // Track if we're pushing down any non-identity projections
        boolean hasNonIdentityProjectionsToPush = false;

        for (Map.Entry<Symbol, Expression> assignment : project.getAssignments().entrySet()) {
            Symbol outputSymbol = assignment.getKey();
            Expression expression = assignment.getValue();
            Set<Symbol> referencedSymbols = extractUnique(expression);

            boolean referencesLeft = leftSymbols.containsAll(referencedSymbols);
            boolean referencesRight = rightSymbols.containsAll(referencedSymbols);
            boolean isIdentity = expression instanceof Reference && ((Reference) expression).name().equals(outputSymbol.name());

            if (referencesLeft && !referencesRight) {
                // Can potentially push to left side
                if (join.getType() == JoinType.RIGHT || join.getType() == JoinType.FULL) {
                    remainingProjections.put(outputSymbol, expression);
                }
                else {
                    leftProjections.put(outputSymbol, expression);
                    if (!isIdentity) {
                        hasNonIdentityProjectionsToPush = true;
                    }
                }
            }
            else if (referencesRight && !referencesLeft) {
                // Can potentially push to right side
                if (join.getType() == JoinType.LEFT || join.getType() == JoinType.FULL) {
                    remainingProjections.put(outputSymbol, expression);
                }
                else {
                    rightProjections.put(outputSymbol, expression);
                    if (!isIdentity) {
                        hasNonIdentityProjectionsToPush = true;
                    }
                }
            }
            else {
                // References both sides or neither - keep above join
                remainingProjections.put(outputSymbol, expression);
            }
        }

        // If there are remaining projections that will stay above the join,
        // ensure their dependencies flow through the join outputs
        Assignments remainingAssignments = remainingProjections.build();
        Set<Symbol> remainingDependencies = ImmutableSet.of();
        if (!remainingAssignments.isEmpty()) {
            // Extract all symbols referenced by remaining projections
            remainingDependencies = remainingAssignments.expressions().stream()
                    .flatMap(expr -> extractUnique(expr).stream())
                    .collect(toImmutableSet());
        }

        // Add identity projections for symbols required by the join
        // Also add identity projections for symbols needed by remaining projections
        // Only add to sides that have TableScans
        Set<Symbol> requiredSymbols = ImmutableSet.<Symbol>builder()
                .addAll(joinRequiredSymbols)
                .addAll(remainingDependencies)
                .build();

        for (Symbol symbol : requiredSymbols) {
            if (leftSymbols.contains(symbol)) {
                leftProjections.putIdentity(symbol);
            }
            if (rightSymbols.contains(symbol)) {
                rightProjections.putIdentity(symbol);
            }
        }

        Assignments leftAssignments = leftProjections.build();
        Assignments rightAssignments = rightProjections.build();

        // If no non-identity projections can be pushed down, return empty
        // This prevents infinite loops where we keep pushing down only identity projections
        if (!hasNonIdentityProjectionsToPush) {
            return Result.empty();
        }

        // Create new project nodes on each side if there are projections to push
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        PlanNode newLeft = join.getLeft();
        PlanNode newRight = join.getRight();

        if (!leftAssignments.isEmpty()) {
            newLeft = new ProjectNode(idAllocator.getNextId(), join.getLeft(), leftAssignments);
        }

        if (!rightAssignments.isEmpty()) {
            newRight = new ProjectNode(idAllocator.getNextId(), join.getRight(), rightAssignments);
        }

        // Build the list of output symbols for the new join
        // Include all symbols from both sides - the Project above will filter as needed
        List<Symbol> newLeftOutputSymbols = newLeft.getOutputSymbols();
        List<Symbol> newRightOutputSymbols = newRight.getOutputSymbols();

        // Create new join with pushed-down projections
        JoinNode newJoin = new JoinNode(
                join.getId(),
                join.getType(),
                newLeft,
                newRight,
                join.getCriteria(),
                newLeftOutputSymbols,
                newRightOutputSymbols,
                join.isMaySkipOutputDuplicates(),
                join.getFilter(),
                join.getDistributionType(),
                join.isSpillable(),
                join.getDynamicFilters(),
                join.getReorderJoinStatsAndCost());

        // If there are remaining projections, keep them above the join
        if (!remainingProjections.build().isEmpty()) {
            // Create identity projections for pushed-down symbols
            for (Symbol symbol : project.getOutputSymbols()) {
                if (remainingProjections.build().get(symbol) == null) {
                    remainingProjections.putIdentity(symbol);
                }
            }

            return Result.ofPlanNode(new ProjectNode(project.getId(), newJoin, remainingProjections.build()));
        }

        // All projections were pushed down, just return the join
        // But we may need to restrict outputs to match the original project's outputs
        if (!newJoin.getOutputSymbols().equals(project.getOutputSymbols())) {
            return Result.ofPlanNode(new ProjectNode(project.getId(), newJoin, Assignments.identity(project.getOutputSymbols())));
        }

        return Result.ofPlanNode(newJoin);
    }
}
