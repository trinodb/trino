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

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Transforms:
 * <pre>
 *  Project(A_X := f1(A.x), G := f2(A_Y.z), E := f3(B))
 *    Join(A_Y = C_Y) => [A, B]
 *      Project(A_Y := A.y, A, B)
 *          Source(A, B)
 *      Project(C_Y := C.y)
 *          Source(C, D)
 *  </pre>
 * to:
 * <pre>
 *  Project(A_X := f1(symbol), G := f2(A_Y.z), E := f3(B))
 *    Join(A_Y = C_Y) => [symbol, B]
 *      Project(symbol := A.x, A_Y := A.y, A, B)
 *        Source(A, B)
 *      Project(C_Y := C.y)
 *        Source(C, D)
 * </pre>
 * <p>
 * Pushes down dereference projections through JoinNode. Excludes dereferences on symbols being used in join criteria to avoid
 * data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferenceThroughJoin
        implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughJoin(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(join().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);

        // Consider dereferences in projections and join filter for pushdown
        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        expressionsBuilder.addAll(projectNode.getAssignments().getExpressions());
        joinNode.getFilter().ifPresent(expressionsBuilder::add);
        Set<SubscriptExpression> dereferences = extractRowSubscripts(expressionsBuilder.build(), false, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes());

        // Exclude criteria symbols
        ImmutableSet.Builder<Symbol> criteriaSymbolsBuilder = ImmutableSet.builder();
        joinNode.getCriteria().forEach(criteria -> {
            criteriaSymbolsBuilder.add(criteria.getLeft());
            criteriaSymbolsBuilder.add(criteria.getRight());
        });
        Set<Symbol> excludeSymbols = criteriaSymbolsBuilder.build();

        dereferences = dereferences.stream()
                .filter(expression -> !excludeSymbols.contains(getBase(expression)))
                .collect(toImmutableSet());

        if (dereferences.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for dereference expressions
        Assignments dereferenceAssignments = Assignments.of(dereferences, context.getSession(), context.getSymbolAllocator(), typeAnalyzer);

        // Rewrite project node assignments using new symbols for dereference expressions
        Map<Expression, SymbolReference> mappings = HashBiMap.create(dereferenceAssignments.getMap())
                .inverse()
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));
        Assignments newAssignments = projectNode.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        Assignments.Builder leftAssignmentsBuilder = Assignments.builder();
        Assignments.Builder rightAssignmentsBuilder = Assignments.builder();

        // Separate dereferences coming from left and right nodes
        dereferenceAssignments.entrySet().stream()
                .forEach(entry -> {
                    Symbol baseSymbol = getOnlyElement(extractAll(entry.getValue()));
                    if (joinNode.getLeft().getOutputSymbols().contains(baseSymbol)) {
                        leftAssignmentsBuilder.put(entry.getKey(), entry.getValue());
                    }
                    else if (joinNode.getRight().getOutputSymbols().contains(baseSymbol)) {
                        rightAssignmentsBuilder.put(entry.getKey(), entry.getValue());
                    }
                    else {
                        throw new IllegalArgumentException(format("Unexpected symbol %s in projectNode", baseSymbol));
                    }
                });

        Assignments leftAssignments = leftAssignmentsBuilder.build();
        Assignments rightAssignments = rightAssignmentsBuilder.build();

        PlanNode leftNode = createProjectNodeIfRequired(joinNode.getLeft(), leftAssignments, context.getIdAllocator());
        PlanNode rightNode = createProjectNodeIfRequired(joinNode.getRight(), rightAssignments, context.getIdAllocator());

        // Prepare new output symbols for join node
        List<Symbol> referredSymbolsInAssignments = newAssignments.getExpressions().stream()
                .flatMap(expression -> extractAll(expression).stream())
                .collect(toList());

        List<Symbol> newLeftOutputSymbols = referredSymbolsInAssignments.stream()
                .filter(symbol -> leftNode.getOutputSymbols().contains(symbol))
                .collect(toList());

        List<Symbol> newRightOutputSymbols = referredSymbolsInAssignments.stream()
                .filter(symbol -> rightNode.getOutputSymbols().contains(symbol))
                .collect(toList());

        JoinNode newJoinNode = new JoinNode(
                context.getIdAllocator().getNextId(),
                joinNode.getType(),
                leftNode,
                rightNode,
                joinNode.getCriteria(),
                newLeftOutputSymbols,
                newRightOutputSymbols,
                joinNode.isMaySkipOutputDuplicates(),
                // Use newly created symbols in filter
                joinNode.getFilter().map(expression -> replaceExpression(expression, mappings)),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newJoinNode, newAssignments));
    }

    private static PlanNode createProjectNodeIfRequired(PlanNode planNode, Assignments dereferences, PlanNodeIdAllocator idAllocator)
    {
        if (dereferences.isEmpty()) {
            return planNode;
        }
        return new ProjectNode(
                idAllocator.getNextId(),
                planNode,
                Assignments.builder()
                        .putIdentities(planNode.getOutputSymbols())
                        .putAll(dereferences)
                        .build());
    }
}
