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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.window;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(G := f1(A.x), H := f2(B.x), J := f3(C.x), K := f4(D.x), L := f5(F))
 *      Window(orderBy = [B], partitionBy = [C], min_D := min(D))
 *          Source(A, B, C, D, E, F)
 *  </pre>
 * to:
 * <pre>
 *  Project(G := f1(symbol), H := f2(B.x), J := f3(C.x), K := f4(D.x), L := f5(F))
 *      Window(orderBy = [B], partitionBy = [C], min_D := min(D))
 *          Project(A, B, C, D, E, F, symbol := A.x)
 *              Source(A, B, C, D, E, F)
 * </pre>
 * <p>
 * Pushes down dereference projections through Window. Excludes dereferences on symbols in ordering scheme and partitionBy
 * to avoid data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughWindow
        implements Rule<ProjectNode>
{
    private static final Capture<WindowNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughWindow(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(window().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        WindowNode windowNode = captures.get(CHILD);

        // Extract dereferences for pushdown
        Set<SubscriptExpression> dereferences = extractRowSubscripts(
                ImmutableList.<Expression>builder()
                        .addAll(projectNode.getAssignments().getExpressions())
                        // also include dereference projections used in window functions
                        .addAll(windowNode.getWindowFunctions().values().stream()
                                .flatMap(function -> function.getArguments().stream())
                                .collect(toImmutableList()))
                        .build(),
                false,
                context.getSession(),
                typeAnalyzer,
                context.getSymbolAllocator().getTypes());

        DataOrganizationSpecification specification = windowNode.getSpecification();
        dereferences = dereferences.stream()
                .filter(expression -> {
                    Symbol symbol = getBase(expression);
                    // Exclude partitionBy, orderBy and synthesized symbols
                    return !specification.getPartitionBy().contains(symbol) &&
                            !specification.getOrderingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()).contains(symbol) &&
                            !windowNode.getCreatedSymbols().contains(symbol);
                })
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

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new WindowNode(
                                windowNode.getId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        windowNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(windowNode.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()),
                                windowNode.getSpecification(),
                                // Replace dereference expressions in functions
                                windowNode.getWindowFunctions().entrySet().stream()
                                        .collect(toImmutableMap(
                                                Map.Entry::getKey,
                                                entry -> {
                                                    WindowNode.Function oldFunction = entry.getValue();
                                                    return new WindowNode.Function(
                                                            oldFunction.getResolvedFunction(),
                                                            oldFunction.getArguments().stream()
                                                                    .map(expression -> replaceExpression(expression, mappings))
                                                                    .collect(toImmutableList()),
                                                            oldFunction.getFrame(),
                                                            oldFunction.isIgnoreNulls());
                                                })),
                                windowNode.getHashSymbol(),
                                windowNode.getPrePartitionedInputs(),
                                windowNode.getPreSortedOrderPrefix()),
                        newAssignments));
    }
}
