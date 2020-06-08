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

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.extractDereferences;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topNRowNumber;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(E := f1(A.x), G := f2(B.x), H := f3(C.x), J := f4(D))
 *      TopNRowNumber(partitionBy = [B], orderBy = [C])
 *          Source(A, B, C, D)
 *  </pre>
 * to:
 * <pre>
 *  Project(E := f1(symbol), G := f2(B.x), H := f3(C.x), J := f4(D))
 *      TopNRowNumber(partitionBy = [B], orderBy = [C])
 *          Project(A, B, C, D, symbol := A.x)
 *              Source(A, B, C, D)
 * </pre>
 * <p>
 * Pushes down dereference projections through TopNRowNumber. Excludes dereferences on symbols in partitionBy and ordering scheme
 * to avoid data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughTopNRowNumber
        implements Rule<ProjectNode>
{
    private static final Capture<TopNRowNumberNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughTopNRowNumber(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(topNRowNumber().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        TopNRowNumberNode topNRowNumberNode = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<DereferenceExpression> dereferences = extractDereferences(projectNode.getAssignments().getExpressions(), false);

        // Exclude dereferences on symbols being used in partitionBy and orderBy
        WindowNode.Specification specification = topNRowNumberNode.getSpecification();
        dereferences = dereferences.stream()
                .filter(expression -> {
                    Symbol symbol = getBase(expression);
                    return !specification.getPartitionBy().contains(symbol)
                            && !specification.getOrderingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()).contains(symbol);
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
                        topNRowNumberNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        topNRowNumberNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(topNRowNumberNode.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()))),
                        newAssignments));
    }
}
