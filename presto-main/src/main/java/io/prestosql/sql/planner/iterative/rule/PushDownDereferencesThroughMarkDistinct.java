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
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.ProjectNode;
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
import static io.prestosql.sql.planner.plan.Patterns.markDistinct;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A.x), E := f2(B.x), G := f3(C))
 *      MarkDistinct(distinctSymbols = [B])
 *          Source(A, B, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(symbol), E := f2(B.x), G := f3(C))
 *      MarkDistinct(distinctSymbols = [B])
 *          Project(A, B, C, symbol := A.x)
 *              Source(A, B, C)
 * </pre>
 * <p>
 * Pushes down dereference projections through MarkDistinct. Excludes dereferences on "distinct symbols" to avoid data
 * replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughMarkDistinct
        implements Rule<ProjectNode>
{
    private static final Capture<MarkDistinctNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughMarkDistinct(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(markDistinct().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        MarkDistinctNode markDistinctNode = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<DereferenceExpression> dereferences = extractDereferences(projectNode.getAssignments().getExpressions(), false);

        // Exclude dereferences on distinct symbols being used in markDistinctNode. We do not need to filter
        // dereferences on markerSymbol since it is supposed to be of boolean type.
        dereferences = dereferences.stream()
                .filter(expression -> !markDistinctNode.getDistinctSymbols().contains(getBase(expression)))
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
                        markDistinctNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        markDistinctNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(markDistinctNode.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()))),
                        newAssignments));
    }
}
