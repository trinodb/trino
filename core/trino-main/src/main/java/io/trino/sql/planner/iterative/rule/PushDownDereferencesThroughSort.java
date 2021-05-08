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
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.sort;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A.x), E := f2(B.x), G := f3(C))
 *      Sort(orderBy = [B])
 *          Source(A, B, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(symbol), E := f2(B.x), G := f3(C))
 *      Sort(orderBy = [B])
 *          Project(A, B, C, symbol := A.x)
 *              Source(A, B, C)
 * </pre>
 * <p>
 * Pushes down dereference projections through Sort. Excludes dereferences on symbols in ordering scheme to avoid data
 * replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughSort
        implements Rule<ProjectNode>
{
    private static final Capture<SortNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughSort(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(sort().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        SortNode sortNode = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<SubscriptExpression> dereferences = extractRowSubscripts(projectNode.getAssignments().getExpressions(), false, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes());

        // Exclude dereferences on symbols used in ordering scheme to avoid replication of data
        dereferences = dereferences.stream()
                .filter(expression -> !sortNode.getOrderingScheme().getOrderBy().contains(getBase(expression)))
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
                        sortNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        sortNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(sortNode.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()))),
                        newAssignments));
    }
}
