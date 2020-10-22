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
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.extractDereferences;
import static io.prestosql.sql.planner.plan.Patterns.assignUniqueId;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(C := f1(A.x), D := f2(B))
 *      AssignUniqueId
 *          Source(A, B)
 *  </pre>
 * to:
 * <pre>
 *  Project(C := f1(symbol), D := f2(B))
 *      AssignUniqueId
 *          Project(A, B, symbol := A.x)
 *              Source(A, B)
 * </pre>
 */
public class PushDownDereferencesThroughAssignUniqueId
        implements Rule<ProjectNode>
{
    private static final Capture<AssignUniqueId> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferencesThroughAssignUniqueId(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(assignUniqueId().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        AssignUniqueId assignUniqueId = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<DereferenceExpression> dereferences = extractDereferences(projectNode.getAssignments().getExpressions(), false);

        // We do not need to filter dereferences on idColumn symbol since it is supposed to be of BIGINT type.

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
                        assignUniqueId.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        assignUniqueId.getSource(),
                                        Assignments.builder()
                                                .putIdentities(assignUniqueId.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()))),
                        newAssignments));
    }
}
