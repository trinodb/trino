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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.markDistinct;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

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
        Set<FieldReference> dereferences = extractRowSubscripts(projectNode.getAssignments().getExpressions(), false);

        // Exclude dereferences on distinct symbols being used in markDistinctNode. We do not need to filter
        // dereferences on markerSymbol since it is supposed to be of boolean type.
        dereferences = dereferences.stream()
                .filter(expression -> !markDistinctNode.getDistinctSymbols().contains(getBase(expression)))
                .collect(toImmutableSet());

        if (dereferences.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for dereference expressions
        Assignments dereferenceAssignments = Assignments.of(dereferences, context.getSymbolAllocator());

        // Rewrite project node assignments using new symbols for dereference expressions
        Map<Expression, Reference> mappings = HashBiMap.create(dereferenceAssignments.assignments())
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
