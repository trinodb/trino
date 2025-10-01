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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(c := f(a.x), d := g(b))
 *    Project(a, b)
 *  </pre>
 * to:
 * <pre>
 *  Project(c := f(symbol), d := g(b))
 *    Project(a, b, symbol := a.x)
 * </pre>
 */
public class PushDownDereferenceThroughProject
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(project().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<FieldReference> dereferences = extractRowSubscripts(node.getAssignments().expressions(), false);

        // Exclude dereferences on symbols being synthesized within child
        dereferences = dereferences.stream()
                .filter(expression -> child.getSource().getOutputSymbols().contains(getBase(expression)))
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
        Assignments assignments = node.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                child.getSource(),
                                Assignments.builder()
                                        .putAll(child.getAssignments())
                                        .putAll(dereferenceAssignments)
                                        .build()),
                        assignments));
    }
}
