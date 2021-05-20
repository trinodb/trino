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
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A.x), E := f2(B), G := f3(C))
 *      Filter(A.x.y = 5 AND B.m = 3)
 *          Source(A, B, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(expr), E := f2(B), G := f3(C))
 *      Filter(expr.y = 5 AND B.m = 3)
 *          Project(A, B, C, expr := A.x)
 *              Source(A, B, C)
 * </pre>
 * <p>
 * Pushes down dereference projections in project node assignments and filter node predicate.
 */
public class PushDownDereferenceThroughFilter
        implements Rule<ProjectNode>
{
    private static final Capture<FilterNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughFilter(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(filter().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Rule.Context context)
    {
        FilterNode filterNode = captures.get(CHILD);

        // Pushdown superset of dereference expressions from projections and filtering predicate
        List<Expression> expressions = ImmutableList.<Expression>builder()
                .addAll(node.getAssignments().getExpressions())
                .add(filterNode.getPredicate())
                .build();

        // Extract dereferences from project node assignments for pushdown
        Set<SubscriptExpression> dereferences = extractRowSubscripts(expressions, false, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes());

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
        Assignments assignments = node.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        PlanNode source = filterNode.getSource();

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        source,
                                        Assignments.builder()
                                                .putIdentities(source.getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()),
                                replaceExpression(filterNode.getPredicate(), mappings)),
                        assignments));
    }
}
