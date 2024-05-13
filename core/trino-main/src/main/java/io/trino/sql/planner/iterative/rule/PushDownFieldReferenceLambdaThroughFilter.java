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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.SystemSessionProperties.isPushFieldDereferenceLambdaIntoScanEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractSubscriptLambdas;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getReferences;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * This rule is to push field reference lambdas into below projection through filter. This rule increases the
 * possibilities of subscript lambdas reaching near table scans
 */

/**
 * Transforms:
 * <pre>
 *  Project(c := f(a, x -> x[1]), d := g(b))
 *    Filter(b = 3)
 *    Project(a, b)
 *  </pre>
 * to:
 * <pre>
 *  Project(c := expr, d := g(b))
 *    Filter(b = 3)
 *    Project(expr := f(a, x -> x[1]), b)
 * </pre>
 */
public class PushDownFieldReferenceLambdaThroughFilter
        implements Rule<ProjectNode>
{
    private static final Capture<FilterNode> CHILD = newCapture();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(filter().capturedAs(CHILD)));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session)
                && isPushFieldDereferenceLambdaIntoScanEnabled(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Rule.Context context)
    {
        FilterNode filterNode = captures.get(CHILD);

        // Extract subscript lambdas from project node assignments for pushdown
        Map<Call, Reference> subscriptLambdas = extractSubscriptLambdas(node.getAssignments().getExpressions());

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        // If filter has same references as subscript inputs, skip to be safe, extending the scope later
        List<Reference> filterSymbolReferences = getReferences(filterNode.getPredicate());
        subscriptLambdas = subscriptLambdas.entrySet().stream()
                .filter(e -> !filterSymbolReferences.contains(e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for subscript lambda expressions
        Assignments subscriptLambdaAssignments = Assignments.of(subscriptLambdas.keySet(), context.getSymbolAllocator());

        // Rewrite project node assignments using new symbols for subscript lambda expressions
        Map<Expression, Reference> mappings = HashBiMap.create(subscriptLambdaAssignments.getMap())
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
                                                .putAll(subscriptLambdaAssignments)
                                                .build()),
                                filterNode.getPredicate()),
                        assignments));
    }
}
