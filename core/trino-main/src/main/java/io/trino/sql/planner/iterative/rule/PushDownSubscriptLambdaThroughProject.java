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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.enablePushSubscriptLambdaIntoScan;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractSubscriptLambdas;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * This rule is to push subscript lambdas into below projection. This rule increases the
 * possibilities of subscript lambdas reaching near table scans
 */

/**
 * Transforms:
 * <pre>
 *  Project(c := f(a, x -> x[1]), d := g(b))
 *    Project(a, b)
 *  </pre>
 * to:
 * <pre>
 *  Project(c := expr d := g(b))
 *    Project(a, b, expr := f(a, x -> x[1]))
 * </pre>
 */
public class PushDownSubscriptLambdaThroughProject
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownSubscriptLambdaThroughProject(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(project().capturedAs(CHILD)));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session)
                && enablePushSubscriptLambdaIntoScan(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Extract subscript lambdas from project node assignments for pushdown
        Map<FunctionCall, SymbolReference> subscriptLambdas = extractSubscriptLambdas(node.getAssignments().getExpressions());

        // Exclude subscript lambdas on symbols being synthesized within child
        subscriptLambdas = subscriptLambdas.entrySet().stream()
                .filter(e -> child.getSource().getOutputSymbols().contains(Symbol.from(e.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for subscript lambda expressions
        Assignments subscriptLambdaAssignments = Assignments.of(subscriptLambdas.keySet(), context.getSession(), context.getSymbolAllocator(), typeAnalyzer);

        // Rewrite project node assignments using new symbols for subscript lambda expressions
        Map<Expression, SymbolReference> mappings = HashBiMap.create(subscriptLambdaAssignments.getMap())
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
                                        .putAll(subscriptLambdaAssignments)
                                        .build()),
                        assignments));
    }
}
