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
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
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
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A.x), E := f2(C), B_BIGINT)
 *      Unnest(replicate = [A, C], unnest = (B_ARRAY -> [B_BIGINT]))
 *          Source(A, B_ARAAY, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(symbol), E := f2(C), B_BIGINT)
 *      Unnest(replicate = [A, C, symbol], unnest = (B_ARAAY -> [B_BIGINT]))
 *          Project(A, B_ARRAY, C, symbol := A.x)
 *              Source(A, B_ARAAY, C)
 * </pre>
 * <p>
 * Pushes down dereference projections through Unnest. Currently, the pushdown is only supported for dereferences on replicate symbols.
 */
public class PushDownDereferenceThroughUnnest
        implements Rule<ProjectNode>
{
    private static final Capture<UnnestNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferenceThroughUnnest(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(unnest().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        UnnestNode unnestNode = captures.get(CHILD);

        // Extract dereferences from project node's assignments and unnest node's filter
        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.builder();
        expressionsBuilder.addAll(projectNode.getAssignments().getExpressions());
        unnestNode.getFilter().ifPresent(expressionsBuilder::add);

        // Extract dereferences for pushdown
        Set<DereferenceExpression> dereferences = extractDereferences(expressionsBuilder.build(), false);

        // Only retain dereferences on replicate symbols
        dereferences = dereferences.stream()
                .filter(expression -> unnestNode.getReplicateSymbols().contains(getBase(expression)))
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

        // Create a new ProjectNode (above the original source) adding dereference projections on replicated symbols
        ProjectNode source = new ProjectNode(
                context.getIdAllocator().getNextId(),
                unnestNode.getSource(),
                Assignments.builder()
                        .putIdentities(unnestNode.getSource().getOutputSymbols())
                        .putAll(dereferenceAssignments)
                        .build());

        // Create projectNode with the new unnest node and assignments with replaced dereferences
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new UnnestNode(
                                context.getIdAllocator().getNextId(),
                                source,
                                ImmutableList.<Symbol>builder()
                                        .addAll(unnestNode.getReplicateSymbols())
                                        .addAll(dereferenceAssignments.getSymbols())
                                        .build(),
                                unnestNode.getMappings(),
                                unnestNode.getOrdinalitySymbol(),
                                unnestNode.getJoinType(),
                                unnestNode.getFilter().map(filter -> replaceExpression(filter, mappings))),
                        newAssignments));
    }
}
