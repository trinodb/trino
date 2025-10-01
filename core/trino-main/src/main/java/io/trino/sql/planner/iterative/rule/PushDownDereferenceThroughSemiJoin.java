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
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.semiJoin;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A.x), E := f2(B.x), G := f3(C))
 *      SemiJoin(sourceJoinSymbol = B, filteringSourceJoinSymbol = B_filtering)
 *          Source(A, B, C)
 *          FilteringSource(B_filtering)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(symbol), E := f2(B.x), G := f3(C))
 *          SemiJoinNode(sourceJoinSymbol = B, filteringSourceJoinSymbol = B_filtering)
 *              Project(A, B, C, symbol := A.x)
 *                  Source(A, B, C)
 *              FilteringSource(B_filtering)
 * </pre>
 * <p>
 * Pushes down dereference projections through SemiJoinNode. Excludes dereferences on sourceJoinSymbol to avoid
 * data replication, since this symbol cannot be pruned.
 */
public class PushDownDereferenceThroughSemiJoin
        implements Rule<ProjectNode>
{
    private static final Capture<SemiJoinNode> CHILD = newCapture();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(semiJoin().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        SemiJoinNode semiJoinNode = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<FieldReference> dereferences = extractRowSubscripts(projectNode.getAssignments().getExpressions(), false);

        // All dereferences can be assumed on the symbols coming from source, since filteringSource output is not propagated,
        // and semiJoinOutput is of type boolean. We exclude pushdown of dereferences on sourceJoinSymbol.
        dereferences = dereferences.stream()
                .filter(expression -> !getBase(expression).equals(semiJoinNode.getSourceJoinSymbol()))
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
        Assignments assignments = projectNode.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        PlanNode newSource = new ProjectNode(
                context.getIdAllocator().getNextId(),
                semiJoinNode.getSource(),
                Assignments.builder()
                        .putIdentities(semiJoinNode.getSource().getOutputSymbols())
                        .putAll(dereferenceAssignments)
                        .build());

        PlanNode newSemiJoin = semiJoinNode.replaceChildren(ImmutableList.of(newSource, semiJoinNode.getFilteringSource()));

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newSemiJoin, assignments));
    }
}
