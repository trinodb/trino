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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNRankingNode;

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
import static io.trino.sql.planner.plan.Patterns.topNRanking;

/**
 * Transforms:
 * <pre>
 *  Project(E := f1(A.x), G := f2(B.x), H := f3(C.x), J := f4(D))
 *      TopNRanking(partitionBy = [B], orderBy = [C])
 *          Source(A, B, C, D)
 *  </pre>
 * to:
 * <pre>
 *  Project(E := f1(symbol), G := f2(B.x), H := f3(C.x), J := f4(D))
 *      TopNRanking(partitionBy = [B], orderBy = [C])
 *          Project(A, B, C, D, symbol := A.x)
 *              Source(A, B, C, D)
 * </pre>
 * <p>
 * Pushes down dereference projections through TopNRanking. Excludes dereferences on symbols in partitionBy and ordering scheme
 * to avoid data replication, since these symbols cannot be pruned.
 */
public class PushDownDereferencesThroughTopNRanking
        implements Rule<ProjectNode>
{
    private static final Capture<TopNRankingNode> CHILD = newCapture();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(topNRanking().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        TopNRankingNode topNRankingNode = captures.get(CHILD);

        // Extract dereferences from project node assignments for pushdown
        Set<FieldReference> dereferences = extractRowSubscripts(projectNode.getAssignments().expressions(), false);

        // Exclude dereferences on symbols being used in partitionBy and orderBy
        DataOrganizationSpecification specification = topNRankingNode.getSpecification();
        dereferences = dereferences.stream()
                .filter(expression -> {
                    Symbol symbol = getBase(expression);
                    return !specification.partitionBy().contains(symbol)
                            && !specification.orderingScheme().map(OrderingScheme::orderBy).orElse(ImmutableList.of()).contains(symbol);
                })
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
                        topNRankingNode.replaceChildren(ImmutableList.of(
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        topNRankingNode.getSource(),
                                        Assignments.builder()
                                                .putIdentities(topNRankingNode.getSource().getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()))),
                        newAssignments));
    }
}
