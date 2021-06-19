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
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 *      Filter(f1(A.x.y) = 1 AND f2(B.m) = 2 AND f3(A.x) = 6)
 *          Source(A, B, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(A, B, C)
 *      Filter(f1(D) = 1 AND f2(E) = 2 AND f3(G) = 6)
 *          Project(A, B, C, D := A.x.y, E := B.m, G := A.x)
 *              Source(A, B, C)
 * </pre>
 * <p>
 * This optimizer extracts all dereference expressions from a filter node located above a table scan into a ProjectNode.
 * <p>
 * Extracting dereferences from a filter (eg. FilterNode(a.x = 5)) can be suboptimal if full columns are being accessed up the
 * plan tree (eg. a), because it can result in replicated shuffling of fields (eg. a.x). So it is safer to pushdown dereferences from
 * Filter only when there's an explicit projection on top of the filter node (Ref PushDereferencesThroughFilter).
 * <p>
 * In case of a FilterNode on top of TableScanNode, we want to push all dereferences into a new ProjectNode below, so that
 * PushProjectionIntoTableScan optimizer can push those columns in the connector, and provide new column handles for the
 * projected subcolumns. PushPredicateIntoTableScan optimizer can then push predicates on these subcolumns into the connector.
 */
public class ExtractDereferencesFromFilterAboveScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;

    public ExtractDereferencesFromFilterAboveScan(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return filter()
                .with(source().matching(tableScan().capturedAs(CHILD)));
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        Set<SubscriptExpression> dereferences = extractRowSubscripts(ImmutableList.of(node.getPredicate()), true, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes());
        if (dereferences.isEmpty()) {
            return Result.empty();
        }

        Assignments assignments = Assignments.of(dereferences, context.getSession(), context.getSymbolAllocator(), typeAnalyzer);
        Map<Expression, SymbolReference> mappings = HashBiMap.create(assignments.getMap())
                .inverse()
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

        PlanNode source = node.getSource();
        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                new FilterNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                source,
                                Assignments.builder()
                                        .putIdentities(source.getOutputSymbols())
                                        .putAll(assignments)
                                        .build()),
                        replaceExpression(node.getPredicate(), mappings)),
                Assignments.identity(node.getOutputSymbols())));
    }
}
