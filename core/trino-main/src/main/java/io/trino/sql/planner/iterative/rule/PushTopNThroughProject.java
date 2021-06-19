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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.exclusiveDereferences;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.topN;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - TopN
 *    - Project (non-identity)
 *       - Source other than Filter(TableScan) or TableScan
 * </pre>
 * Into:
 * <pre>
 * - Project
 *    - TopN
 *       - Source
 * </pre>
 */
public final class PushTopNThroughProject
        implements Rule<TopNNode>
{
    private static final Capture<ProjectNode> PROJECT_CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN =
            topN()
                    .with(source().matching(
                            project()
                                    // do not push topN through identity projection which could be there for column pruning purposes
                                    .matching(projectNode -> !projectNode.isIdentity())
                                    .capturedAs(PROJECT_CHILD)
                                    // do not push topN between projection and table scan so that they can be merged into a PageProcessor
                                    .with(source().matching(node -> !(node instanceof TableScanNode)))));
    private final TypeAnalyzer typeAnalyzer;

    public PushTopNThroughProject(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode parent, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_CHILD);

        // Do not push down if the projection is made up of symbol references and exclusive dereferences. This prevents
        // undoing of PushDownDereferencesThroughTopN. We still push topN in the case of overlapping dereferences since
        // it enables PushDownDereferencesThroughTopN rule to push optimal dereferences.
        Set<Expression> projections = ImmutableSet.copyOf(projectNode.getAssignments().getExpressions());
        if (!extractRowSubscripts(projections, false, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes()).isEmpty()
                && exclusiveDereferences(projections, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes())) {
            return Result.empty();
        }

        // do not push topN between projection and filter(table scan) so that they can be merged into a PageProcessor
        PlanNode projectSource = context.getLookup().resolve(projectNode.getSource());
        if (projectSource instanceof FilterNode) {
            PlanNode filterSource = context.getLookup().resolve(((FilterNode) projectSource).getSource());
            if (filterSource instanceof TableScanNode) {
                return Result.empty();
            }
        }

        Optional<SymbolMapper> symbolMapper = symbolMapper(parent.getOrderingScheme().getOrderBy(), projectNode.getAssignments());
        if (symbolMapper.isEmpty()) {
            return Result.empty();
        }

        TopNNode mappedTopN = symbolMapper.get().map(parent, projectNode.getSource(), context.getIdAllocator().getNextId());
        return Result.ofPlanNode(projectNode.replaceChildren(ImmutableList.of(mappedTopN)));
    }

    private Optional<SymbolMapper> symbolMapper(List<Symbol> symbols, Assignments assignments)
    {
        SymbolMapper.Builder mapper = SymbolMapper.builder();
        for (Symbol symbol : symbols) {
            Expression expression = assignments.get(symbol);
            if (!(expression instanceof SymbolReference)) {
                return Optional.empty();
            }
            mapper.put(symbol, Symbol.from(expression));
        }
        return Optional.of(mapper.build());
    }
}
