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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.Set;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.exclusiveDereferences;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractRowSubscripts;
import static io.trino.sql.planner.iterative.rule.Util.transpose;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

public class PushLimitThroughProject
        implements Rule<LimitNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(
                    project()
                            // do not push limit through identity projection which could be there for column pruning purposes
                            .matching(projectNode -> !projectNode.isIdentity())
                            .capturedAs(CHILD)));

    private final TypeAnalyzer typeAnalyzer;

    public PushLimitThroughProject(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(CHILD);

        // Do not push down if the projection is made up of symbol references and exclusive dereferences. This prevents
        // undoing of PushDownDereferencesThroughLimit. We still push limit in the case of overlapping dereferences since
        // it enables PushDownDereferencesThroughLimit rule to push optimal dereferences.
        Set<Expression> projections = ImmutableSet.copyOf(projectNode.getAssignments().getExpressions());
        if (!extractRowSubscripts(projections, false, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes()).isEmpty()
                && exclusiveDereferences(projections, context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes())) {
            return Result.empty();
        }

        // for a LimitNode without ties and pre-sorted inputs, simply reorder the nodes
        if (!parent.isWithTies() && !parent.requiresPreSortedInputs()) {
            return Result.ofPlanNode(transpose(parent, projectNode));
        }

        // for a LimitNode with ties, the tiesResolvingScheme must be rewritten in terms of symbols before projection
        SymbolMapper.Builder symbolMapper = SymbolMapper.builder();
        Set<Symbol> symbolsForRewrite = ImmutableSet.<Symbol>builder()
                .addAll(parent.getPreSortedInputs())
                .addAll(parent.getTiesResolvingScheme().map(OrderingScheme::getOrderBy).orElse(ImmutableList.of()))
                .build();
        for (Symbol symbol : symbolsForRewrite) {
            Expression expression = projectNode.getAssignments().get(symbol);
            // if a symbol results from some computation, the translation fails
            if (!(expression instanceof SymbolReference)) {
                return Result.empty();
            }
            symbolMapper.put(symbol, Symbol.from(expression));
        }

        LimitNode mappedLimitNode = symbolMapper.build().map(parent, projectNode.getSource());
        return Result.ofPlanNode(projectNode.replaceChildren(ImmutableList.of(mappedLimitNode)));
    }
}
