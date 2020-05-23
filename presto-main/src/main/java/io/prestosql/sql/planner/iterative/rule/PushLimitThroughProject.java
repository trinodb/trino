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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.SymbolMapper;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Set;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.exclusiveDereferences;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.extractDereferences;
import static io.prestosql.sql.planner.iterative.rule.Util.transpose;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

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
        if (!extractDereferences(projections, false).isEmpty() && exclusiveDereferences(projections)) {
            return Result.empty();
        }

        // for a LimitNode without ties, simply reorder the nodes
        if (!parent.isWithTies()) {
            return Result.ofPlanNode(transpose(parent, projectNode));
        }

        // for a LimitNode with ties, the tiesResolvingScheme must be rewritten in terms of symbols before projection
        SymbolMapper.Builder symbolMapper = SymbolMapper.builder();
        for (Symbol symbol : parent.getTiesResolvingScheme().get().getOrderBy()) {
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
