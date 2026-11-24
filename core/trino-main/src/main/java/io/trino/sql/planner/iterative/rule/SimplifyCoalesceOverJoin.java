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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Removes {@code COALESCE} operands made redundant by the join below, e.g. in the
 * {@code coalesce(l, r)} that {@code JOIN ... USING (k)} produces for {@code k}:
 * <ul>
 * <li>INNER: operands after a join key are dropped, since a join key is never null
 * <li>LEFT and RIGHT: null-producing operands after a preserved-side key are dropped, since a
 * null key has no match. A null-producing key followed by its preserved-side counterpart is
 * dropped if {@link EqualityInference} allows substituting one for the other: a non-null
 * null-producing key implies a match.
 * </ul>
 * So {@code coalesce(l, r)} becomes {@code l} for INNER and LEFT, and {@code r} for RIGHT.
 */
public class SimplifyCoalesceOverJoin
{
    private final PlannerContext plannerContext;

    public SimplifyCoalesceOverJoin(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectOverJoinRule(),
                filterOverJoinRule());
    }

    public Rule<ProjectNode> projectOverJoinRule()
    {
        return new ProjectOverJoin();
    }

    public Rule<FilterNode> filterOverJoinRule()
    {
        return new FilterOverJoin();
    }

    private final class ProjectOverJoin
            implements Rule<ProjectNode>
    {
        private static final Capture<JoinNode> JOIN = newCapture();
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(join().capturedAs(JOIN)));

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            JoinNode join = captures.get(JOIN);
            Assignments assignments = node.getAssignments().rewrite(expression -> simplify(expression, join, context.getSession()));
            if (assignments.equals(node.getAssignments())) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(node.getId(), join, assignments));
        }
    }

    private final class FilterOverJoin
            implements Rule<FilterNode>
    {
        private static final Capture<JoinNode> JOIN = newCapture();
        private static final Pattern<FilterNode> PATTERN = filter()
                .with(source().matching(join().capturedAs(JOIN)));

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            JoinNode join = captures.get(JOIN);
            Expression predicate = simplify(node.getPredicate(), join, context.getSession());
            if (predicate.equals(node.getPredicate())) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(node.getId(), join, predicate));
        }
    }

    private Expression simplify(Expression expression, JoinNode join, Session session)
    {
        if (join.getCriteria().isEmpty()) {
            return expression;
        }
        JoinSimplifier simplifier = switch (join.getType()) {
            case INNER -> new InnerJoinSimplifier(join);
            case LEFT -> new OuterJoinSimplifier(join, session, false);
            case RIGHT -> new OuterJoinSimplifier(join, session, true);
            case FULL -> operands -> operands;
        };
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteCoalesce(Coalesce node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Coalesce rewritten = treeRewriter.defaultRewrite(node, context);
                List<Expression> operands = simplifier.simplify(rewritten.operands());
                if (operands.equals(rewritten.operands())) {
                    return rewritten;
                }
                if (operands.size() == 1) {
                    return operands.getFirst();
                }
                return new Coalesce(operands);
            }
        }, expression);
    }

    private interface JoinSimplifier
    {
        List<Expression> simplify(List<Expression> operands);
    }

    private static class InnerJoinSimplifier
            implements JoinSimplifier
    {
        private final Set<Symbol> keys;

        public InnerJoinSimplifier(JoinNode join)
        {
            keys = join.getCriteria().stream()
                    .flatMap(clause -> ImmutableList.of(clause.getLeft(), clause.getRight()).stream())
                    .collect(toImmutableSet());
        }

        @Override
        public List<Expression> simplify(List<Expression> operands)
        {
            for (int i = 0; i < operands.size(); i++) {
                if (operands.get(i) instanceof Reference reference && keys.contains(Symbol.from(reference))) {
                    return operands.subList(0, i + 1);
                }
            }
            return operands;
        }
    }

    private final class OuterJoinSimplifier
            implements JoinSimplifier
    {
        private final Set<Symbol> preservedKeys;
        private final Set<Symbol> preservedSymbols;
        private final Set<Symbol> nullProducingKeys;
        private final Set<Symbol> nullProducingSymbols;
        private final EqualityInference inference;

        public OuterJoinSimplifier(JoinNode join, Session session, boolean rightPreserved)
        {
            List<EquiJoinClause> criteria = rightPreserved
                    ? join.getCriteria().stream().map(EquiJoinClause::flip).collect(toImmutableList())
                    : join.getCriteria();
            preservedKeys = criteria.stream().map(EquiJoinClause::getLeft).collect(toImmutableSet());
            nullProducingKeys = criteria.stream().map(EquiJoinClause::getRight).collect(toImmutableSet());
            preservedSymbols = ImmutableSet.copyOf((rightPreserved ? join.getRight() : join.getLeft()).getOutputSymbols());
            nullProducingSymbols = ImmutableSet.copyOf((rightPreserved ? join.getLeft() : join.getRight()).getOutputSymbols());
            inference = new EqualityInference(
                    plannerContext,
                    getCharVarcharCoercion(session),
                    criteria.stream()
                            .map(clause -> clause.toExpression(plannerContext.getMetadata(), getCharVarcharCoercion(session)))
                            .collect(toImmutableList()));
        }

        @Override
        public List<Expression> simplify(List<Expression> operands)
        {
            ImmutableList.Builder<Expression> result = ImmutableList.builder();
            boolean afterPreservedKey = false;
            for (int i = 0; i < operands.size(); i++) {
                Expression operand = operands.get(i);
                if (operand instanceof Reference reference) {
                    Symbol symbol = Symbol.from(reference);
                    if (afterPreservedKey && nullProducingSymbols.contains(symbol)) {
                        continue;
                    }
                    if (nullProducingKeys.contains(symbol) && i + 1 < operands.size() && isEquivalentPreservedSymbol(operands.get(i + 1), reference)) {
                        continue;
                    }
                    if (preservedKeys.contains(symbol)) {
                        afterPreservedKey = true;
                    }
                }
                result.add(operand);
            }
            return result.build();
        }

        private boolean isEquivalentPreservedSymbol(Expression candidate, Reference nullProducingKey)
        {
            if (!(candidate instanceof Reference reference) || !preservedSymbols.contains(Symbol.from(reference))) {
                return false;
            }
            Expression rewritten = inference.rewrite(nullProducingKey, preservedSymbols);
            return rewritten != null && rewritten.equals(inference.rewrite(reference, preservedSymbols));
        }
    }
}
