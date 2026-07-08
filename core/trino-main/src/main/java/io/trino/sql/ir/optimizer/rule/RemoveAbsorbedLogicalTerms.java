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
package io.trino.sql.ir.optimizer.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Objects.requireNonNull;

/// Apply the absorption laws of boolean algebra, which hold in three-valued logic. E.g.,
/// - `Or(a, And(a, b)) -> a`
/// - `And(a, Or(a, b)) -> a`
/// - `Or(And(a, b), And(a, b, c)) -> And(a, b)`
///
/// In general, a term is absorbed when the sub-terms of another term are a subset of its own
/// sub-terms. The shared sub-terms must be deterministic — otherwise the two occurrences may
/// evaluate to different values — and the absorbed extra sub-terms must not fail, since
/// dropping them also drops their error.
public class RemoveAbsorbedLogicalTerms
        implements IrOptimizerRule
{
    private final PlannerContext plannerContext;

    public RemoveAbsorbedLogicalTerms(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Logical(Logical.Operator operator, List<Expression> terms))) {
            return Optional.empty();
        }

        Logical.Operator innerOperator = operator == AND ? OR : AND;

        List<Set<Expression>> subTerms = terms.stream()
                .map(term -> subTerms(term, innerOperator))
                .toList();

        boolean[] absorbed = new boolean[terms.size()];
        for (int i = 0; i < terms.size(); i++) {
            if (!(terms.get(i) instanceof Logical inner) || inner.operator() != innerOperator) {
                // a term without sub-terms can only be absorbed by an equal term, which
                // RemoveRedundantLogicalTerms already handles
                continue;
            }
            for (int j = 0; j < terms.size(); j++) {
                if (j != i && absorbs(subTerms.get(j), subTerms.get(i), j < i)) {
                    absorbed[i] = true;
                    break;
                }
            }
        }

        ImmutableList.Builder<Expression> remaining = ImmutableList.builderWithExpectedSize(terms.size());
        for (int i = 0; i < terms.size(); i++) {
            if (!absorbed[i]) {
                remaining.add(terms.get(i));
            }
        }

        List<Expression> newTerms = remaining.build();
        if (newTerms.size() == terms.size()) {
            return Optional.empty();
        }

        if (newTerms.size() == 1) {
            return Optional.of(newTerms.getFirst());
        }

        return Optional.of(new Logical(operator, newTerms));
    }

    private boolean absorbs(Set<Expression> absorber, Set<Expression> term, boolean firstWinsTie)
    {
        if (!term.containsAll(absorber)) {
            return false;
        }

        if (absorber.size() == term.size() && !firstWinsTie) {
            // terms with equal sub-term sets absorb each other; keep the first one
            return false;
        }

        return absorber.stream()
                .allMatch(DeterminismEvaluator::isDeterministic) && Sets.difference(term, absorber).stream().
        noneMatch(extra -> mayFail(plannerContext, extra));
    }

    private static Set<Expression> subTerms(Expression term, Logical.Operator operator)
    {
        if (term instanceof Logical(Logical.Operator innerOperator, List<Expression> terms) && innerOperator == operator) {
            return ImmutableSet.copyOf(terms);
        }
        return ImmutableSet.of(term);
    }
}
