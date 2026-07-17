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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/// Extract a conjunct common to every branch of a boolean Case out to a top-level conjunction. E.g.,
/// - `Case(When(c, And(X, A)), .., And(X, B)) -> And(X, Case(When(c, A), .., B))`
///
/// Exposes a conjunct common to every branch for predicate pushdown (for example, an equality
/// buried in an IF becomes a hash-join key). A conjunct that may fail is left in place: within a
/// branch a sibling conjunct may short-circuit before it, so hoisting it could surface a failure
/// that the original expression avoids.
public class ExtractCommonConjunctFromCase
        implements IrOptimizerRule
{
    private final PlannerContext context;

    public ExtractCommonConjunctFromCase(PlannerContext context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case caseTerm) || !BOOLEAN.equals(caseTerm.type())) {
            return Optional.empty();
        }

        List<Expression> results = branchResults(caseTerm);
        Set<Expression> common = commonConjuncts(results);
        if (common.isEmpty()) {
            return Optional.empty();
        }

        Case reduced = new Case(
                caseTerm.whenClauses().stream()
                        .map(clause -> new WhenClause(clause.getOperand(), removeConjuncts(clause.getResult(), common)))
                        .collect(toImmutableList()),
                removeConjuncts(caseTerm.defaultValue(), common));

        return Optional.of(combineConjuncts(ImmutableList.<Expression>builderWithExpectedSize(common.size() + 1)
                .addAll(common)
                .add(reduced)
                .build()));
    }

    private static List<Expression> branchResults(Case caseTerm)
    {
        return ImmutableList.<Expression>builder()
                .addAll(caseTerm.whenClauses().stream().map(WhenClause::getResult).collect(toImmutableList()))
                .add(caseTerm.defaultValue())
                .build();
    }

    private Set<Expression> commonConjuncts(List<Expression> results)
    {
        // Deterministic, non-failing, non-trivial conjuncts present in every branch, keyed by structural equality.
        Set<Expression> common = extractConjuncts(results.getFirst()).stream()
                .filter(conjunct -> !conjunct.equals(TRUE) && isDeterministic(conjunct) && !mayFail(context, conjunct))
                .collect(toCollection(LinkedHashSet::new));

        for (int branch = 1; branch < results.size() && !common.isEmpty(); branch++) {
            common.retainAll(extractConjuncts(results.get(branch)));
        }

        return common;
    }

    private static Expression removeConjuncts(Expression expression, Set<Expression> removed)
    {
        return combineConjuncts(Sets.difference(ImmutableSet.copyOf(extractConjuncts(expression)), removed));
    }
}
