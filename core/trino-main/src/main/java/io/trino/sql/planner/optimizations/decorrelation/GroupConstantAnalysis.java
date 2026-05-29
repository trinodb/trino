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

package io.trino.sql.planner.optimizations.decorrelation;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DeterminismEvaluator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.type.TypeCoercion;

import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrUtils.extractConjuncts;

/// Group-constant analysis: which right-side columns are pinned to a single value within every
/// correlation group by the equality conjuncts of a join condition. Extending grouping or dedup
/// keys below a join is sound exactly for these columns; an injective cast of a bare reference
/// pins like the reference itself.
public final class GroupConstantAnalysis
{
    private GroupConstantAnalysis() {}

    /// Base-side symbols that an equality conjunct of the join condition equates to an expression
    /// of the input side only — constant within each outer row's matched subset (legacy
    /// `PlanNodeDecorrelator`'s constant-symbol rule).
    public static Set<Symbol> groupConstantSymbols(Expression condition, Set<Symbol> rightSymbols, TypeCoercion typeCoercion)
    {
        ImmutableSet.Builder<Symbol> constants = ImmutableSet.builder();
        for (Expression conjunct : extractConjuncts(condition)) {
            if (matchComparison(conjunct) instanceof Comparison.Equal(Expression left, Expression right)) {
                equatedRightSymbol(left, right, rightSymbols, typeCoercion).ifPresent(constants::add);
                equatedRightSymbol(right, left, rightSymbols, typeCoercion).ifPresent(constants::add);
            }
        }
        return constants.build();
    }

    private static Optional<Symbol> equatedRightSymbol(Expression side, Expression otherSide, Set<Symbol> rightSymbols, TypeCoercion typeCoercion)
    {
        Expression candidate = side;
        // An injective cast pins its source symbol just as well: equality on the cast value fixes
        // the source value within each outer row's matched set (mirrors legacy
        // PlanNodeDecorrelator's isSimpleInjectiveCast).
        if (candidate instanceof Cast cast
                && cast.expression() instanceof Reference reference
                && typeCoercion.isInjectiveCoercion(Symbol.from(reference).type(), cast.type())) {
            candidate = cast.expression();
        }
        if (candidate instanceof Reference reference
                && rightSymbols.contains(Symbol.from(reference))
                && Sets.intersection(SymbolsExtractor.extractUnique(otherSide), rightSymbols).isEmpty()
                // A non-deterministic equating expression (`t.c = o.k + random()`) is re-sampled
                // per joined row, so it pins nothing — legacy's isConstant has the same clause.
                && DeterminismEvaluator.isDeterministic(otherSide)) {
            return Optional.of(Symbol.from(reference));
        }
        return Optional.empty();
    }
}
