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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import jakarta.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.Logical.Operator.AND;

/**
 * A lower and an upper bound on the same primitive column inside a conjunction are evaluated as one columnar kernel.
 * Evaluated separately, the second bound is a gather pass through the positions the first one kept; fused, both
 * comparisons run in the single sequential pass over the column, without a branch between them.
 * <p>
 * Only primitive columns qualify. A second primitive comparison costs less than the gather pass it replaces, while a
 * second slice comparison, or a branch that skips it, costs more than the pass. Measured on bigint and double the fused
 * kernel took about half the time of the pair at any selectivity; on varchar it took 40 percent longer.
 */
final class SameColumnRange
{
    private SameColumnRange() {}

    /**
     * Replaces each lower bound and upper bound over the same reference by a two-term AND of the two comparisons, placed
     * where the earlier of the two stood. Other terms are left in place.
     */
    static List<Expression> fuseTerms(List<Expression> terms)
    {
        Expression[] fused = terms.toArray(new Expression[0]);
        boolean[] consumed = new boolean[fused.length];
        Map<Symbol, Integer> pendingLower = new HashMap<>();
        Map<Symbol, Integer> pendingUpper = new HashMap<>();
        for (int i = 0; i < fused.length; i++) {
            Bound bound = matchBound(fused[i]);
            if (bound == null) {
                continue;
            }
            Map<Symbol, Integer> complements = bound.lower() ? pendingUpper : pendingLower;
            Integer partner = complements.remove(bound.symbol());
            if (partner == null) {
                Map<Symbol, Integer> pending = bound.lower() ? pendingLower : pendingUpper;
                pending.putIfAbsent(bound.symbol(), i);
                continue;
            }
            Expression lower = bound.lower() ? fused[i] : fused[partner];
            Expression upper = bound.lower() ? fused[partner] : fused[i];
            fused[partner] = new Logical(AND, ImmutableList.of(lower, upper));
            consumed[i] = true;
        }

        ImmutableList.Builder<Expression> result = ImmutableList.builderWithExpectedSize(fused.length);
        for (int i = 0; i < fused.length; i++) {
            if (!consumed[i]) {
                result.add(fused[i]);
            }
        }
        return result.build();
    }

    /**
     * Whether the expression is the two-term AND produced by {@link #fuseTerms}: a lower bound followed by an upper bound
     * on the same reference.
     */
    static boolean isFusedRange(Expression expression)
    {
        if (!(expression instanceof Logical logical) || logical.operator() != AND || logical.terms().size() != 2) {
            return false;
        }
        Bound lower = matchBound(logical.terms().get(0));
        Bound upper = matchBound(logical.terms().get(1));
        return lower != null && upper != null && lower.lower() && !upper.lower() && lower.symbol().equals(upper.symbol());
    }

    private record Bound(Symbol symbol, boolean lower) {}

    // comparisons are canonical: greater-than forms arrive as less-than with flipped operands, so a constant
    // on the left is a lower bound of the reference and a constant on the right is an upper bound
    @Nullable
    private static Bound matchBound(Expression expression)
    {
        Comparison comparison = matchComparison(expression);
        if (!(comparison instanceof Comparison.LessThan) && !(comparison instanceof Comparison.LessThanOrEqual)) {
            return null;
        }
        if (comparison.left() instanceof Constant constant && constant.value() != null && comparison.right() instanceof Reference reference && isPrimitive(reference)) {
            return new Bound(Symbol.from(reference), true);
        }
        if (comparison.left() instanceof Reference reference && comparison.right() instanceof Constant constant && constant.value() != null && isPrimitive(reference)) {
            return new Bound(Symbol.from(reference), false);
        }
        return null;
    }

    private static boolean isPrimitive(Reference reference)
    {
        return reference.type().getJavaType().isPrimitive();
    }
}
