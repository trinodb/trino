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
package org.weakref.solver;

/**
 * Pairwise specificity comparison for overload resolution.
 * <p>
 * Implementations define a partial order over {@link FunctionResolver.Resolution}s. The
 * {@link FunctionResolver} consults this partial order to pick a unique "most specific"
 * winner: any candidate strictly dominated by another is dropped; if exactly one survives,
 * it's the resolution. If none dominate any other, the call is ambiguous.
 * <p>
 * <b>Contract.</b> Implementations must satisfy:
 * <ul>
 *   <li>Reversibility: {@code compare(a, b).reverse() == compare(b, a)} for all {@code a, b}.</li>
 *   <li>Reflexivity: {@code compare(a, a) == EQUIVALENT}.</li>
 * </ul>
 * Implementations are NOT required to be transitive — {@link Order#INCOMPARABLE} is a
 * first-class outcome, and transitivity would promote it to equivalence, which is wrong.
 * <p>
 * <b>Scope.</b> {@code Specificity} only orders <i>already-applicable</i> resolutions. Picking
 * which candidates apply is the {@link Solver}'s job; picking among them is this interface's.
 * <p>
 * <b>Composition.</b> Use {@link #then} to fall through to a secondary comparator whenever the
 * primary returns {@link Order#INCOMPARABLE} or {@link Order#EQUIVALENT}. This is how dialect
 * presets layer richer rules on top of a minimal framework default.
 */
@FunctionalInterface
public interface Specificity
{
    /**
     * Compare two applicable resolutions.
     */
    Order compare(FunctionResolver.Resolution left, FunctionResolver.Resolution right);

    /**
     * Fall through to {@code next} when {@code this} can't distinguish two candidates.
     */
    default Specificity then(Specificity next)
    {
        return (left, right) -> {
            Order result = this.compare(left, right);
            return (result == Order.INCOMPARABLE || result == Order.EQUIVALENT)
                    ? next.compare(left, right)
                    : result;
        };
    }

    /**
     * Framework-default specificity: prefer the candidate that needed fewer coercions.
     * <p>
     * This is the only ranking dimension the framework itself takes an opinion on, and it's
     * universally uncontroversial — every type system of any kind prefers the match that
     * requires fewer conversions. When two candidates tie on count, this comparator returns
     * {@link Order#INCOMPARABLE}; dialects with richer rules should layer their own
     * comparator via {@link #then} to break such ties.
     */
    Specificity BY_COERCION_COUNT = (left, right) -> {
        int leftCount = coercionCount(left);
        int rightCount = coercionCount(right);
        if (leftCount < rightCount) {
            return Order.MORE_SPECIFIC;
        }
        if (leftCount > rightCount) {
            return Order.LESS_SPECIFIC;
        }
        return Order.INCOMPARABLE;
    };

    private static int coercionCount(FunctionResolver.Resolution resolution)
    {
        return (int) resolution.argumentCoercions().stream()
                .filter(FunctionResolver.ArgumentCoercion::coercionNeeded)
                .count();
    }

    enum Order
    {
        MORE_SPECIFIC,
        LESS_SPECIFIC,
        EQUIVALENT,
        INCOMPARABLE;

        public Order reverse()
        {
            return switch (this) {
                case MORE_SPECIFIC -> LESS_SPECIFIC;
                case LESS_SPECIFIC -> MORE_SPECIFIC;
                case EQUIVALENT, INCOMPARABLE -> this;
            };
        }
    }
}
