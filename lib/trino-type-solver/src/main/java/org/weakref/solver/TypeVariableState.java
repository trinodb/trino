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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Mutable state for a single type-kind variable during solving.
 * <p>
 * Tracks three complementary pieces of information:
 * <ul>
 *   <li><b>Bounds</b> — lower and upper types the variable must lie between. Added as
 *       {@link Subtype} constraints flow through the solver.</li>
 *   <li><b>Domain</b> — the set of concrete {@link Alternative} witnesses still feasible;
 *       narrowed by intersection whenever new coercion candidates arrive.</li>
 *   <li><b>Binding</b> — a concrete expression once the variable is committed. Once set,
 *       all subsequent bounds/candidates must be consistent or the solver throws
 *       {@link UnsatisfiableException}.</li>
 * </ul>
 * A family restriction ({@code mustBeRow}) exists to handle {@link Expression.AnyRow} —
 * "the variable must be <i>some</i> row type." This is the only family flag in the model.
 */
public final class TypeVariableState
        implements VariableState
{
    private Optional<Expression> binding = Optional.empty();
    private Optional<Set<Expression>> lowerBounds = Optional.empty();
    private Optional<Set<Expression>> upperBounds = Optional.empty();
    private boolean mustBeRow;
    private final Domain domain = new Domain();

    /**
     * Commit the variable to {@code expression}. If already bound, fails unless the new
     * expression is identical. Sets both the upper and lower bound to the binding so
     * subsequent bound checks become trivial equality tests.
     */
    public void bind(Expression expression)
    {
        verifyRowRestriction(expression);
        if (binding.isPresent() && !binding.orElseThrow().equals(expression)) {
            throw new IllegalStateException("Variable is already bound to " + binding.orElseThrow());
        }
        this.binding = Optional.of(expression);
        this.lowerBounds = Optional.of(Set.of(expression));
        this.upperBounds = Optional.of(Set.of(expression));
    }

    public void addUpperBound(Expression expression)
    {
        verifyRowRestriction(expression);
        if (binding.isPresent()) {
            throw new IllegalStateException("Variable is already bound to " + binding.orElseThrow());
        }
        upperBounds = append(upperBounds, expression);
    }

    public void addLowerBound(Expression expression)
    {
        verifyRowRestriction(expression);
        if (binding.isPresent()) {
            throw new IllegalStateException("Variable is already bound to " + binding.orElseThrow());
        }
        lowerBounds = append(lowerBounds, expression);
    }

    public void restrictToRow()
    {
        mustBeRow = true;
        binding.ifPresent(this::verifyRowRestriction);
        lowerBounds.ifPresent(bounds -> bounds.forEach(this::verifyRowRestriction));
        upperBounds.ifPresent(bounds -> bounds.forEach(this::verifyRowRestriction));
    }

    public boolean mustBeRow()
    {
        return mustBeRow;
    }

    public boolean isCompatible(Expression expression)
    {
        return !mustBeRow || isRowType(expression);
    }

    private void verifyRowRestriction(Expression expression)
    {
        if (expression instanceof Expression.Variable) {
            return;
        }
        if (!isCompatible(expression)) {
            throw new UnsatisfiableException("Expression " + expression + " is not a row type, but variable is restricted to row");
        }
    }

    private static boolean isRowType(Expression expression)
    {
        return expression instanceof Expression.Row || expression instanceof Expression.AnyRow;
    }

    private static Optional<Set<Expression>> append(Optional<Set<Expression>> bounds, Expression expression)
    {
        if (bounds.isEmpty()) {
            HashSet<Expression> set = new HashSet<>();
            set.add(expression);
            return Optional.of(set);
        }
        else {
            bounds.orElseThrow().add(expression);
            return bounds;
        }
    }

    public Optional<Set<Expression>> lowerBounds()
    {
        return lowerBounds;
    }

    public Optional<Set<Expression>> upperBounds()
    {
        return upperBounds;
    }

    public Optional<Expression> binding()
    {
        return binding;
    }

    public Domain domain()
    {
        return domain;
    }

    @Override
    public String toString()
    {
        return "[" + (mustBeRow ? "mustBeRow, " : "") + "lower=" + lowerBounds + ", upper=" + upperBounds + ", domain=" + domain + "]";
    }
}
