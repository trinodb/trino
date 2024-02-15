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
package io.trino.spi.connector;

import com.google.errorprone.annotations.FormatMethod;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConstraintApplicationResult<T>
{
    private final boolean retainOriginalPlan;
    private final List<Alternative<T>> alternatives;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     *
     * @deprecated use {@link #ConstraintApplicationResult(boolean, java.util.List)}
     */
    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, boolean precalculateStatistics)
    {
        this(false, List.of(new Alternative<>(handle, remainingFilter, Optional.empty(), precalculateStatistics)));
    }

    /**
     * @param remainingExpression the remaining expression, which will be AND-ed with {@code remainingFilter},
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     * @deprecated use {@link #ConstraintApplicationResult(boolean, java.util.List)}
     */
    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, ConnectorExpression remainingExpression, boolean precalculateStatistics)
    {
        this(false, List.of(new Alternative<>(handle, remainingFilter, Optional.of(remainingExpression), precalculateStatistics)));
    }

    /**
     * @param retainOriginalPlan use the original plan node as the first alternative.
     * @param alternatives a non-empty list of alternative plans the connector would like to choose from at the worker.
     * Different alternatives must have different handles.
     * The engine might prune alternatives from the end of the list, therefore:
     * 1. Elements should be ordered by priority.
     * 2. If {@code retainOriginalPlan} is {@code false}, then the first element
     * should be the most pessimistic (an alternative that can be used for all splits).
     */
    public ConstraintApplicationResult(boolean retainOriginalPlan, List<Alternative<T>> alternatives)
    {
        checkArgument(!alternatives.isEmpty(), "Must supply at least one alternative");
        // this check is for discouraging duplicated alternatives and because later on we use handle as a map key
        checkArgument(alternatives.size() == alternatives.stream().map(alternative -> alternative.handle).distinct().count(), "Different alternatives must have different handles");
        this.retainOriginalPlan = retainOriginalPlan;
        this.alternatives = requireNonNull(alternatives, "alternatives is null");
    }

    /**
     * @deprecated use {@link Alternative#handle()} of each Alternative returned from {@link #getAlternatives()}
     */
    @Deprecated
    public T getHandle()
    {
        return getSingleTransformedAlternative().handle();
    }

    /**
     * @deprecated use {@link Alternative#remainingFilter()} of each Alternative returned from {@link #getAlternatives()}
     */
    @Deprecated
    public TupleDomain<ColumnHandle> getRemainingFilter()
    {
        return getSingleTransformedAlternative().remainingFilter();
    }

    /**
     * @deprecated use {@link Alternative#remainingExpression()} of each Alternative returned from {@link #getAlternatives()}
     */
    @Deprecated
    public Optional<ConnectorExpression> getRemainingExpression()
    {
        return getSingleTransformedAlternative().remainingExpression();
    }

    /**
     * @deprecated use {@link Alternative#precalculateStatistics()} of each Alternative returned from {@link #getAlternatives()}
     */
    @Deprecated
    public boolean isPrecalculateStatistics()
    {
        return getSingleTransformedAlternative().precalculateStatistics();
    }

    public boolean isRetainOriginalPlan()
    {
        return retainOriginalPlan;
    }

    public List<Alternative<T>> getAlternatives()
    {
        return alternatives;
    }

    public <U> ConstraintApplicationResult<U> transform(Function<T, U> transformHandle)
    {
        return new ConstraintApplicationResult<>(
                retainOriginalPlan,
                alternatives.stream().map(alternative -> alternative.transform(transformHandle)).toList());
    }

    private Alternative<T> getSingleTransformedAlternative()
    {
        checkArgument(!retainOriginalPlan, "Plan is not transformed (the original plan is to be retained)");
        checkArgument(alternatives.size() == 1, "There are more than one alternative");
        return alternatives.get(0);
    }

    @FormatMethod
    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }

    public record Alternative<T>(T handle, TupleDomain<ColumnHandle> remainingFilter, Optional<ConnectorExpression> remainingExpression, boolean precalculateStatistics)
    {
        /**
         * @param remainingExpression the remaining expression, which will be AND-ed with {@code remainingFilter},
         * or {@link Optional#empty()} if the remaining expression is equal to the original expression.
         * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
         * as the connector may be unable to provide good table statistics for {@code handle}. This parameter is only taken into account
         * for the first alternative and only if {@code retainOriginalPlan} is {@code false}.
         */
        public Alternative(
                T handle,
                TupleDomain<ColumnHandle> remainingFilter,
                Optional<ConnectorExpression> remainingExpression,
                boolean precalculateStatistics)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
            this.precalculateStatistics = precalculateStatistics;
        }

        public <U> Alternative<U> transform(Function<T, U> transformHandle)
        {
            return new Alternative<>(
                    transformHandle.apply(handle()),
                    remainingFilter,
                    remainingExpression,
                    precalculateStatistics);
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", Alternative.class.getSimpleName() + "[", "]")
                    .add("handle=" + handle)
                    .add("remainingFilter=" + remainingFilter)
                    .add("remainingExpression=" + remainingExpression)
                    .add("precalculateStatistics=" + precalculateStatistics)
                    .toString();
        }
    }
}
