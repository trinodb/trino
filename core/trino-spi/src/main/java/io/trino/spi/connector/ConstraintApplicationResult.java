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

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ConstraintApplicationResult<T>
{
    private final T handle;
    private final TupleDomain<ColumnHandle> remainingFilter;
    private final Optional<ConnectorExpression> remainingExpression;
    private final boolean precalculateStatistics;
    private final List<ConstraintApplicationResult<T>> secondaryMicroPlans;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, boolean precalculateStatistics)
    {
        this(handle, remainingFilter, Optional.empty(), precalculateStatistics, emptyList());
    }

    /**
     * @param remainingExpression the remaining expression, which will be AND-ed with {@code remainingFilter},
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, ConnectorExpression remainingExpression, boolean precalculateStatistics)
    {
        this(handle, remainingFilter, Optional.of(remainingExpression), precalculateStatistics, emptyList());
    }

    public ConstraintApplicationResult(
            T handle,
            TupleDomain<ColumnHandle> remainingFilter,
            ConnectorExpression remainingExpression,
            boolean precalculateStatistics,
            List<ConstraintApplicationResult<T>> secondaryMicroPlans)
    {
        this(handle, remainingFilter, Optional.of(remainingExpression), precalculateStatistics, secondaryMicroPlans);
    }

    /**
     * @param remainingExpression the remaining expression, which will be AND-ed with {@code remainingFilter},
     * or {@link Optional#empty()} if the remaining expression is equal to the original expression.
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    private ConstraintApplicationResult(
            T handle,
            TupleDomain<ColumnHandle> remainingFilter,
            Optional<ConnectorExpression> remainingExpression,
            boolean precalculateStatistics,
            List<ConstraintApplicationResult<T>> secondaryMicroPlans)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        this.precalculateStatistics = precalculateStatistics;
        this.secondaryMicroPlans = requireNonNull(secondaryMicroPlans, "secondaryMicroPlans is null");
    }

    public T getHandle()
    {
        return handle;
    }

    public TupleDomain<ColumnHandle> getRemainingFilter()
    {
        return remainingFilter;
    }

    public Optional<ConnectorExpression> getRemainingExpression()
    {
        return remainingExpression;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }

    public List<ConstraintApplicationResult<T>> getSecondaryMicroPlans()
    {
        return secondaryMicroPlans;
    }

    public <U> ConstraintApplicationResult<U> transform(Function<T, U> transformHandle)
    {
        U transformedHandle = transformHandle.apply(handle);
        List<ConstraintApplicationResult<U>> transformedMicroPlans = secondaryMicroPlans.stream()
                .map(mp -> mp.transform(transformHandle))
                .collect(Collectors.toList());
        return new ConstraintApplicationResult<>(transformedHandle, remainingFilter, remainingExpression, precalculateStatistics, transformedMicroPlans);
    }
}
