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

import io.trino.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;

public class ConstraintApplicationResult<T>
{
    private final T handle;
    private final TupleDomain<ColumnHandle> remainingFilter;
    private final boolean precalculateStatistics;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public ConstraintApplicationResult(T handle, TupleDomain<ColumnHandle> remainingFilter, boolean precalculateStatistics)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.remainingFilter = requireNonNull(remainingFilter, "remainingFilter is null");
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle()
    {
        return handle;
    }

    public TupleDomain<ColumnHandle> getRemainingFilter()
    {
        return remainingFilter;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }
}
