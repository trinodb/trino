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

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class JoinApplicationResult<T>
{
    private final T tableHandle;
    private final Map<ColumnHandle, ColumnHandle> leftColumnHandles;
    private final Map<ColumnHandle, ColumnHandle> rightColumnHandles;
    private final boolean precalculateStatistics;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public JoinApplicationResult(
            T tableHandle,
            Map<ColumnHandle, ColumnHandle> leftColumnHandles,
            Map<ColumnHandle, ColumnHandle> rightColumnHandles,
            boolean precalculateStatistics)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.leftColumnHandles = Map.copyOf(leftColumnHandles);
        this.rightColumnHandles = Map.copyOf(rightColumnHandles);
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getTableHandle()
    {
        return tableHandle;
    }

    public Map<ColumnHandle, ColumnHandle> getLeftColumnHandles()
    {
        return leftColumnHandles;
    }

    public Map<ColumnHandle, ColumnHandle> getRightColumnHandles()
    {
        return rightColumnHandles;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }
}
