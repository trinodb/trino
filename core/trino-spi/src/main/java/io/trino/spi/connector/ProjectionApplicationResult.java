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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ProjectionApplicationResult<T>
{
    private final T handle;
    private final List<ConnectorExpression> projections;
    private final List<Assignment> assignments;
    private final boolean precalculateStatistics;

    public ProjectionApplicationResult(T handle, List<ConnectorExpression> projections, List<Assignment> assignments, boolean precalculateStatistics)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.projections = List.copyOf(requireNonNull(projections, "projections is null"));
        this.assignments = List.copyOf(requireNonNull(assignments, "assignments is null"));
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle()
    {
        return handle;
    }

    public List<ConnectorExpression> getProjections()
    {
        return projections;
    }

    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }
}
