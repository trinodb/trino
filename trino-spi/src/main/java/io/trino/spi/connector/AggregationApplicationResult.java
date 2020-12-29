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
package io.prestosql.spi.connector;

import io.prestosql.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AggregationApplicationResult<T>
{
    private final T handle;
    private final List<ConnectorExpression> projections;
    private final List<Assignment> assignments;
    private final Map<ColumnHandle, ColumnHandle> groupingColumnMapping;

    public AggregationApplicationResult(
            T handle,
            List<ConnectorExpression> projections,
            List<Assignment> assignments,
            Map<ColumnHandle, ColumnHandle> groupingColumnMapping)
    {
        this.handle = requireNonNull(handle, "handle is null");
        requireNonNull(groupingColumnMapping, "goupingSetMapping is null");
        requireNonNull(projections, "projections is null");
        requireNonNull(assignments, "assignment is null");
        this.groupingColumnMapping = Map.copyOf(groupingColumnMapping);
        this.projections = List.copyOf(projections);
        this.assignments = List.copyOf(assignments);
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

    public Map<ColumnHandle, ColumnHandle> getGroupingColumnMapping()
    {
        return groupingColumnMapping;
    }
}
