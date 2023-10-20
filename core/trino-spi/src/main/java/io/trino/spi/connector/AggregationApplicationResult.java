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
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationApplicationResult<T>
{
    private final T handle;
    private final Optional<List<ConnectorExpression>> projections;
    private final Optional<List<AggregateFunction>> aggregations;
    private final List<Assignment> assignments;
    private final Map<ColumnHandle, ColumnHandle> groupingColumnMapping;
    private final boolean precalculateStatistics;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public AggregationApplicationResult(
            T handle,
            List<ConnectorExpression> projections,
            List<Assignment> assignments,
            Map<ColumnHandle, ColumnHandle> groupingColumnMapping,
            boolean precalculateStatistics)
    {
        this(handle, Optional.of(projections), Optional.empty(), assignments, groupingColumnMapping, precalculateStatistics);
    }

    public AggregationApplicationResult(
            T handle,
            Optional<List<ConnectorExpression>> projections,
            Optional<List<AggregateFunction>> aggregations,
            List<Assignment> assignments,
            Map<ColumnHandle, ColumnHandle> groupingColumnMapping,
            boolean precalculateStatistics)
    {
        this.handle = requireNonNull(handle, "handle is null");
        requireNonNull(groupingColumnMapping, "groupingColumnMapping is null");
        requireNonNull(projections, "projections is null");
        requireNonNull(assignments, "assignments is null");
        this.groupingColumnMapping = Map.copyOf(groupingColumnMapping);
        checkArgument(projections.isPresent() != aggregations.isPresent(), "Expected exactly one to be present from: %s, %s", projections, aggregations);
        this.projections = projections.map(List::copyOf);
        this.aggregations = aggregations.map(List::copyOf);
        this.assignments = List.copyOf(assignments);
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle()
    {
        return handle;
    }

    public Optional<List<ConnectorExpression>> getProjections()
    {
        return projections;
    }

    public Optional<List<AggregateFunction>> getAggregations()
    {
        return aggregations;
    }

    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    public Map<ColumnHandle, ColumnHandle> getGroupingColumnMapping()
    {
        return groupingColumnMapping;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }
}
