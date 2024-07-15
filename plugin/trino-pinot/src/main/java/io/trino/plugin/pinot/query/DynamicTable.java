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
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.PinotColumnHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * @param groupingColumns semantically aggregation is applied after constraint
 * @param orderBy semantically sorting is applied after aggregation
 * @param limit semantically limit is applied after sorting
 */
public record DynamicTable(
        String tableName,
        Optional<String> suffix,
        List<PinotColumnHandle> projections,
        Optional<String> filter,
        List<PinotColumnHandle> groupingColumns,
        List<PinotColumnHandle> aggregateColumns,
        Optional<String> havingExpression,
        List<OrderByExpression> orderBy,
        OptionalLong limit,
        OptionalLong offset,
        Map<String, String> queryOptions,
        String query)
{
    public DynamicTable
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(suffix, "suffix is null");
        projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        requireNonNull(filter, "filter is null");
        groupingColumns = ImmutableList.copyOf(requireNonNull(groupingColumns, "groupingColumns is null"));
        aggregateColumns = ImmutableList.copyOf(requireNonNull(aggregateColumns, "aggregateColumns is null"));
        requireNonNull(havingExpression, "havingExpression is null");
        orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        requireNonNull(limit, "limit is null");
        requireNonNull(offset, "offset is null");
        queryOptions = ImmutableMap.copyOf(requireNonNull(queryOptions, "queryOptions is null"));
        requireNonNull(query, "query is null");
    }

    public boolean aggregateInProjections()
    {
        return projections.stream()
                .anyMatch(PinotColumnHandle::isAggregate);
    }
}
