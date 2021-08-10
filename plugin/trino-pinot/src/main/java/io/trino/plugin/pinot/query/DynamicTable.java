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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.PinotColumnHandle;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DynamicTable
{
    private final String tableName;

    private final Optional<String> suffix;

    private final List<String> selections;

    private final Optional<String> filter;

    // semantically aggregation is applied after constraint
    private final List<String> groupingColumns;
    private final List<PinotColumnHandle> aggregateColumns;

    // semantically sorting is applied after aggregation
    private final List<OrderByExpression> orderBy;

    // semantically limit is applied after sorting
    private final OptionalLong limit;
    private final OptionalLong offset;

    private final String query;

    @JsonCreator
    public DynamicTable(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("suffix") Optional<String> suffix,
            @JsonProperty("selections") List<String> selections,
            @JsonProperty("filter") Optional<String> filter,
            @JsonProperty("groupingColumns") List<String> groupingColumns,
            @JsonProperty("aggregateColumns") List<PinotColumnHandle> aggregateColumns,
            @JsonProperty("orderBy") List<OrderByExpression> orderBy,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("offset") OptionalLong offset,
            @JsonProperty("query") String query)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.suffix = requireNonNull(suffix, "suffix is null");
        this.selections = ImmutableList.copyOf(requireNonNull(selections, "selections is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.groupingColumns = ImmutableList.copyOf(requireNonNull(groupingColumns, "groupingColumns is null"));
        this.aggregateColumns = ImmutableList.copyOf(requireNonNull(aggregateColumns, "aggregateColumns is null"));
        this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        this.limit = requireNonNull(limit, "limit is null");
        this.offset = requireNonNull(offset, "offset is null");
        this.query = requireNonNull(query, "query is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<String> getSuffix()
    {
        return suffix;
    }

    @JsonProperty
    public List<String> getSelections()
    {
        return selections;
    }

    @JsonProperty
    public Optional<String> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public List<String> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<PinotColumnHandle> getAggregateColumns()
    {
        return aggregateColumns;
    }

    @JsonProperty
    public List<OrderByExpression> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public OptionalLong getOffset()
    {
        return offset;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }

        if (!(other instanceof DynamicTable)) {
            return false;
        }

        DynamicTable that = (DynamicTable) other;
        return tableName.equals(that.tableName) &&
                selections.equals(that.selections) &&
                filter.equals(that.filter) &&
                groupingColumns.equals(that.groupingColumns) &&
                aggregateColumns.equals(that.aggregateColumns) &&
                orderBy.equals(that.orderBy) &&
                limit.equals(that.limit) &&
                offset.equals(that.offset) &&
                query.equals(that.query);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, selections, filter, groupingColumns, aggregateColumns, orderBy, limit, offset, query);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("selections", selections)
                .add("filter", filter)
                .add("groupingColumns", groupingColumns)
                .add("aggregateColumns", aggregateColumns)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .add("offset", offset)
                .add("query", query)
                .toString();
    }
}
