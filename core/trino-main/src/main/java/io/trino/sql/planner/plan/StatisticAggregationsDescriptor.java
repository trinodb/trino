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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.TableStatisticType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StatisticAggregationsDescriptor<T>
{
    private final Map<String, T> grouping;
    private final Map<TableStatisticType, T> tableStatistics;
    private final Map<ColumnStatisticMetadata, T> columnStatistics;

    public static <T> StatisticAggregationsDescriptor<T> empty()
    {
        return StatisticAggregationsDescriptor.<T>builder().build();
    }

    public StatisticAggregationsDescriptor(
            Map<String, T> grouping,
            Map<TableStatisticType, T> tableStatistics,
            Map<ColumnStatisticMetadata, T> columnStatistics)
    {
        this.grouping = ImmutableMap.copyOf(requireNonNull(grouping, "grouping is null"));
        this.tableStatistics = ImmutableMap.copyOf(requireNonNull(tableStatistics, "tableStatistics is null"));
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    @JsonCreator
    @Deprecated // for JSON serialization only
    public static <T> StatisticAggregationsDescriptor<T> fromJson(
            @JsonProperty("grouping") Map<String, T> grouping,
            @JsonProperty("tableStatistics") Map<TableStatisticType, T> tableStatistics,
            @JsonProperty("columnStatisticsList") List<ColumnStatisticAggregationsDescriptor<T>> columnStatistics)
    {
        return new StatisticAggregationsDescriptor<>(
                grouping,
                tableStatistics,
                columnStatistics.stream()
                        .collect(toImmutableMap(ColumnStatisticAggregationsDescriptor::metadata, ColumnStatisticAggregationsDescriptor::input)));
    }

    @JsonProperty
    public Map<String, T> getGrouping()
    {
        return grouping;
    }

    @JsonProperty
    public Map<TableStatisticType, T> getTableStatistics()
    {
        return tableStatistics;
    }

    @JsonIgnore
    public Map<ColumnStatisticMetadata, T> getColumnStatistics()
    {
        return columnStatistics;
    }

    @JsonProperty
    @Deprecated // for JSON serialization only
    public List<ColumnStatisticAggregationsDescriptor<T>> getColumnStatisticsList()
    {
        return columnStatistics.entrySet().stream()
                .map(entry -> new ColumnStatisticAggregationsDescriptor<T>(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StatisticAggregationsDescriptor<?> that = (StatisticAggregationsDescriptor<?>) o;
        return Objects.equals(grouping, that.grouping) &&
                Objects.equals(tableStatistics, that.tableStatistics) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(grouping, tableStatistics, columnStatistics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("grouping", grouping)
                .add("tableStatistics", tableStatistics)
                .add("columnStatistics", columnStatistics)
                .toString();
    }

    public static <B> Builder<B> builder()
    {
        return new Builder<>();
    }

    public <T2> StatisticAggregationsDescriptor<T2> map(Function<T, T2> mapper)
    {
        return new StatisticAggregationsDescriptor<>(
                map(this.getGrouping(), mapper),
                map(this.getTableStatistics(), mapper),
                map(this.getColumnStatistics(), mapper));
    }

    private static <K, V1, V2> Map<K, V2> map(Map<K, V1> input, Function<V1, V2> mapper)
    {
        return input.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> mapper.apply(entry.getValue())));
    }

    public static class Builder<T>
    {
        private final ImmutableMap.Builder<String, T> grouping = ImmutableMap.builder();
        private final ImmutableMap.Builder<TableStatisticType, T> tableStatistics = ImmutableMap.builder();
        private final ImmutableMap.Builder<ColumnStatisticMetadata, T> columnStatistics = ImmutableMap.builder();

        public void addGrouping(String column, T key)
        {
            grouping.put(column, key);
        }

        public void addTableStatistic(TableStatisticType type, T key)
        {
            tableStatistics.put(type, key);
        }

        public void addColumnStatistic(ColumnStatisticMetadata statisticMetadata, T key)
        {
            columnStatistics.put(statisticMetadata, key);
        }

        public StatisticAggregationsDescriptor<T> build()
        {
            return new StatisticAggregationsDescriptor<>(grouping.buildOrThrow(), tableStatistics.buildOrThrow(), columnStatistics.buildOrThrow());
        }
    }

    public record ColumnStatisticAggregationsDescriptor<T>(ColumnStatisticMetadata metadata, T input) {}
}
