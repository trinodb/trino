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
package io.trino.plugin.pinot.query.ptf;

import com.google.common.collect.ImmutableMap;
import org.apache.pinot.common.request.PinotQuery;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public record SerializablePinotQuery(
        int version,
        Optional<SerializableDataSource> dataSource,
        Optional<List<SerializableExpression>> selectList,
        Optional<SerializableExpression> filterExpression,
        Optional<List<SerializableExpression>> groupByList,
        Optional<List<SerializableExpression>> orderByList,
        Optional<SerializableExpression> havingExpression,
        int limit,
        int offset,
        Optional<Map<String, String>> debugOptions,
        Optional<Map<String, String>> queryOptions,
        boolean explain,
        Optional<Map<SerializableExpression, SerializableExpression>> expressionOverrideHints)
{
    public SerializablePinotQuery
    {
        requireNonNull(dataSource, "dataSource is null");
        requireNonNull(selectList, "selectList is null");
        requireNonNull(filterExpression, "filterExpression is null");
        requireNonNull(groupByList, "groupByList is null");
        requireNonNull(orderByList, "orderByList is null");
        requireNonNull(havingExpression, "havingExpression is null");
        requireNonNull(debugOptions, "debugOptions is null");
        requireNonNull(queryOptions, "queryOptions is null");
        requireNonNull(expressionOverrideHints, "expressionOverrideHints is null");
        debugOptions = debugOptions.map(ImmutableMap::copyOf);
        queryOptions = queryOptions.map(ImmutableMap::copyOf);
    }

    public SerializablePinotQuery(PinotQuery pinotQuery)
    {
        this(pinotQuery.getVersion(),
                Optional.ofNullable(pinotQuery.getDataSource()).map(SerializableDataSource::new),
                Optional.ofNullable(pinotQuery.getSelectList())
                        .map(selectList -> selectList.stream()
                                .map(SerializableExpression::new)
                                .collect(toImmutableList())),
                Optional.ofNullable(pinotQuery.getFilterExpression())
                        .map(SerializableExpression::new),
                Optional.ofNullable(pinotQuery.getGroupByList())
                        .map(selectList -> selectList.stream()
                                .map(SerializableExpression::new)
                                .collect(toImmutableList())),
                Optional.ofNullable(pinotQuery.getOrderByList())
                        .map(selectList -> selectList.stream()
                                .map(SerializableExpression::new)
                                .collect(toImmutableList())),
                Optional.ofNullable(pinotQuery.getHavingExpression())
                        .map(SerializableExpression::new),
                pinotQuery.getLimit(),
                pinotQuery.getOffset(),
                Optional.ofNullable(pinotQuery.getDebugOptions()),
                Optional.ofNullable(pinotQuery.getQueryOptions()),
                pinotQuery.isExplain(),
                Optional.of(pinotQuery.getExpressionOverrideHints())
                        .map(expressionOverrideHints -> expressionOverrideHints.entrySet().stream()
                                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(new SerializableExpression(entry.getKey()), new SerializableExpression(entry.getValue())))
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    public PinotQuery toPinotQuery()
    {
        PinotQuery pinotQuery = new PinotQuery();
        pinotQuery.setVersion(version);
        dataSource.map(SerializableDataSource::toDataSource).ifPresent(pinotQuery::setDataSource);
        selectList.map(selectList -> selectList.stream()
                .map(SerializableExpression::toExpression)
                .collect(toImmutableList()))
                .ifPresent(pinotQuery::setSelectList);
        filterExpression.map(SerializableExpression::toExpression)
                .ifPresent(pinotQuery::setFilterExpression);
        groupByList.map(groupByList -> groupByList.stream()
                        .map(SerializableExpression::toExpression)
                        .collect(toImmutableList()))
                .ifPresent(pinotQuery::setSelectList);
        orderByList.map(orderByList -> orderByList.stream()
                        .map(SerializableExpression::toExpression)
                        .collect(toImmutableList()))
                .ifPresent(pinotQuery::setSelectList);
        havingExpression.map(SerializableExpression::toExpression)
                .ifPresent(pinotQuery::setFilterExpression);
        pinotQuery.setLimit(limit);
        pinotQuery.setOffset(offset);
        debugOptions.ifPresent(pinotQuery::setDebugOptions);
        queryOptions.ifPresent(pinotQuery::setQueryOptions);
        pinotQuery.setExplain(explain);
        expressionOverrideHints.map(expressionOverrideHints -> expressionOverrideHints.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey().toExpression(), entry.getValue().toExpression()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))
                .ifPresent(pinotQuery::setExpressionOverrideHints);
        return pinotQuery;
    }
}
