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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record ElasticsearchTableHandle(
        Type type,
        String schema,
        String index,
        TupleDomain<ColumnHandle> constraint,
        Map<String, String> regexes,
        Map<String, String> prefixes,
        Optional<String> query,
        OptionalLong limit,
        List<ElasticsearchColumnSort> sortOrder,
        Set<ElasticsearchColumnHandle> columns,
        Optional<ElasticsearchAggregation> aggregation)
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY
    }

    public ElasticsearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this(type,
                schema,
                index,
                TupleDomain.all(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                query,
                OptionalLong.empty(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty());
    }

    public ElasticsearchTableHandle withColumns(Set<ElasticsearchColumnHandle> columns)
    {
        return new ElasticsearchTableHandle(
                type,
                schema,
                index,
                constraint,
                regexes,
                prefixes,
                query,
                limit,
                sortOrder,
                columns,
                aggregation);
    }

    public ElasticsearchTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new ElasticsearchTableHandle(
                type,
                schema,
                index,
                constraint,
                regexes,
                prefixes,
                query,
                limit,
                sortOrder,
                columns,
                aggregation);
    }

    public ElasticsearchTableHandle withTopN(long limit, List<ElasticsearchColumnSort> sortOrder)
    {
        return new ElasticsearchTableHandle(
                type,
                schema,
                index,
                constraint,
                regexes,
                prefixes,
                query,
                OptionalLong.of(limit),
                sortOrder,
                columns,
                aggregation);
    }

    public ElasticsearchTableHandle
    {
        requireNonNull(type, "type is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(index, "index is null");
        requireNonNull(constraint, "constraint is null");
        regexes = ImmutableMap.copyOf(regexes);
        prefixes = ImmutableMap.copyOf(prefixes);
        columns = ImmutableSet.copyOf(columns);
        sortOrder = ImmutableList.copyOf(sortOrder);
        requireNonNull(query, "query is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(aggregation, "aggregation is null");
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(type + ":" + index);

        StringBuilder attributes = new StringBuilder();
        if (!regexes.isEmpty()) {
            attributes.append("regexes=[");
            attributes.append(regexes.entrySet().stream()
                    .map(regex -> regex.getKey() + ":" + regex.getValue())
                    .collect(Collectors.joining(", ")));
            attributes.append("]");
        }
        if (!prefixes.isEmpty()) {
            attributes.append("prefixes=[");
            attributes.append(prefixes.entrySet().stream()
                    .map(prefix -> prefix.getKey() + ":" + prefix.getValue())
                    .collect(Collectors.joining(", ")));
            attributes.append("]");
        }
        limit.ifPresent(value -> attributes.append("limit=" + value));
        if (!sortOrder.isEmpty()) {
            attributes.append("sort=" + sortOrder);
        }
        query.ifPresent(value -> attributes.append("query" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
