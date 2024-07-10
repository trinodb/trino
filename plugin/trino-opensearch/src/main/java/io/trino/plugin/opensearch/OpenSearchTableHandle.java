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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record OpenSearchTableHandle(
        Type type,
        String schema,
        String index,
        TupleDomain<ColumnHandle> constraint,
        Map<String, String> regexes,
        Optional<String> query,
        OptionalLong limit,
        Set<OpenSearchColumnHandle> columns)
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY
    }

    public OpenSearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this(
                type,
                schema,
                index,
                TupleDomain.all(),
                ImmutableMap.of(),
                query,
                OptionalLong.empty(),
                ImmutableSet.of());
    }

    public OpenSearchTableHandle withColumns(Set<OpenSearchColumnHandle> columns)
    {
        return new OpenSearchTableHandle(
                type,
                schema,
                index,
                constraint,
                regexes,
                query,
                limit,
                columns);
    }

    public OpenSearchTableHandle
    {
        requireNonNull(type, "type is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(index, "index is null");
        requireNonNull(constraint, "constraint is null");
        regexes = ImmutableMap.copyOf(requireNonNull(regexes, "regexes is null"));
        columns = ImmutableSet.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(query, "query is null");
        requireNonNull(limit, "limit is null");
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
        limit.ifPresent(value -> attributes.append("limit=" + value));
        query.ifPresent(value -> attributes.append("query" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
