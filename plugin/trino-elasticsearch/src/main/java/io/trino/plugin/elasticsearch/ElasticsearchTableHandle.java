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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.plugin.elasticsearch.expression.TopN;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record ElasticsearchTableHandle(
        Type type,
        String schema,
        String index,
        TupleDomain<ColumnHandle> constraint,
        Map<String, String> regexes,
        Optional<String> query,
        List<TermAggregation> termAggregations,
        List<MetricAggregation> metricAggregations,
        Optional<TopN> topN)
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN, QUERY, AGGREGATION
    }

    public ElasticsearchTableHandle(Type type, String schema, String index, Optional<String> query)
    {
        this(
                type,
                schema,
                index,
                TupleDomain.all(),
                ImmutableMap.of(),
                query,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.empty());
    }

    public ElasticsearchTableHandle
    {
        requireNonNull(type, "type is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(index, "index is null");
        requireNonNull(constraint, "constraint is null");
        regexes = ImmutableMap.copyOf(requireNonNull(regexes, "regexes is null"));
        requireNonNull(query, "query is null");
        requireNonNull(termAggregations, "aggTerms is null");
        requireNonNull(metricAggregations, "aggregates is null");
        requireNonNull(topN, "topN is null");
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
        query.ifPresent(value -> attributes.append("query" + value));
        topN.ifPresent(value -> attributes.append("topN=" + value));

        if (attributes.length() > 0) {
            builder.append("(");
            builder.append(attributes);
            builder.append(")");
        }

        return builder.toString();
    }
}
