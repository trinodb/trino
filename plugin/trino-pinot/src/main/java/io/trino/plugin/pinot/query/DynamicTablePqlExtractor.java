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

import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.pinot.query.PinotQueryBuilder.getFilterClause;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class DynamicTablePqlExtractor
{
    private DynamicTablePqlExtractor()
    {
    }

    public static String extractPql(DynamicTable table, TupleDomain<ColumnHandle> tupleDomain, List<PinotColumnHandle> columnHandles)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("select ");
        if (!table.getSelections().isEmpty()) {
            builder.append(table.getSelections().stream()
                    .map(DynamicTablePqlExtractor::quoteIdentifier)
                    .collect(joining(", ")));
        }
        if (!table.getGroupingColumns().isEmpty()) {
            builder.append(table.getGroupingColumns().stream()
                    .map(DynamicTablePqlExtractor::quoteIdentifier)
                    .collect(joining(", ")));
            if (!table.getAggregateColumns().isEmpty()) {
                builder.append(", ");
            }
        }
        builder.append(table.getAggregateColumns().stream()
                .map(PinotColumnHandle::getColumnName)
                .collect(joining(", ")));
        builder.append(" from ");
        builder.append(table.getTableName());
        builder.append(table.getSuffix().orElse(""));

        Optional<String> filter = getFilter(table.getFilter(), tupleDomain, columnHandles);
        if (filter.isPresent()) {
            builder.append(" where ")
                    .append(filter.get());
        }
        if (!table.getGroupingColumns().isEmpty()) {
            builder.append(" group by ");
            builder.append(table.getGroupingColumns().stream()
                    .map(DynamicTablePqlExtractor::quoteIdentifier)
                    .collect(joining(", ")));
        }
        if (!table.getOrderBy().isEmpty()) {
            builder.append(" order by ")
                    .append(table.getOrderBy().stream()
                            .map(DynamicTablePqlExtractor::convertOrderByExpressionToPql)
                            .collect(joining(", ")));
        }
        if (table.getLimit().isPresent()) {
            builder.append(" limit ")
                    .append(table.getLimit().getAsLong());
            if (!table.getSelections().isEmpty() && table.getOffset().isPresent()) {
                builder.append(", ")
                        .append(table.getOffset().getAsLong());
            }
        }
        return builder.toString();
    }

    private static Optional<String> getFilter(Optional<String> filter, TupleDomain<ColumnHandle> tupleDomain, List<PinotColumnHandle> columnHandles)
    {
        Optional<String> tupleFilter = getFilterClause(tupleDomain, Optional.empty(), columnHandles);

        if (tupleFilter.isPresent() && filter.isPresent()) {
            return Optional.of(format("%s AND %s", encloseInParentheses(tupleFilter.get()), encloseInParentheses(filter.get())));
        }
        else if (filter.isPresent()) {
            return filter;
        }
        else if (tupleFilter.isPresent()) {
            return tupleFilter;
        }
        else {
            return Optional.empty();
        }
    }

    private static String convertOrderByExpressionToPql(OrderByExpression orderByExpression)
    {
        requireNonNull(orderByExpression, "orderByExpression is null");
        StringBuilder builder = new StringBuilder()
                .append(quoteIdentifier(orderByExpression.getColumn()));
        if (!orderByExpression.isAsc()) {
            builder.append(" desc");
        }
        return builder.toString();
    }

    public static String encloseInParentheses(String value)
    {
        return format("(%s)", value);
    }

    private static String quoteIdentifier(String identifier)
    {
        checkArgument(!identifier.contains("\""), "Identifier contains double quotes: '%s'", identifier);
        return format("\"%s\"", identifier);
    }
}
