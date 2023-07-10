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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.bigquery.BigQueryType.convertToString;
import static io.trino.plugin.bigquery.BigQueryUtil.quote;
import static io.trino.plugin.bigquery.BigQueryUtil.toBigQueryColumnName;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class BigQueryFilterQueryBuilder
{
    private final TupleDomain<ColumnHandle> tupleDomain;

    public static Optional<String> buildFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new BigQueryFilterQueryBuilder(tupleDomain).buildFilter();
    }

    private BigQueryFilterQueryBuilder(TupleDomain<ColumnHandle> tupleDomain)
    {
        this.tupleDomain = tupleDomain;
    }

    private Optional<String> buildFilter()
    {
        Optional<Map<ColumnHandle, Domain>> domains = tupleDomain.getDomains();
        return domains.map(this::toConjuncts)
                .map(this::concat);
    }

    private String concat(List<String> clauses)
    {
        return clauses.isEmpty() ? null : clauses.stream().collect(joining(" AND "));
    }

    private List<String> toConjuncts(Map<ColumnHandle, Domain> domains)
    {
        List<BigQueryColumnHandle> columns = domains.keySet().stream().map(BigQueryColumnHandle.class::cast).collect(toList());
        return toConjuncts(columns);
    }

    private List<String> toConjuncts(List<BigQueryColumnHandle> columns)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of("FALSE");
        }
        ImmutableList.Builder<String> clauses = ImmutableList.builder();
        for (BigQueryColumnHandle column : columns) {
            Domain domain = tupleDomain.getDomains().get().get(column);
            if (domain != null) {
                toPredicate(toBigQueryColumnName(column.getName()), domain, column).ifPresent(clauses::add);
            }
        }
        return clauses.build();
    }

    private Optional<String> toPredicate(String columnName, Domain domain, BigQueryColumnHandle column)
    {
        if (domain.getValues().isNone()) {
            String predicate = domain.isNullAllowed() ? quote(columnName) + " IS NULL" : "FALSE";
            return Optional.of(predicate);
        }

        if (domain.getValues().isAll()) {
            String predicate = domain.isNullAllowed() ? "TRUE" : quote(columnName) + " IS NOT NULL";
            return Optional.of(predicate);
        }

        List<String> disjuncts = new ArrayList<>();
        List<String> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                Optional<String> value = convertToString(column.getTrinoType(), column.getBigqueryType(), range.getSingleValue());
                if (value.isEmpty()) {
                    return Optional.empty();
                }
                singleValues.add(value.get());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    Optional<String> predicate = toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), column);
                    if (predicate.isEmpty()) {
                        return Optional.empty();
                    }
                    rangeConjuncts.add(predicate.get());
                }
                if (!range.isHighUnbounded()) {
                    Optional<String> predicate = toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), column);
                    if (predicate.isEmpty()) {
                        return Optional.empty();
                    }
                    rangeConjuncts.add(predicate.get());
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + concat(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            String predicate = quote(columnName) + " = " + getOnlyElement(singleValues);
            disjuncts.add(predicate);
        }
        else if (singleValues.size() > 1) {
            String values = String.join(",", singleValues);
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return Optional.of("(" + String.join(" OR ", disjuncts) + ")");
    }

    private Optional<String> toPredicate(String columnName, String operator, Object value, BigQueryColumnHandle column)
    {
        Optional<String> valueAsString = convertToString(column.getTrinoType(), column.getBigqueryType(), value);
        if (valueAsString.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(quote(columnName) + " " + operator + " " + valueAsString.get());
    }
}
