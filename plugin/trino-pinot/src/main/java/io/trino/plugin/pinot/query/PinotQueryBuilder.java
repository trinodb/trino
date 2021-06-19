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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class PinotQueryBuilder
{
    private PinotQueryBuilder()
    {
    }

    public static String generatePql(PinotTableHandle tableHandle, List<PinotColumnHandle> columnHandles, Optional<String> tableNameSuffix, Optional<String> timePredicate, int limitForSegmentQueries)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        StringBuilder pqlBuilder = new StringBuilder();
        List<String> columnNames;
        if (columnHandles.isEmpty()) {
            // This occurs when the query is SELECT COUNT(*) FROM pinotTable ...
            columnNames = ImmutableList.of("*");
        }
        else {
            columnNames = columnHandles.stream()
                    .map(PinotColumnHandle::getColumnName)
                    .collect(toImmutableList());
        }

        pqlBuilder.append("SELECT ");
        pqlBuilder.append(String.join(", ", columnNames))
                .append(" FROM ")
                .append(getTableName(tableHandle, tableNameSuffix))
                .append(" ");
        generateFilterPql(pqlBuilder, tableHandle, timePredicate, columnHandles);
        OptionalLong appliedLimit = tableHandle.getLimit();
        long limit = limitForSegmentQueries + 1;
        if (appliedLimit.isPresent()) {
            limit = Math.min(limit, appliedLimit.getAsLong());
        }
        pqlBuilder.append(" LIMIT ")
                .append(limit);
        return pqlBuilder.toString();
    }

    private static String getTableName(PinotTableHandle tableHandle, Optional<String> tableNameSuffix)
    {
        if (tableNameSuffix.isPresent()) {
            return new StringBuilder(tableHandle.getTableName())
                    .append(tableNameSuffix.get())
                    .toString();
        }
        return tableHandle.getTableName();
    }

    private static void generateFilterPql(StringBuilder pqlBuilder, PinotTableHandle tableHandle, Optional<String> timePredicate, List<PinotColumnHandle> columnHandles)
    {
        Optional<String> filterClause = getFilterClause(tableHandle.getConstraint(), timePredicate, columnHandles);
        if (filterClause.isPresent()) {
            pqlBuilder.append(" WHERE ")
                    .append(filterClause.get());
        }
    }

    public static Optional<String> getFilterClause(TupleDomain<ColumnHandle> tupleDomain, Optional<String> timePredicate, List<PinotColumnHandle> columnHandles)
    {
        ImmutableList.Builder<String> conjunctsBuilder = ImmutableList.builder();
        timePredicate.ifPresent(conjunctsBuilder::add);
        if (!tupleDomain.equals(TupleDomain.all())) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElseThrow();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                conjunctsBuilder.add(toPredicate(((PinotColumnHandle) entry.getKey()).getColumnName(), entry.getValue()));
            }
        }
        List<String> conjuncts = conjunctsBuilder.build();
        if (!conjuncts.isEmpty()) {
            return Optional.of(Joiner.on(" AND ").join(conjuncts));
        }
        else {
            return Optional.empty();
        }
    }

    private static String toPredicate(String columnName, Domain domain)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(convertValue(range.getType(), range.getSingleValue()));
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(toConjunct(columnName, range.isLowInclusive() ? ">=" : ">", convertValue(range.getType(), range.getLowBoundedValue())));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toConjunct(columnName, range.isHighInclusive() ? "<=" : "<", convertValue(range.getType(), range.getHighBoundedValue())));
                }
                // If rangeConjuncts is null, then the range was ALL, which is not supported in pql
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toConjunct(columnName, "=", getOnlyElement(singleValues)));
            }
            else if (singleValues.size() > 1) {
                disjuncts.add(inClauseValues(columnName, singleValues));
            }
        }
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private static Object convertValue(Type type, Object value)
    {
        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((Long) value));
        }
        return value;
    }

    private static String toConjunct(String columnName, String operator, Object value)
    {
        if (value instanceof Slice) {
            value = ((Slice) value).toStringUtf8();
        }
        return format("%s %s %s", columnName, operator, singleQuote(value));
    }

    private static String inClauseValues(String columnName, List<Object> singleValues)
    {
        return format("%s IN (%s)", columnName, singleValues.stream()
                .map(PinotQueryBuilder::singleQuote)
                .collect(joining(", ")));
    }

    private static String singleQuote(Object value)
    {
        return format("'%s'", value);
    }
}
