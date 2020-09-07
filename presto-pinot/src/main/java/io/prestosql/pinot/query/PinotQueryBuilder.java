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
package io.prestosql.pinot.query;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.pinot.PinotColumnHandle;
import io.prestosql.pinot.PinotTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
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
            for (PinotColumnHandle columnHandle : columnHandles) {
                Domain domain = tupleDomain.getDomains().get().get(columnHandle);
                if (domain != null) {
                    conjunctsBuilder.add(toPredicate(columnHandle.getColumnName(), domain));
                }
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
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toConjunct(columnName, ">", range.getLow().getValue()));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toConjunct(columnName, ">=", range.getLow().getValue()));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toConjunct(columnName, "<=", range.getHigh().getValue()));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toConjunct(columnName, "<", range.getHigh().getValue()));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
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
                disjuncts.add(format("%s IN (%s)", inClauseValues(columnName, singleValues)));
            }
        }
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
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
