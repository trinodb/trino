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
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.codec.binary.Hex;

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
        List<String> quotedColumnNames;
        if (columnHandles.isEmpty()) {
            // This occurs when the query is SELECT COUNT(*) FROM pinotTable ...
            quotedColumnNames = ImmutableList.of("*");
        }
        else {
            quotedColumnNames = columnHandles.stream()
                    .map(column -> quoteIdentifier(column.getColumnName()))
                    .collect(toImmutableList());
        }

        pqlBuilder.append("SELECT ");
        pqlBuilder.append(String.join(", ", quotedColumnNames))
                .append(" FROM ")
                .append(getTableName(tableHandle, tableNameSuffix))
                .append(" ");
        generateFilterPql(pqlBuilder, tableHandle, timePredicate);
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

    private static void generateFilterPql(StringBuilder pqlBuilder, PinotTableHandle tableHandle, Optional<String> timePredicate)
    {
        Optional<String> filterClause = getFilterClause(tableHandle.getConstraint(), timePredicate, false);
        if (filterClause.isPresent()) {
            pqlBuilder.append(" WHERE ")
                    .append(filterClause.get());
        }
    }

    public static Optional<String> getFilterClause(TupleDomain<ColumnHandle> tupleDomain, Optional<String> timePredicate, boolean forHavingClause)
    {
        ImmutableList.Builder<String> conjunctsBuilder = ImmutableList.builder();
        checkState((forHavingClause && timePredicate.isEmpty()) || !forHavingClause, "Unexpected time predicate with having clause");
        timePredicate.ifPresent(conjunctsBuilder::add);
        if (!tupleDomain.equals(TupleDomain.all())) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElseThrow();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) entry.getKey();
                // If this is for a having clause, only include aggregate columns.
                // If this is for a where clause, only include non-aggregate columns.
                // i.e. (forHavingClause && isAggregate) || (!forHavingClause && !isAggregate)
                if (forHavingClause == pinotColumnHandle.isAggregate()) {
                    conjunctsBuilder.add(toPredicate(pinotColumnHandle, entry.getValue()));
                }
            }
        }
        List<String> conjuncts = conjunctsBuilder.build();
        if (!conjuncts.isEmpty()) {
            return Optional.of(Joiner.on(" AND ").join(conjuncts));
        }
        return Optional.empty();
    }

    private static String toPredicate(PinotColumnHandle pinotColumnHandle, Domain domain)
    {
        String predicateArgument = pinotColumnHandle.isAggregate() ? pinotColumnHandle.getExpression() : quoteIdentifier(pinotColumnHandle.getColumnName());
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
                    rangeConjuncts.add(toConjunct(predicateArgument, range.isLowInclusive() ? ">=" : ">", convertValue(range.getType(), range.getLowBoundedValue())));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toConjunct(predicateArgument, range.isHighInclusive() ? "<=" : "<", convertValue(range.getType(), range.getHighBoundedValue())));
                }
                // If rangeConjuncts is null, then the range was ALL, which is not supported in pql
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }
        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toConjunct(predicateArgument, "=", getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(inClauseValues(predicateArgument, singleValues));
        }
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private static Object convertValue(Type type, Object value)
    {
        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((Long) value));
        }
        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return Hex.encodeHexString(((Slice) value).getBytes());
        }
        if (type instanceof TimestampType) {
            return toMillis((Long) value);
        }
        return value;
    }

    private static Long toMillis(Long value)
    {
        if (value == null) {
            return null;
        }
        return Timestamps.epochMicrosToMillisWithRounding(value);
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

    private static String quoteIdentifier(String identifier)
    {
        return format("\"%s\"", identifier.replaceAll("\"", "\"\""));
    }
}
