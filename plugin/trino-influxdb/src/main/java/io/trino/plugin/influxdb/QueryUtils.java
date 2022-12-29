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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.DiscreteValues;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import org.influxdb.dto.Query;
import org.influxdb.querybuilder.SelectQueryImpl;
import org.influxdb.querybuilder.SelectionQueryImpl;
import org.influxdb.querybuilder.WhereNested;
import org.influxdb.querybuilder.WhereQueryImpl;
import org.influxdb.querybuilder.clauses.AndConjunction;
import org.influxdb.querybuilder.clauses.NestedClause;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.influxdb.TimestampUtils.unixTimestamp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.eq;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.gt;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.gte;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.lt;
import static org.influxdb.querybuilder.BuiltQuery.QueryBuilder.lte;

public class QueryUtils
{
    private QueryUtils() {}

    public static Query buildQueryCommand(InfluxTableHandle tableHandle, List<InfluxColumnHandle> columnHandles)
    {
        SelectionQueryImpl selectionQuery = QueryBuilder.select();
        appendProjection(selectionQuery, tableHandle, columnHandles);
        SelectQueryImpl selectQuery = selectionQuery.from(tableHandle.getSchemaName(), wrapQuota(tableHandle.getTableName()));
        appendWhereClause(selectQuery, tableHandle);
        tableHandle.getLimit().ifPresent(selectQuery::limit);
        return selectQuery;
    }

    private static void appendProjection(SelectionQueryImpl query, InfluxTableHandle tableHandle, List<InfluxColumnHandle> columnHandles)
    {
        List<String> columns = buildProjection(tableHandle.getProjections());
        if (!columns.isEmpty()) {
            columns.stream()
                    .map(QueryUtils::wrapQuota)
                    .forEach(query::column);
        }
        else {
            columnHandles.stream()
                    .map(col -> wrapQuota(col.getName()))
                    .forEach(query::column);
        }
    }

    private static List<String> buildProjection(List<ColumnHandle> assignments)
    {
        if (assignments.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<String> columns = ImmutableList.builder();
        assignments.stream()
                .map(InfluxColumnHandle.class::cast)
                .map(InfluxColumnHandle::getName)
                .forEach(columns::add);
        return columns.build();
    }

    private static void appendWhereClause(SelectQueryImpl query, InfluxTableHandle tableHandle)
    {
        TupleDomain<ColumnHandle> constraint = tableHandle.getConstraint();
        constraint.getDomains().ifPresent(columnHandleDomainMap -> {
            WhereQueryImpl<SelectQueryImpl> where = query.where();
            columnHandleDomainMap.forEach((columnHandle, domain) ->
                    appendDomainFilter(where, (InfluxColumnHandle) columnHandle, domain));
        });
    }

    private static void appendDomainFilter(WhereQueryImpl<SelectQueryImpl> where, InfluxColumnHandle columnHandle, Domain domain)
    {
        String columnName = wrapQuota(columnHandle.getName());
        domain.getValues().getValuesProcessor().consume(
                ranges -> appendDomainRangeFilter(where, columnName, ranges),
                discreteValues -> appendDomainDiscreteFilter(where, columnName, discreteValues),
                allOrNone -> {});
    }

    private static void appendDomainRangeFilter(WhereQueryImpl<SelectQueryImpl> where, String columnName, Ranges ranges)
    {
        List<Range> orderedRanges = ranges.getOrderedRanges();
        WhereNested<WhereQueryImpl<SelectQueryImpl>> nestedWhere = where.andNested();
        if (orderedRanges.stream().allMatch(x -> x.getType() instanceof TimestampType)) {
            appendDomainTimeRangeFilter(nestedWhere, columnName, orderedRanges);
        }
        else if (orderedRanges.stream().allMatch(x -> x.getType() == DOUBLE || x.getType() == BIGINT)) {
            appendDomainNumRangeFilter(nestedWhere, columnName, orderedRanges);
        }
        else if (orderedRanges.stream().allMatch(x -> x.getType() == BOOLEAN)) {
            appendDomainBooleanRangeFilter(nestedWhere, columnName, orderedRanges);
        }
        else if (orderedRanges.stream().allMatch(x -> x.getType() == VARCHAR)) {
            appendDomainVarcharRangeFilter(nestedWhere, columnName, orderedRanges);
        }
        nestedWhere.close();
    }

    private static void appendDomainTimeRangeFilter(WhereNested<WhereQueryImpl<SelectQueryImpl>> where, String columnName, List<Range> ranges)
    {
        for (Range range : ranges) {
            if (range.isSingleValue()) {
                Object singleValue = range.getSingleValue();
                LongTimestamp timestamp = (LongTimestamp) singleValue;
                where.or(eq(columnName, unixTimestamp(timestamp)));
            }
            else {
                boolean lowInclusive = range.isLowInclusive();
                Optional<Object> lowValue = range.getLowValue();
                boolean highInclusive = range.isHighInclusive();
                Optional<Object> highValue = range.getHighValue();

                if (lowValue.isPresent() && highValue.isPresent()) {
                    long lowTimestamp = unixTimestamp((LongTimestamp) lowValue.get());
                    AndConjunction lowConjunction = new AndConjunction(lowInclusive ? gte(columnName, lowTimestamp) : gt(columnName, lowTimestamp));
                    long highTimestamp = unixTimestamp((LongTimestamp) highValue.get());
                    AndConjunction highConjunction = new AndConjunction(highInclusive ? lte(columnName, highTimestamp) : lt(columnName, highTimestamp));
                    where.or(new NestedClause(ImmutableList.of(lowConjunction, highConjunction)));
                }
                else if (lowValue.isPresent()) {
                    long lowTimestamp = unixTimestamp((LongTimestamp) lowValue.get());
                    where.or(lowInclusive ? gte(columnName, lowTimestamp) : gt(columnName, lowTimestamp));
                }
                else if (highValue.isPresent()) {
                    long highTimestamp = unixTimestamp((LongTimestamp) highValue.get());
                    where.or(highInclusive ? lte(columnName, highTimestamp) : lt(columnName, highTimestamp));
                }
            }
        }
    }

    private static void appendDomainNumRangeFilter(WhereNested<WhereQueryImpl<SelectQueryImpl>> where, String columnName, List<Range> ranges)
    {
        for (Range range : ranges) {
            if (range.isSingleValue()) {
                Object singleValue = range.getSingleValue();
                if (singleValue instanceof Double doubleValue) {
                    BigDecimal value = new BigDecimal(doubleValue.toString());
                    if ((value.toString()).contains("E") || (value.toString()).contains("e")) {
                        where.and(eq(1, 1));
                    }
                    else {
                        where.or(eq(columnName, value));
                    }
                }
                else if (singleValue instanceof Long longValue) {
                    where.or(eq(columnName, longValue));
                }
            }
            else {
                boolean lowInclusive = range.isLowInclusive();
                Optional<Object> lowValue = range.getLowValue();
                boolean highInclusive = range.isHighInclusive();
                Optional<Object> highValue = range.getHighValue();

                if (lowValue.isPresent() && highValue.isPresent()) {
                    AndConjunction lowConjunction = new AndConjunction(lowInclusive ? gte(columnName, lowValue.get()) : gt(columnName, lowValue.get()));
                    AndConjunction highConjunction = new AndConjunction(highInclusive ? lte(columnName, highValue.get()) : lt(columnName, highValue.get()));
                    where.or(new NestedClause(ImmutableList.of(lowConjunction, highConjunction)));
                }
                else if (lowValue.isPresent()) {
                    Object value = lowValue.get();
                    where.or(lowInclusive ? gte(columnName, value) : gt(columnName, value));
                }
                else if (highValue.isPresent()) {
                    Object value = highValue.get();
                    where.or(highInclusive ? lte(columnName, value) : lt(columnName, value));
                }
            }
        }
    }

    private static void appendDomainBooleanRangeFilter(WhereNested<WhereQueryImpl<SelectQueryImpl>> where, String columnName, List<Range> ranges)
    {
        for (Range range : ranges) {
            if (range.isSingleValue()) {
                Boolean singleValue = (Boolean) range.getSingleValue();
                where.and(eq(columnName, singleValue));
            }
        }
    }

    private static void appendDomainVarcharRangeFilter(WhereNested<WhereQueryImpl<SelectQueryImpl>> where, String columnName, List<Range> ranges)
    {
        for (Range range : ranges) {
            if (range.isSingleValue()) {
                Object singleValue = range.getSingleValue();
                singleValue = ((Slice) singleValue).toStringUtf8();
                where.and(eq(columnName, singleValue));
            }
        }
    }

    private static void appendDomainDiscreteFilter(WhereQueryImpl<SelectQueryImpl> where, String columnName, DiscreteValues discreteValues)
    {
        WhereNested<WhereQueryImpl<SelectQueryImpl>> whereNested = where.andNested();
        for (Object value : discreteValues.getValues()) {
            whereNested.or(eq(columnName, value));
        }
        whereNested.close();
    }

    private static String wrapQuota(String column)
    {
        return "\"" + column + "\"";
    }
}
