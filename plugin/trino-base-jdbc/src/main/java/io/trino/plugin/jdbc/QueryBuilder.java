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
package io.trino.plugin.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryBuilder
{
    private static final Logger log = Logger.get(QueryBuilder.class);

    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private final JdbcClient client;

    private static class BoundValue
    {
        private final WriteFunction writeFunction;
        private final Object value;

        public BoundValue(WriteFunction writeFunction, Object value)
        {
            this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
            this.value = requireNonNull(value, "value is null");
        }

        public WriteFunction getWriteFunction()
        {
            return writeFunction;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public QueryBuilder(JdbcClient client)
    {
        this.client = requireNonNull(client, "jdbcClient is null");
    }

    public PreparedStatement buildSql(
            ConnectorSession session,
            Connection connection,
            RemoteTableName remoteTableName,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<String> additionalPredicate,
            Function<String, String> sqlFunction)
            throws SQLException
    {
        String sql = "SELECT " + getProjection(columns);
        sql += " FROM " + getRelation(remoteTableName);

        List<BoundValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(client, session, connection, tupleDomain, accumulator);
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get())
                    .build();
        }
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }

        sql += getGroupBy(groupingSets);

        String query = sqlFunction.apply(sql);
        log.debug("Preparing query: %s", query);
        PreparedStatement statement = client.getPreparedStatement(connection, query);

        for (int i = 0; i < accumulator.size(); i++) {
            BoundValue boundValue = accumulator.get(i);
            int parameterIndex = i + 1;
            WriteFunction writeFunction = boundValue.getWriteFunction();
            Class<?> javaType = writeFunction.getJavaType();
            Object value = boundValue.getValue();
            if (javaType == boolean.class) {
                ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, (boolean) value);
            }
            else if (javaType == long.class) {
                ((LongWriteFunction) writeFunction).set(statement, parameterIndex, (long) value);
            }
            else if (javaType == double.class) {
                ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, (double) value);
            }
            else if (javaType == Slice.class) {
                ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, (Slice) value);
            }
            else {
                ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, value);
            }
        }

        return statement;
    }

    protected String getRelation(RemoteTableName remoteTableName)
    {
        return client.quoted(remoteTableName);
    }

    protected String getProjection(List<JdbcColumnHandle> columns)
    {
        if (columns.isEmpty()) {
            return "1";
        }
        return columns.stream()
                .map(jdbcColumnHandle -> format("%s AS %s", jdbcColumnHandle.toSqlExpression(client::quoted), client.quoted(jdbcColumnHandle.getColumnName())))
                .collect(joining(", "));
    }

    private static Domain pushDownDomain(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain)
    {
        return client.toTrinoType(session, connection, column.getJdbcTypeHandle())
                .orElseThrow(() -> new IllegalStateException(format("Unsupported type %s with handle %s", column.getColumnType(), column.getJdbcTypeHandle())))
                .getPredicatePushdownController().apply(session, domain).getPushedDown();
    }

    private List<String> toConjuncts(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            TupleDomain<ColumnHandle> tupleDomain,
            List<BoundValue> accumulator)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of(ALWAYS_FALSE);
        }
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            JdbcColumnHandle column = ((JdbcColumnHandle) entry.getKey());
            Domain domain = pushDownDomain(client, session, connection, column, entry.getValue());
            builder.add(toPredicate(session, connection, column, domain, accumulator));
        }
        return builder.build();
    }

    private String toPredicate(ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain, List<BoundValue> accumulator)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? client.quoted(column.getColumnName()) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : client.quoted(column.getColumnName()) + " IS NOT NULL";
        }

        String predicate = toPredicate(session, connection, column, domain.getValues(), accumulator);
        if (!domain.isNullAllowed()) {
            return predicate;
        }
        return format("(%s OR %s IS NULL)", predicate, client.quoted(column.getColumnName()));
    }

    private String toPredicate(ConnectorSession session, Connection connection, JdbcColumnHandle column, ValueSet valueSet, List<BoundValue> accumulator)
    {
        checkArgument(!valueSet.isNone(), "none values should be handled earlier");

        if (!valueSet.isDiscreteSet()) {
            ValueSet complement = valueSet.complement();
            if (complement.isDiscreteSet()) {
                return format("NOT (%s)", toPredicate(session, connection, column, complement, accumulator));
            }
        }

        WriteFunction writeFunction = getWriteFunction(session, connection, column);

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(column, writeFunction, ">", range.getLow().getValue(), accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(column, writeFunction, ">=", range.getLow().getValue(), accumulator));
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
                            rangeConjuncts.add(toPredicate(column, writeFunction, "<=", range.getHigh().getValue(), accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(column, writeFunction, "<", range.getHigh().getValue(), accumulator));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                if (rangeConjuncts.size() == 1) {
                    disjuncts.add(getOnlyElement(rangeConjuncts));
                }
                else {
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(column, writeFunction, "=", getOnlyElement(singleValues), accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(writeFunction, value, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), writeFunction.getBindExpression()));
            disjuncts.add(client.quoted(column.getColumnName()) + " IN (" + values + ")");
        }

        checkState(!disjuncts.isEmpty());
        if (disjuncts.size() == 1) {
            return getOnlyElement(disjuncts);
        }
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(JdbcColumnHandle column, WriteFunction writeFunction, String operator, Object value, List<BoundValue> accumulator)
    {
        bindValue(writeFunction, value, accumulator);
        return format("%s %s %s", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
    }

    private WriteFunction getWriteFunction(ConnectorSession session, Connection connection, JdbcColumnHandle column)
    {
        WriteFunction writeFunction = client.toTrinoType(session, connection, column.getJdbcTypeHandle())
                .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s for %s", column.getColumnType(), column.getJdbcTypeHandle(), column)))
                .getWriteFunction();
        verify(writeFunction.getJavaType() == column.getColumnType().getJavaType(), "Java type mismatch for %s: %s, %s", column, writeFunction, column.getColumnType());
        return writeFunction;
    }

    private String getGroupBy(Optional<List<List<JdbcColumnHandle>>> groupingSets)
    {
        if (groupingSets.isEmpty()) {
            return "";
        }

        verify(!groupingSets.get().isEmpty());
        if (groupingSets.get().size() == 1) {
            List<JdbcColumnHandle> groupingSet = getOnlyElement(groupingSets.get());
            if (groupingSet.isEmpty()) {
                // global aggregation
                return "";
            }
            return " GROUP BY " + groupingSet.stream()
                    .map(JdbcColumnHandle::getColumnName)
                    .map(client::quoted)
                    .collect(joining(", "));
        }
        return " GROUP BY GROUPING SETS " +
                groupingSets.get().stream()
                        .map(groupingSet -> groupingSet.stream()
                                .map(JdbcColumnHandle::getColumnName)
                                .map(client::quoted)
                                .collect(joining(", ", "(", ")")))
                        .collect(joining(", ", "(", ")"));
    }

    private static void bindValue(WriteFunction writeFunction, Object value, List<BoundValue> accumulator)
    {
        accumulator.add(new BoundValue(writeFunction, value));
    }
}
