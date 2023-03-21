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
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DefaultQueryBuilder
        implements QueryBuilder
{
    private static final Logger log = Logger.get(DefaultQueryBuilder.class);

    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private final RemoteQueryModifier queryModifier;

    @Inject
    public DefaultQueryBuilder(RemoteQueryModifier queryModifier)
    {
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
    }

    @Override
    public PreparedQuery prepareSelectQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcRelationHandle baseRelation,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ParameterizedExpression> additionalPredicate)
    {
        if (!tupleDomain.isNone()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElseThrow();
            columns.stream()
                    .filter(domains::containsKey)
                    .filter(column -> columnExpressions.containsKey(column.getColumnName()))
                    .findFirst()
                    .ifPresent(column -> { throw new IllegalArgumentException(format("Column %s has an expression and a constraint attached at the same time", column)); });
        }

        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();

        String sql = "SELECT " + getProjection(client, columns, columnExpressions, accumulator::add);
        sql += getFrom(client, baseRelation, accumulator::add);

        toConjuncts(client, session, connection, tupleDomain, conjuncts, accumulator::add);
        additionalPredicate.ifPresent(predicate -> {
            conjuncts.add(predicate.expression());
            accumulator.addAll(predicate.parameters());
        });
        List<String> clauses = conjuncts.build();
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }

        sql += getGroupBy(client, groupingSets);

        return new PreparedQuery(sql, accumulator.build());
    }

    @Override
    public PreparedQuery prepareJoinQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> leftAssignments,
            Map<JdbcColumnHandle, String> rightAssignments)
    {
        // Verify assignments are present. This is safe assumption as join conditions are not pruned, and simplifies the code here.
        verify(!leftAssignments.isEmpty(), "leftAssignments is empty");
        verify(!rightAssignments.isEmpty(), "rightAssignments is empty");
        // Joins wih no conditions are not pushed down, so it is a same assumption and simplifies the code here
        verify(!joinConditions.isEmpty(), "joinConditions is empty");

        String leftRelationAlias = "l";
        String rightRelationAlias = "r";

        String query = format(
                "SELECT %s, %s FROM (%s) %s %s (%s) %s ON %s",
                formatAssignments(client, leftRelationAlias, leftAssignments),
                formatAssignments(client, rightRelationAlias, rightAssignments),
                leftSource.getQuery(),
                leftRelationAlias,
                formatJoinType(joinType),
                rightSource.getQuery(),
                rightRelationAlias,
                joinConditions.stream()
                        .map(condition -> formatJoinCondition(client, leftRelationAlias, rightRelationAlias, condition))
                        .collect(joining(" AND ")));
        List<QueryParameter> parameters = ImmutableList.<QueryParameter>builder()
                .addAll(leftSource.getParameters())
                .addAll(rightSource.getParameters())
                .build();
        return new PreparedQuery(query, parameters);
    }

    @Override
    public PreparedQuery prepareDeleteQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcNamedRelationHandle baseRelation,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ParameterizedExpression> additionalPredicate)
    {
        String sql = "DELETE FROM " + getRelation(client, baseRelation.getRemoteTableName());

        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();

        toConjuncts(client, session, connection, tupleDomain, conjuncts, accumulator::add);
        additionalPredicate.ifPresent(predicate -> {
            conjuncts.add(predicate.expression());
            accumulator.addAll(predicate.parameters());
        });
        List<String> clauses = conjuncts.build();
        if (!clauses.isEmpty()) {
            sql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }
        return new PreparedQuery(sql, accumulator.build());
    }

    @Override
    public PreparedStatement prepareStatement(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            PreparedQuery preparedQuery,
            Optional<Integer> columnCount)
            throws SQLException
    {
        String modifiedQuery = queryModifier.apply(session, preparedQuery.getQuery());
        log.debug("Preparing query: %s", modifiedQuery);
        columnCount = columnCount.map(count -> max(count, 1)); // Query builder appends a dummy projection when no columns projected
        PreparedStatement statement = client.getPreparedStatement(connection, modifiedQuery, columnCount);

        List<QueryParameter> parameters = preparedQuery.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            QueryParameter parameter = parameters.get(i);
            int parameterIndex = i + 1;
            WriteFunction writeFunction = parameter.getJdbcType()
                    .map(jdbcType -> getWriteFunction(client, session, connection, jdbcType, parameter.getType()))
                    .orElseGet(() -> getWriteFunction(client, session, parameter.getType()));
            Class<?> javaType = writeFunction.getJavaType();
            Object value = parameter.getValue()
                    // The value must be present, since DefaultQueryBuilder never creates null parameters. Values coming from Domain's ValueSet are non-null, and
                    // nullable domains are handled explicitly, with SQL syntax.
                    .orElseThrow(() -> new VerifyException("Value is missing"));
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

    protected String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        return format(
                "%s.%s %s %s.%s",
                leftRelationAlias,
                buildJoinColumn(client, condition.getLeftColumn()),
                condition.getOperator().getValue(),
                rightRelationAlias,
                buildJoinColumn(client, condition.getRightColumn()));
    }

    protected String buildJoinColumn(JdbcClient client, JdbcColumnHandle columnHandle)
    {
        return client.quoted(columnHandle.getColumnName());
    }

    protected String formatAssignments(JdbcClient client, String relationAlias, Map<JdbcColumnHandle, String> assignments)
    {
        return assignments.entrySet().stream()
                .map(entry -> format("%s.%s AS %s", relationAlias, client.quoted(entry.getKey().getColumnName()), client.quoted(entry.getValue())))
                .collect(joining(", "));
    }

    protected String formatJoinType(JoinType joinType)
    {
        switch (joinType) {
            case INNER:
                return "INNER JOIN";
            case LEFT_OUTER:
                return "LEFT JOIN";
            case RIGHT_OUTER:
                return "RIGHT JOIN";
            case FULL_OUTER:
                return "FULL JOIN";
        }
        throw new IllegalStateException("Unsupported join type: " + joinType);
    }

    protected String getRelation(JdbcClient client, RemoteTableName remoteTableName)
    {
        return client.quoted(remoteTableName);
    }

    protected String getProjection(JdbcClient client, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions, Consumer<QueryParameter> accumulator)
    {
        if (columns.isEmpty()) {
            return "1 x";
        }
        List<String> projections = new ArrayList<>();
        for (JdbcColumnHandle jdbcColumnHandle : columns) {
            String columnAlias = client.quoted(jdbcColumnHandle.getColumnName());
            ParameterizedExpression expression = columnExpressions.get(jdbcColumnHandle.getColumnName());
            if (expression == null) {
                projections.add(columnAlias);
            }
            else {
                projections.add(format("%s AS %s", expression.expression(), columnAlias));
                expression.parameters().forEach(accumulator);
            }
        }
        return String.join(", ", projections);
    }

    private String getFrom(JdbcClient client, JdbcRelationHandle baseRelation, Consumer<QueryParameter> accumulator)
    {
        if (baseRelation instanceof JdbcNamedRelationHandle) {
            return " FROM " + getRelation(client, ((JdbcNamedRelationHandle) baseRelation).getRemoteTableName());
        }
        if (baseRelation instanceof JdbcQueryRelationHandle) {
            PreparedQuery preparedQuery = ((JdbcQueryRelationHandle) baseRelation).getPreparedQuery();
            preparedQuery.getParameters().forEach(accumulator);
            return " FROM (" + preparedQuery.getQuery() + ") o";
        }
        throw new IllegalArgumentException("Unsupported relation: " + baseRelation);
    }

    protected Domain pushDownDomain(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain)
    {
        return client.toColumnMapping(session, connection, column.getJdbcTypeHandle())
                .orElseThrow(() -> new IllegalStateException(format("Unsupported type %s with handle %s", column.getColumnType(), column.getJdbcTypeHandle())))
                .getPredicatePushdownController().apply(session, domain).getPushedDown();
    }

    protected void toConjuncts(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            TupleDomain<ColumnHandle> tupleDomain,
            ImmutableList.Builder<String> result,
            Consumer<QueryParameter> accumulator)
    {
        if (tupleDomain.isNone()) {
            result.add(ALWAYS_FALSE);
            return;
        }
        for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            JdbcColumnHandle column = ((JdbcColumnHandle) entry.getKey());
            Domain domain = pushDownDomain(client, session, connection, column, entry.getValue());
            result.add(toPredicate(client, session, connection, column, domain, accumulator));
        }
    }

    protected String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain, Consumer<QueryParameter> accumulator)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? client.quoted(column.getColumnName()) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : client.quoted(column.getColumnName()) + " IS NOT NULL";
        }

        String predicate = toPredicate(client, session, connection, column, domain.getValues(), accumulator);
        if (!domain.isNullAllowed()) {
            return predicate;
        }
        return format("(%s OR %s IS NULL)", predicate, client.quoted(column.getColumnName()));
    }

    protected String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, ValueSet valueSet, Consumer<QueryParameter> accumulator)
    {
        checkArgument(!valueSet.isNone(), "none values should be handled earlier");

        if (!valueSet.isDiscreteSet()) {
            ValueSet complement = valueSet.complement();
            if (complement.isDiscreteSet()) {
                return format("NOT (%s)", toPredicate(client, session, connection, column, complement, accumulator));
            }
        }

        JdbcTypeHandle jdbcType = column.getJdbcTypeHandle();
        Type type = column.getColumnType();
        WriteFunction writeFunction = getWriteFunction(client, session, connection, jdbcType, type);

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : valueSet.getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(toPredicate(client, session, column, jdbcType, type, writeFunction, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), accumulator));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toPredicate(client, session, column, jdbcType, type, writeFunction, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), accumulator));
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
            disjuncts.add(toPredicate(client, session, column, jdbcType, type, writeFunction, "=", getOnlyElement(singleValues), accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
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

    protected String toPredicate(JdbcClient client, ConnectorSession session, JdbcColumnHandle column, JdbcTypeHandle jdbcType, Type type, WriteFunction writeFunction, String operator, Object value, Consumer<QueryParameter> accumulator)
    {
        accumulator.accept(new QueryParameter(jdbcType, type, Optional.of(value)));
        return format("%s %s %s", client.quoted(column.getColumnName()), operator, writeFunction.getBindExpression());
    }

    protected String getGroupBy(JdbcClient client, Optional<List<List<JdbcColumnHandle>>> groupingSets)
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

    private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Connection connection, JdbcTypeHandle jdbcType, Type type)
    {
        WriteFunction writeFunction = client.toColumnMapping(session, connection, jdbcType)
                .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s", type, jdbcType)))
                .getWriteFunction();
        verify(writeFunction.getJavaType() == type.getJavaType(), "Java type mismatch: %s, %s", writeFunction, type);
        return writeFunction;
    }

    private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Type type)
    {
        return client.toWriteMapping(session, type).getWriteFunction();
    }
}
