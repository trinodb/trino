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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class DelegatingQueryBuilder
        implements QueryBuilder
{
    protected final QueryBuilder delegate;

    public DelegatingQueryBuilder(QueryBuilder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public PreparedQuery prepareSelectQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcRelationHandle baseRelation,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<String> additionalPredicate)
    {
        return delegate.prepareSelectQuery(client, session, connection, baseRelation, groupingSets, columns, columnExpressions, tupleDomain, additionalPredicate);
    }

    @Override
    public PreparedQuery prepareJoinQuery(
            JdbcClient client,
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> leftAssignments,
            Map<JdbcColumnHandle, String> rightAssignments
    ) {
        return delegate.prepareJoinQuery(client, session, joinType, leftSource, rightSource, joinConditions, leftAssignments, rightAssignments);
    }

    @Override
    public PreparedQuery prepareDeleteQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcNamedRelationHandle baseRelation,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate.prepareDeleteQuery(client, session, connection, baseRelation, tupleDomain);
    }

    @Override
    public PreparedStatement prepareStatement(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            PreparedQuery preparedQuery) throws SQLException
    {
        return delegate.prepareStatement(client, session, connection, preparedQuery);
    }

    @Override
    public String formatAssignments(JdbcClient client, String relationAlias, Map<JdbcColumnHandle, String> assignments)
    {
        return delegate.formatAssignments(client, relationAlias, assignments);
    }

    @Override
    public String formatJoinType(JoinType joinType)
    {
        return delegate.formatJoinType(joinType);
    }

    @Override
    public String formatJoinConditions(JdbcClient client, String leftRelationAlias, String rightRelationAlias, List<JdbcJoinCondition> joinConditions)
    {
        return delegate.formatJoinConditions(client, leftRelationAlias, rightRelationAlias, joinConditions);
    }

    @Override
    public String formatJoinCondition(JdbcClient client, String leftRelationAlias, String rightRelationAlias, JdbcJoinCondition condition)
    {
        return delegate.formatJoinCondition(client, leftRelationAlias, rightRelationAlias, condition);
    }

    @Override
    public String getRelation(JdbcClient client, RemoteTableName remoteTableName)
    {
        return delegate.getRelation(client, remoteTableName);
    }

    @Override
    public String getFrom(JdbcClient client, JdbcRelationHandle baseRelation, Consumer<QueryParameter> accumulator)
    {
        return delegate.getFrom(client, baseRelation, accumulator);
    }

    @Override
    public List<String> toConjuncts(JdbcClient client, ConnectorSession session, Connection connection, TupleDomain<ColumnHandle> tupleDomain, Consumer<QueryParameter> accumulator)
    {
        return delegate.toConjuncts(client, session, connection, tupleDomain, accumulator);
    }

    @Override
    public String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, Domain domain, Consumer<QueryParameter> accumulator)
    {
        return delegate.toPredicate(client, session, connection, column, domain, accumulator);
    }

    @Override
    public String toPredicate(JdbcClient client, ConnectorSession session, Connection connection, JdbcColumnHandle column, ValueSet valueSet, Consumer<QueryParameter> accumulator)
    {
        return delegate.toPredicate(client, session, connection, column, valueSet, accumulator);
    }

    @Override
    public String toPredicate(
            JdbcClient client,
            JdbcColumnHandle column,
            JdbcTypeHandle jdbcType,
            Type type,
            WriteFunction writeFunction,
            String operator,
            Object value,
            Consumer<QueryParameter> accumulator)
    {
        return delegate.toPredicate(client, column, jdbcType, type, writeFunction, operator, value, accumulator);
    }

    @Override
    public String getProjection(JdbcClient client, List<JdbcColumnHandle> columns, Map<String, String> columnExpressions)
    {
        return delegate.getProjection(client, columns, columnExpressions);
    }

    @Override
    public String getGroupBy(JdbcClient client, Optional<List<List<JdbcColumnHandle>>> groupingSets)
    {
        return delegate.getGroupBy(client, groupingSets);
    }
}
