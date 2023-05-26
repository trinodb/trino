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
package io.trino.plugin.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.TupleDomain;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Neo4jQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public Neo4jQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    /**
     *  This connector only supports table functions, so wrap the user entered table function query into a Neo4j call subquery with projected columns
     *  The other methods prepareJoinQuery and prepareDeleteQuery need not be implemented
     */
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
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();
        String columnProjection = columns.stream().map(column -> "`" + column.getColumnName() + "`").collect(Collectors.joining(","));
        columnProjection = columnProjection.equals("") ? "*" : columnProjection;
        PreparedQuery preparedQuery = ((JdbcQueryRelationHandle) baseRelation).getPreparedQuery();
        // wrap the original query in a subquery and return column projection
        String sql = String.format("call {%s} return %s", preparedQuery.getQuery(), columnProjection);
        preparedQuery.getParameters().forEach(accumulator::add);

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
        throw new UnsupportedOperationException("This connector does not support join operations");
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
        throw new UnsupportedOperationException("This connector does not support delete queries");
    }
}
