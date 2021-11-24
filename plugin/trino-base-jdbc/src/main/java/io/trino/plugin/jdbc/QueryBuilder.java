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
import io.trino.spi.predicate.TupleDomain;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface QueryBuilder
{
    PreparedQuery prepareQuery(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcRelationHandle baseRelation,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<String> additionalPredicate);

    PreparedQuery prepareJoinQuery(
            JdbcClient client,
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> leftAssignments,
            Map<JdbcColumnHandle, String> rightAssignments);

    PreparedQuery prepareDelete(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcNamedRelationHandle baseRelation,
            TupleDomain<ColumnHandle> tupleDomain);

    PreparedStatement prepareStatement(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            PreparedQuery preparedQuery)
            throws SQLException;
}
