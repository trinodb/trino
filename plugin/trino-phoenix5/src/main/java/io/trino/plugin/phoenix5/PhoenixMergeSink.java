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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcAssignmentItem;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMergeSink;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class PhoenixMergeSink
        implements ConnectorMergeSink
{
    private final ConnectorMergeSink delegate;

    public PhoenixMergeSink(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            PhoenixClient phoenixClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier queryModifier,
            QueryBuilder queryBuilder)
    {
        requireNonNull(session, "session is null");
        requireNonNull(mergeHandle, "mergeHandle is null");
        requireNonNull(phoenixClient, "phoenixClient is null");
        requireNonNull(pageSinkId, "pageSinkId is null");
        requireNonNull(queryModifier, "queryModifier is null");
        requireNonNull(queryBuilder, "queryBuilder is null");

        this.delegate = new JdbcMergeSink(session, mergeHandle, phoenixClient, pageSinkId, queryModifier, new MergeQueryBuilder(queryBuilder));
    }

    /**
     * The class only used in Phoenix merge process.
     */
    private record MergeQueryBuilder(QueryBuilder delegate)
            implements QueryBuilder
    {
        @Override
        public PreparedQuery prepareSelectQuery(JdbcClient client, ConnectorSession session, Connection connection, JdbcRelationHandle baseRelation, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions, TupleDomain<ColumnHandle> tupleDomain, Optional<ParameterizedExpression> additionalPredicate)
        {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public PreparedQuery prepareJoinQuery(JdbcClient client, ConnectorSession session, Connection connection, JoinType joinType, PreparedQuery leftSource, Map<JdbcColumnHandle, String> leftProjections, PreparedQuery rightSource, Map<JdbcColumnHandle, String> rightProjections, List<ParameterizedExpression> joinConditions)
        {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public PreparedQuery legacyPrepareJoinQuery(JdbcClient client, ConnectorSession session, Connection connection, JoinType joinType, PreparedQuery leftSource, PreparedQuery rightSource, List<JdbcJoinCondition> joinConditions, Map<JdbcColumnHandle, String> leftAssignments, Map<JdbcColumnHandle, String> rightAssignments)
        {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public PreparedQuery prepareDeleteQuery(JdbcClient client, ConnectorSession session, Connection connection, JdbcNamedRelationHandle baseRelation, TupleDomain<ColumnHandle> tupleDomain, Optional<ParameterizedExpression> additionalPredicate)
        {
            return delegate.prepareDeleteQuery(client, session, connection, baseRelation, tupleDomain, additionalPredicate);
        }

        @Override
        public PreparedQuery prepareUpdateQuery(JdbcClient client, ConnectorSession session, Connection connection, JdbcNamedRelationHandle baseRelation, TupleDomain<ColumnHandle> tupleDomain, Optional<ParameterizedExpression> additionalPredicate, List<JdbcAssignmentItem> assignments)
        {
            // Phoenix update is perform as insert with primary keys(upsert)
            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<WriteFunction> writeFunctionBuilder = ImmutableList.builder();
            for (JdbcAssignmentItem assignmentItem : assignments) {
                JdbcColumnHandle column = assignmentItem.column();
                namesBuilder.add(column.getColumnName());
                typesBuilder.add(column.getColumnType());
                writeFunctionBuilder.add(getWriteFunction(client, session, column.getColumnType()));
            }

            // Put the primary keys domain after the update assignments
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("primary keys domain not exists"));
            for (ColumnHandle columnHandle : domains.keySet()) {
                JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
                namesBuilder.add(column.getColumnName());
                typesBuilder.add(column.getColumnType());
                writeFunctionBuilder.add(getWriteFunction(client, session, column.getColumnType()));
            }

            String query = client.buildInsertSql(
                    new PhoenixOutputTableHandle(
                            baseRelation.getRemoteTableName(),
                            namesBuilder.build(),
                            typesBuilder.build(),
                            Optional.empty(),
                            Optional.empty()),
                    writeFunctionBuilder.build());
            return new PreparedQuery(query, ImmutableList.of());
        }

        @Override
        public PreparedStatement prepareStatement(JdbcClient client, ConnectorSession session, Connection connection, PreparedQuery preparedQuery, Optional<Integer> columnCount)
        {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public CallableStatement callProcedure(JdbcClient client, ConnectorSession session, Connection connection, JdbcProcedureHandle.ProcedureQuery procedureQuery)
        {
            throw new UnsupportedOperationException("Not supported");
        }

        private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Type type)
        {
            return client.toWriteMapping(session, type).getWriteFunction();
        }
    }

    @Override
    public void storeMergedRows(Page page)
    {
        delegate.storeMergedRows(page);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return delegate.finish();
    }

    @Override
    public void abort()
    {
        delegate.abort();
    }
}
