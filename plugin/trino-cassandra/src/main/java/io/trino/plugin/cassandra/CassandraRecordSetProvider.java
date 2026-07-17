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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.inject.Inject;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final CassandraSession cassandraSession;
    private final CassandraTypeManager cassandraTypeManager;

    @Inject
    public CassandraRecordSetProvider(CassandraSession cassandraSession, CassandraTypeManager cassandraTypeManager)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager);
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        CassandraSplit cassandraSplit = (CassandraSplit) split;
        CassandraTableHandle cassandraTable = (CassandraTableHandle) table;

        List<CassandraColumnHandle> cassandraColumns = columns.stream()
                .map(column -> (CassandraColumnHandle) column)
                .collect(toList());

        if (cassandraTable.relationHandle() instanceof CassandraQueryRelationHandle queryRelationHandle) {
            return new CassandraRecordSet(
                    cassandraSession,
                    cassandraTypeManager,
                    SimpleStatement.newInstance(queryRelationHandle.getQuery()),
                    cassandraColumns);
        }

        String where = cassandraSplit.getWhereClause();
        Select select = CassandraCqlUtils.selectFrom(cassandraTable.getRequiredNamedRelation(), cassandraColumns);
        if (!where.isBlank()) {
            select = select.whereRaw(where);
        }
        return new CassandraRecordSet(
                cassandraSession,
                cassandraTypeManager,
                select.build().setIdempotent(true),
                cassandraColumns);
    }
}
