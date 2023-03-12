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

import io.airlift.log.Logger;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(CassandraRecordSetProvider.class);

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

        if (cassandraTable.getRelationHandle() instanceof CassandraQueryRelationHandle queryRelationHandle) {
            return new CassandraRecordSet(cassandraSession, cassandraTypeManager, queryRelationHandle.getQuery(), cassandraColumns);
        }

        String selectCql = CassandraCqlUtils.selectFrom(cassandraTable.getRequiredNamedRelation(), cassandraColumns).asCql();
        StringBuilder sb = new StringBuilder(selectCql);
        if (sb.charAt(sb.length() - 1) == ';') {
            sb.setLength(sb.length() - 1);
        }
        sb.append(cassandraSplit.getWhereClause());
        String cql = sb.toString();
        log.debug("Creating record set: %s", cql);

        return new CassandraRecordSet(cassandraSession, cassandraTypeManager, cql, cassandraColumns);
    }
}
