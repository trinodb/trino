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
package io.prestosql.plugin.accumulo.io;

import io.prestosql.plugin.accumulo.conf.AccumuloConfig;
import io.prestosql.plugin.accumulo.model.AccumuloColumnHandle;
import io.prestosql.plugin.accumulo.model.AccumuloSplit;
import io.prestosql.plugin.accumulo.model.AccumuloTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import org.apache.accumulo.core.client.Connector;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a ConnectorRecordSetProvider for Accumulo. Generates {@link AccumuloRecordSet} objects for a provided split.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordCursor
 */
public class AccumuloRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final Connector connector;
    private final String username;

    @Inject
    public AccumuloRecordSetProvider(
            Connector connector,
            AccumuloConfig config)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.username = requireNonNull(config, "config is null").getUsername();
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        AccumuloSplit accSplit = (AccumuloSplit) split;
        AccumuloTableHandle accTable = (AccumuloTableHandle) table;

        List<AccumuloColumnHandle> accColumns = columns.stream()
                .map(AccumuloColumnHandle.class::cast)
                .collect(toImmutableList());

        return new AccumuloRecordSet(connector, session, accSplit, username, accTable, accColumns);
    }
}
