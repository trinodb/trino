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

package io.prestosql.plugin.influx;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InfluxRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final InfluxClient client;

    @Inject
    public InfluxRecordSetProvider(InfluxClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle tableHandle, List<? extends ColumnHandle> columns)
    {
        InfluxTableHandle table = (InfluxTableHandle) tableHandle;
        ImmutableList.Builder<InfluxColumn> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            InfluxColumnHandle column = (InfluxColumnHandle) handle;
            InfluxError.GENERAL.check(column.getMeasurement().equals(table.getMeasurement()), "bad measurement for " + column + " in " + table);
            InfluxError.GENERAL.check(column.getRetentionPolicy().equals(table.getRetentionPolicy()), "bad retention-policy for " + column + " in " + table);
            handles.add(column);
        }
        // we SELECT * as this returns tags, and returns nulls for unpopulated fields;
        // if we ask for specific columns, e.g. SELECT a WHERE b = 1, we would be asking for SELECT a WHERE b = 1 AND a IS NOT NULL instead
        // and there's no way to ask for tags without grouping
        String query = new InfluxQL("SELECT * ").append(table.getFromWhere()).toString();
        JsonNode results = client.execute(query);  // actually run the query against our Influx server
        return new InfluxRecordSet(handles.build(), results);
    }
}
