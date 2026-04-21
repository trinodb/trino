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
package io.trino.plugin.loki;

import com.google.inject.Inject;
import io.github.jeschkies.loki.client.LokiClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// TODO: replace with LokiPageSourceProvider eventually.
public class LokiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final LokiClient lokiClient;

    @Inject
    public LokiRecordSetProvider(LokiClient lokiClient)
    {
        this.lokiClient = requireNonNull(lokiClient, "lokiClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        LokiSplit lokiSplit = (LokiSplit) split;

        List<LokiColumnHandle> handles = columns.stream().map(LokiColumnHandle.class::cast).collect(toImmutableList());
        return new LokiRecordSet(lokiClient, lokiSplit, handles);
    }
}
