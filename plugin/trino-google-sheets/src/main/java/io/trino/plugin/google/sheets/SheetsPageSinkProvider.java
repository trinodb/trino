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

package io.trino.plugin.google.sheets;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class SheetsPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final SheetsClient sheetsClient;

    @Inject
    public SheetsPageSinkProvider(SheetsClient sheetsClient)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "sheetsClient is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        throw new UnsupportedOperationException("Google Sheets connector does not support creating page sinks using a ConnectorOutputTableHandle");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        SheetsConnectorInsertTableHandle handle = (SheetsConnectorInsertTableHandle) insertTableHandle;
        return new SheetsPageSink(sheetsClient, handle.getTableName(), handle.getColumns());
    }
}
