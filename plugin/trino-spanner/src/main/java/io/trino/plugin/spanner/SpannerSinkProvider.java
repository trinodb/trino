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
package io.trino.plugin.spanner;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

public class SpannerSinkProvider
        implements ConnectorPageSinkProvider
{
    private final SpannerClient client;
    private final RemoteQueryModifier modifier;
    private final SpannerTableProperties spannerTableProperties;
    private final SpannerConfig config;

    @Inject
    public SpannerSinkProvider(
            ConnectionFactory connectionFactory,
            RemoteQueryModifier modifier,
            SpannerClient client,
            SpannerConfig config,
            SpannerTableProperties propertiesProvider)
    {
        System.out.println("Called Spanner Sink provider");
        this.client = client;
        this.modifier = modifier;
        System.out.println("TABLE PROPS in SINK " + propertiesProvider.getTableProperties());
        this.spannerTableProperties = propertiesProvider;
        this.config = config;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return new SpannerSink(config, session, (JdbcOutputTableHandle) outputTableHandle, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        return new SpannerSink(config, session, (JdbcOutputTableHandle) tableHandle, pageSinkId);
    }
}
