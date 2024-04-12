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
package io.trino.plugin.mongodb;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

public class MongoPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MongoSession mongoSession;
    private final String implicitPrefix;

    @Inject
    public MongoPageSinkProvider(MongoClientConfig config, MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
        this.implicitPrefix = config.getImplicitRowFieldPrefix();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        MongoOutputTableHandle handle = (MongoOutputTableHandle) outputTableHandle;
        return new MongoPageSink(mongoSession, handle.getTemporaryRemoteTableName().orElseGet(handle::remoteTableName), handle.columns(), implicitPrefix, handle.pageSinkIdColumnName(), pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        MongoInsertTableHandle handle = (MongoInsertTableHandle) insertTableHandle;
        return new MongoPageSink(mongoSession, handle.getTemporaryRemoteTableName().orElseGet(handle::remoteTableName), handle.columns(), implicitPrefix, handle.pageSinkIdColumnName(), pageSinkId);
    }
}
