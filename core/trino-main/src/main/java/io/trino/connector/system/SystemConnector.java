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
package io.trino.connector.system;

import io.trino.metadata.InternalNodeManager;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.InternalConnector;
import io.trino.transaction.TransactionId;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SystemConnector
        implements InternalConnector
{
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction;

    public SystemConnector(
            InternalNodeManager nodeManager,
            SystemTablesProvider tables,
            Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction,
            AccessControl accessControl,
            String catalogName)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(tables, "tables is null");
        requireNonNull(transactionHandleFunction, "transactionHandleFunction is null");
        requireNonNull(accessControl, "accessControl is null");
        requireNonNull(catalogName, "catalogName is null");

        this.metadata = new SystemTablesMetadata(tables);
        this.splitManager = new SystemSplitManager(nodeManager, tables);
        this.pageSourceProvider = new SystemPageSourceProvider(tables, accessControl, catalogName);
        this.transactionHandleFunction = transactionHandleFunction;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new SystemTransactionHandle(transactionId, transactionHandleFunction);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
