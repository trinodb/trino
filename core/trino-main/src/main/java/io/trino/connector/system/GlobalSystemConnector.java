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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.operator.table.Sequence.SequenceFunctionHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.InternalConnector;
import io.trino.transaction.TransactionId;

import java.util.Set;

import static io.trino.operator.table.Sequence.getSequenceFunctionSplitSource;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

public class GlobalSystemConnector
        implements InternalConnector
{
    public static final String NAME = "system";
    public static final CatalogHandle CATALOG_HANDLE = createRootCatalogHandle(NAME, new CatalogVersion("system"));

    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;
    private final Set<ConnectorTableFunction> tableFunctions;

    @Inject
    public GlobalSystemConnector(Set<SystemTable> systemTables, Set<Procedure> procedures, Set<ConnectorTableFunction> tableFunctions)
    {
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new GlobalSystemTransactionHandle(transactionId);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return new ConnectorMetadata() {};
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ConnectorSplitManager()
        {
            @Override
            public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle functionHandle)
            {
                if (functionHandle instanceof SequenceFunctionHandle sequenceFunctionHandle) {
                    return getSequenceFunctionSplitSource(sequenceFunctionHandle);
                }

                throw new UnsupportedOperationException();
            }
        };
    }
}
