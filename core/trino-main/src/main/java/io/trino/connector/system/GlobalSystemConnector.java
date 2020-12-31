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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.transaction.InternalConnector;
import io.prestosql.transaction.TransactionId;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GlobalSystemConnector
        implements InternalConnector
{
    public static final String NAME = "system";

    private final Set<SystemTable> systemTables;
    private final Set<Procedure> procedures;

    public GlobalSystemConnector(Set<SystemTable> systemTables, Set<Procedure> procedures)
    {
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly)
    {
        return new GlobalSystemTransactionHandle(transactionId);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
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
}
