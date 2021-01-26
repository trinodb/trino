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

import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.transaction.TransactionManager;

import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class CoordinatorSystemTablesProvider
        implements SystemTablesProvider
{
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final String catalogName;
    private final StaticSystemTablesProvider staticProvider;

    public CoordinatorSystemTablesProvider(TransactionManager transactionManager, Metadata metadata, String catalogName, StaticSystemTablesProvider staticProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.staticProvider = requireNonNull(staticProvider, "staticProvider is null");
    }

    @Override
    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        // dynamic are not listed, so ony list static tables
        return staticProvider.listSystemTables(session);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<SystemTable> staticSystemTable = staticProvider.getSystemTable(session, tableName);
        if (staticSystemTable.isPresent()) {
            return staticSystemTable;
        }

        // This means there is no known static table, but that doesn't mean a dynamic table must exist.
        // This node could have a different config that causes that table to not exist.

        if (!isCoordinatorTransaction(session)) {
            // this is a session from another coordinator, so there are no dynamic tables here for that session
            return Optional.empty();
        }
        Optional<SystemTable> systemTable = metadata.getSystemTable(
                ((FullConnectorSession) session).getSession(),
                new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName()));

        // dynamic tables require access to the transaction and thus can only run on the current coordinator
        if (systemTable.isPresent() && systemTable.get().getDistribution() != SINGLE_COORDINATOR) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Distribution for dynamic system table must be " + SINGLE_COORDINATOR);
        }

        return systemTable;
    }

    private boolean isCoordinatorTransaction(ConnectorSession connectorSession)
    {
        return Optional.of(connectorSession)
                .filter(FullConnectorSession.class::isInstance)
                .map(FullConnectorSession.class::cast)
                .map(FullConnectorSession::getSession)
                .flatMap(Session::getTransactionId)
                .map(transactionManager::transactionExists)
                .orElse(false);
    }
}
