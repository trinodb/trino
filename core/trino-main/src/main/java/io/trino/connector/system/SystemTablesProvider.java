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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.transaction.TransactionManager;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class SystemTablesProvider
{
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final String catalogName;
    private final Set<SystemTable> systemTables;
    private final Map<SchemaTableName, SystemTable> systemTablesMap;

    public SystemTablesProvider(TransactionManager transactionManager, Metadata metadata, String catalogName, Set<SystemTable> systemTables)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.systemTablesMap = systemTables.stream()
                .collect(toImmutableMap(
                        table -> table.getTableMetadata().getTable(),
                        identity()));
    }

    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        // dynamic are not listed, so ony list static tables
        return systemTables;
    }

    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<SystemTable> staticSystemTable = Optional.ofNullable(systemTablesMap.get(tableName));
        if (staticSystemTable.isPresent()) {
            return staticSystemTable;
        }

        // This means there is no known static table, but that doesn't mean a dynamic table must exist.
        // This node could have a different config that causes that table to not exist.
        if (!isCoordinatorTransaction(session)) {
            // this is a session from another coordinator, so there are no dynamic tables here for that session
            return Optional.empty();
        }

        // dynamic tables that are not SINGLE_COORDINATOR mode need to implement the
        // PageSourceProvider interface for SystemSplit through the Connector interface
        return metadata.getSystemTable(
                ((FullConnectorSession) session).getSession(),
                new QualifiedObjectName(catalogName, tableName.getSchemaName(), tableName.getTableName()));
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
