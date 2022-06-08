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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.SystemTableHandle;

import java.util.Optional;
import java.util.Set;

public interface SystemTablesProvider
{
    Set<SystemTable> listSystemTables(ConnectorSession session);

    /**
     * Resolves table name. Returns {@link Optional#empty()} if table is not found.
     * Some tables which are not part of set returned by {@link #listSystemTables(ConnectorSession)}
     * can still be validly resolved.
     */
    Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion);

    /**
     * Retrieves the system table corresponding to the specified system table handle.
     */
    Optional<SystemTable> getSystemTable(ConnectorSession session, SystemTableHandle systemTableHandle);

    /**
     * Resolves table name. Returns {@link Optional#empty()} if table is not found.
     * If the underlying connector supports time travel queries, the returned table
     * handle will contain corresponding information for being able to get the state
     * of the table at a previous moment in time.
     */
    Optional<SystemTableHandle> getSystemTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion);
}
