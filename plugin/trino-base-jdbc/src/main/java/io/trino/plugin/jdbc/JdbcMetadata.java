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
package io.trino.plugin.jdbc;

import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;

public interface JdbcMetadata
        extends ConnectorMetadata
{
    JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery);

    JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery);

    void rollback();

    static List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcClient jdbcClient, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        return jdbcClient.getColumns(session, schemaTableName, remoteTableName);
    }
}
