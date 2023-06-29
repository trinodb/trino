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
package io.trino.plugin.jdbc.mapping;

import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DefaultIdentifierMapping
        implements IdentifierMapping
{
    private final RemoteIdentifierSupplier remoteIdentifierSupplier;

    @Inject
    public DefaultIdentifierMapping(RemoteIdentifierSupplier remoteIdentifierSupplier)
    {
        this.remoteIdentifierSupplier = requireNonNull(remoteIdentifierSupplier, "remoteIdentifierSupplier is null");
    }

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName)
    {
        return remoteSchemaName.toLowerCase(ENGLISH);
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName)
    {
        return remoteTableName.toLowerCase(ENGLISH);
    }

    @Override
    public String fromRemoteColumnName(String remoteColumnName)
    {
        return remoteColumnName.toLowerCase(ENGLISH);
    }

    @Override
    public String toRemoteSchemaName(ConnectorIdentity identity, Connection connection, String schemaName)
    {
        return toRemoteIdentifier(connection, schemaName);
    }

    @Override
    public String toRemoteTableName(ConnectorIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        return toRemoteIdentifier(connection, tableName);
    }

    @Override
    public String toRemoteColumnName(Connection connection, String columnName)
    {
        return toRemoteIdentifier(connection, columnName);
    }

    private String toRemoteIdentifier(Connection connection, String identifier)
    {
        return remoteIdentifierSupplier.toRemoteIdentifier(connection, identifier);
    }
}
