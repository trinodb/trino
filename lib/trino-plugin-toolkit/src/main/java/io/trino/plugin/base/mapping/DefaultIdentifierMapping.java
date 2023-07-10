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
package io.trino.plugin.base.mapping;

import io.trino.spi.security.ConnectorIdentity;

import static java.util.Locale.ENGLISH;

public class DefaultIdentifierMapping
        implements IdentifierMapping
{
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
    public String toRemoteSchemaName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String schemaName)
    {
        return toRemoteIdentifier(schemaName, remoteIdentifiers);
    }

    @Override
    public String toRemoteTableName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String remoteSchema, String tableName)
    {
        return toRemoteIdentifier(tableName, remoteIdentifiers);
    }

    @Override
    public String toRemoteColumnName(RemoteIdentifiers remoteIdentifiers, String columnName)
    {
        return toRemoteIdentifier(columnName, remoteIdentifiers);
    }

    private String toRemoteIdentifier(String identifier, RemoteIdentifiers remoteIdentifiers)
    {
        if (remoteIdentifiers.storesUpperCaseIdentifiers()) {
            return identifier.toUpperCase(ENGLISH);
        }
        return identifier;
    }
}
