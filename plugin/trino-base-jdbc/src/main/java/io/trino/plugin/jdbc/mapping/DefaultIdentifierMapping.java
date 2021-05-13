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

import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;

public class DefaultIdentifierMapping
        implements IdentifierMapping
{
    // Caching this on a field is LazyConnectorFactory friendly
    private Boolean storesUpperCaseIdentifiers;

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
    public String toRemoteSchemaName(JdbcIdentity identity, Connection connection, String schemaName)
    {
        return toRemoteIdentifier(connection, schemaName);
    }

    @Override
    public String toRemoteTableName(JdbcIdentity identity, Connection connection, String remoteSchema, String tableName)
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
        if (storesUpperCaseIdentifiers(connection)) {
            return identifier.toUpperCase(ENGLISH);
        }
        return identifier;
    }

    private boolean storesUpperCaseIdentifiers(Connection connection)
    {
        if (storesUpperCaseIdentifiers != null) {
            return storesUpperCaseIdentifiers;
        }
        try {
            DatabaseMetaData metadata = connection.getMetaData();
            storesUpperCaseIdentifiers = metadata.storesUpperCaseIdentifiers();
            return storesUpperCaseIdentifiers;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
