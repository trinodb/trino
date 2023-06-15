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

import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;

public class DatabaseMetaDataRemoteIdentifierSupplier
        implements RemoteIdentifierSupplier
{
    private Boolean storesUpperCaseIdentifiers;

    @Override
    public String toRemoteIdentifier(Connection connection, String identifier)
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
