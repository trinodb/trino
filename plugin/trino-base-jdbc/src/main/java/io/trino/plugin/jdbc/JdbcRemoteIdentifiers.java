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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class JdbcRemoteIdentifiers
        implements RemoteIdentifiers
{
    private final BaseJdbcClient baseJdbcClient;
    private final Connection connection;
    private final boolean storesUpperCase;

    public JdbcRemoteIdentifiers(BaseJdbcClient baseJdbcClient, Connection connection, boolean storesUpperCase)
    {
        this.baseJdbcClient = requireNonNull(baseJdbcClient, "baseJdbcClient is null");
        this.connection = requireNonNull(connection, "connection is null");
        this.storesUpperCase = storesUpperCase;
    }

    @Override
    public Set<String> getRemoteSchemas()
    {
        return baseJdbcClient.listSchemas(connection)
                .stream()
                .collect(toImmutableSet());
    }

    @Override
    public Set<String> getRemoteTables(String remoteSchema)
    {
        try (ResultSet resultSet = baseJdbcClient.getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
            return tableNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
    {
        return storesUpperCase;
    }

    public static class JdbcRemoteIdentifiersFactory
    {
        private final BaseJdbcClient baseJdbcClient;
        private Boolean storesUpperCaseIdentifiers;

        public JdbcRemoteIdentifiersFactory(BaseJdbcClient baseJdbcClient)
        {
            this.baseJdbcClient = requireNonNull(baseJdbcClient, "baseJdbcClient is null");
        }

        public JdbcRemoteIdentifiers createJdbcRemoteIdentifies(Connection connection)
        {
            return new JdbcRemoteIdentifiers(baseJdbcClient, connection, storesUpperCaseIdentifiers(connection));
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
}
