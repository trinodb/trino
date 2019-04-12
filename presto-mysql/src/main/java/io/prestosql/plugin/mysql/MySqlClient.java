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
package io.prestosql.plugin.mysql;

import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.mysql.jdbc.SQLError.SQL_STATE_ER_TABLE_EXISTS_ERROR;
import static com.mysql.jdbc.SQLError.SQL_STATE_SYNTAX_ERROR;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class MySqlClient
        extends BaseJdbcClient
{
    @Inject
    public MySqlClient(BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        super(config, "`", connectionFactory(config, mySqlConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema") && !schemaName.equalsIgnoreCase("mysql")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        // Abort connection before closing. Without this, the MySQL driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(Statement.class)) {
            statement.unwrap(Statement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP.equals(type)) {
            // TODO use `timestampWriteFunction`
            return WriteMapping.longMapping("datetime", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            else if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            }
            else if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState());
            throw new PrestoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            // MySQL versions earlier than 8 do not support the above RENAME COLUMN syntax
            if (SQL_STATE_SYNTAX_ERROR.equals(e.getSQLState())) {
                throw new PrestoException(NOT_SUPPORTED, format("Rename column not supported in catalog: '%s'", handle.getCatalogName()), e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // MySQL doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        verify(handle.getSchemaName() == null);
        renameTable(identity, null, handle.getCatalogName(), handle.getTableName(), newTableName);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }
}
