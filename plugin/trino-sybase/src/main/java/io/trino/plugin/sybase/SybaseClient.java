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
package io.trino.plugin.sybase;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SybaseClient
        extends BaseJdbcClient
{
    public static final int SYBASE_MAX_LIST_EXPRESSIONS = 500;

    private static final Joiner DOT_JOINER = Joiner.on(".");

    @Inject
    public SybaseClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping)
    {
        super(config, "\"", connectionFactory, identifierMapping);

        requireNonNull(config, "sybaseConfig is null");
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        String sql = format(
                "sp_rename %s, %s",
                singleQuote(catalogName, schemaName, tableName),
                singleQuote(newTable.getTableName()));
        execute(session, sql);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        String sql = format(
                "sp_rename %s, %s, 'COLUMN'",
                singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), jdbcColumn.getColumnName()),
                singleQuote(newColumnName));
        execute(session, sql);
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "SELECT %s INTO %s FROM %s WHERE 0 = 1",
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        return super.schemaExists(session, schema);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return legacyColumnMapping(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        return legacyToWriteMapping(session, type);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT TOP %s * FROM (%s) o", limit, sql));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.close();
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }
}
