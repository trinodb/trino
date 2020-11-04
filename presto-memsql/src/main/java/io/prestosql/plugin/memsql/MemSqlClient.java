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
package io.prestosql.plugin.memsql;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.PredicatePushdownController;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.ColumnMapping.PUSHDOWN_AND_KEEP;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MemSqlClient
        extends BaseJdbcClient
{
    static final int MEMSQL_VARCHAR_MAX_LENGTH = 21844;
    static final int MEMSQL_TEXT_MAX_LENGTH = 65535;
    static final int MEMSQL_MEDIUMTEXT_MAX_LENGTH = 16777215;

    private final Type jsonType;

    @Inject
    public MemSqlClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, "`", connectionFactory);
        requireNonNull(typeManager, "typeManager is null");
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        // for MemSQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterSchema(schemaName)) {
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
    protected boolean filterSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("memsql")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> unsignedMapping = getUnsignedMapping(typeHandle);
        if (unsignedMapping.isPresent()) {
            return unsignedMapping;
        }

        if (jdbcTypeName.equalsIgnoreCase("json")) {
            return Optional.of(jsonColumnMapping());
        }

        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                VarcharType varcharType = (columnSize <= VarcharType.MAX_LENGTH) ? createVarcharType(columnSize) : createUnboundedVarcharType();
                // Remote database can be case insensitive.
                PredicatePushdownController predicatePushdownController = PUSHDOWN_AND_KEEP;
                return Optional.of(ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), predicatePushdownController));
            case Types.DECIMAL:
                int precision = columnSize;
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
        }

        // TODO add explicit mappings
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // MemSQL doesn't accept `some;column` in CTAS statements - so we explicitly block it and throw a proper error message
        tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .filter(s -> s.contains(";"))
                .findAny()
                .ifPresent(illegalColumnName -> {
                    throw new PrestoException(JDBC_ERROR, format("Incorrect column name '%s'", illegalColumnName));
                });

        super.createTable(session, tableMetadata);
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // MemSQL doesn't accept `some;column` in CTAS statements - so we explicitly block it and throw a proper error message
        columnNames.stream()
                .filter(s -> s.contains(";"))
                .findAny()
                .ifPresent(illegalColumnName -> {
                    throw new PrestoException(JDBC_ERROR, format("Incorrect column name '%s'", illegalColumnName));
                });

        super.copyTableSchema(connection, catalogName, schemaName, tableName, newTableName, columnNames);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MemSQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // MemSQL doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        verify(handle.getSchemaName() == null);
        renameTable(identity, null, handle.getCatalogName(), handle.getTableName(), newTableName);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            // MemSQL versions earlier than 5.7 do not support the CHANGE syntax
            String sql = format(
                    "ALTER TABLE %s CHANGE %s %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MemSQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            else if (varcharType.getBoundedLength() <= MEMSQL_VARCHAR_MAX_LENGTH) {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            else if (varcharType.getBoundedLength() <= MEMSQL_TEXT_MAX_LENGTH) {
                dataType = "text";
            }
            else if (varcharType.getBoundedLength() <= MEMSQL_MEDIUMTEXT_MAX_LENGTH) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("longblob", varbinaryWriteFunction());
        }
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        // TODO implement TIME type
        // TODO add support for other TIMESTAMP precisions
        if (TIMESTAMP_MILLIS.equals(type)) {
            return WriteMapping.longMapping("datetime", timestampWriteFunction(TIMESTAMP_MILLIS));
        }

        // TODO add explicit mappings
        return super.toWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isEmpty()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(smallintColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20), UNNECESSARY));
        }

        return Optional.empty();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }
}
