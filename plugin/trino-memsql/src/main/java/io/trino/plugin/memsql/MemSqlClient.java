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
package io.trino.plugin.memsql;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.min;
import static java.lang.String.format;
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
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

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

        switch (typeHandle.getJdbcType()) {
            case Types.CHAR:
            case Types.NCHAR: // TODO it it is dummy copied from StandardColumnMappings, verify if it is proper mapping
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.VARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.DECIMAL:
                int precision = typeHandle.getRequiredColumnSize();
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
        }

        // TODO add explicit mappings
        return legacyToPrestoType(session, connection, typeHandle);
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
                    throw new TrinoException(JDBC_ERROR, format("Incorrect column name '%s'", illegalColumnName));
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
                    throw new TrinoException(JDBC_ERROR, format("Incorrect column name '%s'", illegalColumnName));
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
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // MemSQL doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        verify(handle.getSchemaName() == null);
        renameTable(session, null, handle.getCatalogName(), handle.getTableName(), newTableName);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
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
            throw new TrinoException(JDBC_ERROR, e);
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
        return legacyToWriteMapping(session, type);
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

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // Not supported in MemSQL
            return Optional.empty();
        }
        return super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(JdbcJoinCondition joinCondition)
    {
        if (joinCondition.getOperator() == JoinCondition.Operator.IS_DISTINCT_FROM) {
            // Not supported in MemSQL
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
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
        if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20)));
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
