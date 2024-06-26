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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;

public class VerticaClient
        extends BaseJdbcClient
{
    // Date format is different between read and write in Vertica
    private static final DateTimeFormatter DATE_READ_FORMATTER = DateTimeFormatter.ofPattern("u-MM-dd");
    private static final DateTimeFormatter DATE_WRITE_FORMATTER = new DateTimeFormatterBuilder()
            .appendValueReduced(ChronoField.YEAR, 4, 7, 1000)
            .appendPattern("-MM-dd[ G]")
            .toFormatter();

    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("v_catalog")
            .add("v_monitor")
            .add("v_txtindex")
            .build();

    @Inject
    public VerticaClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INTERNAL_SCHEMAS.contains(schemaName.toLowerCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        connection.setAutoCommit(true);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        ImmutableList.Builder<String> createTableSqlsBuilder = ImmutableList.builder();
        createTableSqlsBuilder.add(format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns)));
        Optional<String> tableComment = tableMetadata.getComment();
        if (tableComment.isPresent()) {
            createTableSqlsBuilder.add(buildTableCommentSql(remoteTableName, tableComment));
        }
        return createTableSqlsBuilder.build();
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(typeHandle);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
            case Types.FLOAT:
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0);
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
            case Types.CHAR:
                return Optional.of(charColumnMapping(createCharType(typeHandle.getRequiredColumnSize()), false));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return Optional.of(varcharColumnMapping(createVarcharType(typeHandle.getRequiredColumnSize()), false));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        (resultSet, index) -> LocalDate.parse(resultSet.getString(index), DATE_READ_FORMATTER).toEpochDay(),
                        dateWriteFunctionUsingString()));
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType) {
            // TODO: Handle size difference between codepoints in Trino and byte length in Vertica
            int length = ((CharType) type).getLength();
            checkArgument(length <= 65000, "Maximum char length in Vertica is 65,000");
            return WriteMapping.sliceMapping("char(" + length + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            // TODO: Handle size difference between codepoints in Trino and byte length in Vertica
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "long varchar";
            }
            else {
                checkArgument(varcharType.getBoundedLength() <= 32_000_000, "Maximum varchar length in Vertica is 32,000,000");
                dataType = "long varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingString());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        // Vertica doesn't allow dropping the first column of any projection sort order, or columns that participate in a projection segmentation expression.
        // We should check the column position and throw an exception explicitly for supporting the statement.
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        // TODO Implement this method
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(newTableName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
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

    private static LongWriteFunction dateWriteFunctionUsingString()
    {
        return (statement, index, day) -> statement.setString(index, DATE_WRITE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }
}
