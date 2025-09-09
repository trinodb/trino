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
package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Types;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class ExasolClient
        extends BaseJdbcClient
{
    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("EXA_STATISTICS")
            .add("SYS")
            .build();
    private static final int EXASOL_MAX_SUPPORTED_VARCHAR_SIZE = 20000;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    @Inject
    public ExasolClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left, right)").to("left = right")
                .map("$not_equal(left, right)").to("left <> right")
                .map("$identical(left, right)").to("(left = right OR (left IS NULL AND right IS NULL))")
                .map("$less_than(left, right)").to("left < right")
                .map("$less_than_or_equal(left, right)").to("left <= right")
                .map("$greater_than(left, right)").to("left > right")
                .map("$greater_than_or_equal(left, right)").to("left >= right")
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INTERNAL_SCHEMAS.contains(schemaName.toUpperCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // Deactivated because test 'testCaseSensitiveAggregationPushdown()' requires write access which is not implemented for Exasol
        return Optional.empty();
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        if (isHashType(typeHandle)) {
            return Optional.of(hashTypeColumnMapping());
        }
        switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int columnSize = typeHandle.requiredColumnSize();
                return Optional.of(decimalColumnMapping(createDecimalType(columnSize, decimalDigits)));
            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.VARCHAR:
                // String data is sorted by its binary representation.
                // https://docs.exasol.com/db/latest/sql/select.htm#UsageNotes
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.DATE:
                return Optional.of(dateColumnMapping());
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private boolean isHashType(JdbcTypeHandle typeHandle)
    {
        return typeHandle.jdbcType() == Types.CHAR
                && typeHandle.jdbcTypeName().isPresent()
                && typeHandle.jdbcTypeName().get().equalsIgnoreCase("HASHTYPE");
    }

    private static ColumnMapping dateColumnMapping()
    {
        // Exasol driver does not support LocalDate
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingSqlDate(),
                dateWriteFunctionUsingSqlDate());
    }

    private static LongReadFunction dateReadFunctionUsingSqlDate()
    {
        return (resultSet, columnIndex) -> {
            Date date = resultSet.getDate(columnIndex);
            return date.toLocalDate().toEpochDay();
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingSqlDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            LocalDate localDate = LocalDate.ofEpochDay(value);
            statement.setDate(index, Date.valueOf(localDate));
        });
    }

    private static ColumnMapping hashTypeColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                hashTypeReadFunction(),
                hashTypeWriteFunction());
    }

    private static SliceReadFunction hashTypeReadFunction()
    {
        return (resultSet, columnIndex) -> {
            String hex = resultSet.getString(columnIndex);
            byte[] bytes = HexFormat.of().parseHex(hex);
            return Slices.wrappedBuffer(bytes);
        };
    }

    private static SliceWriteFunction hashTypeWriteFunction()
    {
        return SliceWriteFunction.of(Types.CHAR, (statement, index, slice) -> {
            byte[] bytes = slice.getBytes();
            String hex = HexFormat.of().formatHex(bytes);
            statement.setString(index, hex);
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            int length = varcharType.isUnbounded()
                    ? EXASOL_MAX_SUPPORTED_VARCHAR_SIZE
                    : Math.min(varcharType.getBoundedLength(), EXASOL_MAX_SUPPORTED_VARCHAR_SIZE);
            String dataType = "varchar(" + length + ")";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }
}
