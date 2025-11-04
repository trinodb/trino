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
package io.trino.plugin.teradata;

import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.weakref.jmx.$internal.guava.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;

public class TeradataClient
        extends BaseJdbcClient
{
    private static final PredicatePushdownController TERADATA_STRING_PUSHDOWN = FULL_PUSHDOWN;
    private final TeradataConfig.TeradataCaseSensitivity teradataJDBCCaseSensitivity;

    @Inject
    public TeradataClient(
            BaseJdbcConfig config,
            TeradataConfig teradataConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.teradataJDBCCaseSensitivity = teradataConfig.getTeradataCaseSensitivity();
    }

    private static ColumnMapping charColumnMapping(int charLength, boolean isCaseSensitive)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength, isCaseSensitive);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, boolean isCaseSensitive)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private boolean deriveCaseSensitivity(CaseSensitivity caseSensitivity)
    {
        return switch (teradataJDBCCaseSensitivity) {
            case CASE_INSENSITIVE -> false;
            case CASE_SENSITIVE -> true;
            default -> caseSensitivity != null;
        };
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
    {
        execute(session, format(
                "CREATE DATABASE %s AS PERMANENT = 60000000, SPOOL = 120000000",
                quoted(remoteSchemaName)));
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName,
            List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s AS ( SELECT * FROM %s ) WITH DATA";
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        int schemaNameLimit = databaseMetadata.getMaxSchemaNameLength();
        if (schemaName.length() > schemaNameLimit) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", schemaNameLimit, schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(),
                    tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'",
                    databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
        String dropSchema = "DROP DATABASE " + quoted(remoteSchemaName);
        execute(session, connection, dropSchema);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schema");
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support modifying table rows");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support add column operations");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName,
            RemoteTableName remoteTableName)
    {
        // try to use result set metadata from select * from table to populate the mapping
        try {
            HashMap<String, CaseSensitivity> caseMap = new HashMap<>();
            String sql = format("select * from %s.%s where 0=1", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSetMetaData rsmd = pstmt.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                caseMap.put(rsmd.getColumnName(i), rsmd.isCaseSensitive(i) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            pstmt.close();
            return caseMap;
        }
        catch (SQLException e) {
            // behavior of base jdbc
            return ImmutableMap.of();
        }
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // this method should ultimately encompass all the expected teradata data types
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.jdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.REAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                // teradata float is 64 bit
                // trino double is 64 bit
                // teradata float / real / double precision all map to jdbc type float
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
            case Types.DECIMAL:
                return numberMapping(typeHandle);
            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity().orElse(null))));
            case Types.VARCHAR:
                // see prior note on trino case sensitivity
                return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity().orElse(null))));
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private Optional<ColumnMapping> numberMapping(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.requiredColumnSize();
        int scale = typeHandle.requiredDecimalDigits();
        if (precision > Decimals.MAX_PRECISION) {
            // this will trigger for number(*) as precision is 40
            return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale)));
        }
        return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return switch (type) {
            case Type typeInstance when typeInstance == TINYINT -> WriteMapping.longMapping("smallint", tinyintWriteFunction());
            case Type typeInstance when typeInstance == SMALLINT -> WriteMapping.longMapping("smallint", smallintWriteFunction());
            case Type typeInstance when typeInstance == INTEGER -> WriteMapping.longMapping("integer", integerWriteFunction());
            case Type typeInstance when typeInstance == BIGINT -> WriteMapping.longMapping("bigint", bigintWriteFunction());
            case Type typeInstance when typeInstance == REAL -> WriteMapping.longMapping("FLOAT", realWriteFunction());
            case Type typeInstance when typeInstance == DOUBLE -> WriteMapping.doubleMapping("double precision", doubleWriteFunction());
            case Type typeInstance when typeInstance == DATE -> WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
            case DecimalType decimalTypeInstance -> {
                String dataType = String.format("decimal(%s, %s)", decimalTypeInstance.getPrecision(), decimalTypeInstance.getScale());
                if (decimalTypeInstance.isShort()) {
                    yield WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalTypeInstance));
                }
                yield WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalTypeInstance));
            }
            case CharType charTypeInstance -> WriteMapping.sliceMapping("char(" + charTypeInstance.getLength() + ")", charWriteFunction());
            case VarcharType varcharTypeInstance -> {
                String dataType = varcharTypeInstance.isUnbounded()
                        ? "clob"
                        : "varchar(" + varcharTypeInstance.getBoundedLength() + ")";
                yield WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        };
    }
}
