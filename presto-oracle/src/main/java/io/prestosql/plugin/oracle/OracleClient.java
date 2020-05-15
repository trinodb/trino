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
package io.prestosql.plugin.oracle;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class OracleClient
        extends BaseJdbcClient
{
    private final boolean synonymsEnabled;
    private final int fetchSize = 1000;
    private final int varcharMaxSize;
    private final int timestampDefaultPrecision;
    private final int numberDefaultScale;
    private final RoundingMode numberRoundingMode;

    @Inject
    public OracleClient(
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);

        requireNonNull(oracleConfig, "oracle config is null");
        this.synonymsEnabled = oracleConfig.isSynonymsEnabled();
        this.varcharMaxSize = oracleConfig.getVarcharMaxSize();
        this.timestampDefaultPrecision = oracleConfig.getTimestampDefaultPrecision();
        this.numberDefaultScale = oracleConfig.getNumberDefaultScale();
        this.numberRoundingMode = oracleConfig.getNumberRoundingMode();
    }

    private String[] getTableTypes()
    {
        if (synonymsEnabled) {
            return new String[] {"TABLE", "VIEW", "SYNONYM"};
        }
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                getTableTypes());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }

        String newTableName = newTable.getTableName().toUpperCase(ENGLISH);
        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTableName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        // ORA-02420: missing schema authorization clause
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();

        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.FLOAT:
                if (columnSize == 63) {
                    return Optional.of(realColumnMapping());
                }
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
                int precision = columnSize == 0 ? Decimals.MAX_PRECISION : columnSize;
                int scale = typeHandle.getDecimalDigits();

                if (scale == 0) {
                    return Optional.of(bigintColumnMapping());
                }
                if (scale < 0 || scale > precision) {
                    return Optional.of(decimalColumnMapping(createDecimalType(precision, numberDefaultScale), numberRoundingMode));
                }

                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale), numberRoundingMode));
            case Types.LONGVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof BooleanType) {
            return WriteMapping.booleanMapping("number(1,0)", booleanWriteFunction());
        }
        if (type instanceof TinyintType) {
            return WriteMapping.longMapping("number(3,0)", tinyintWriteFunction());
        }
        if (type instanceof SmallintType) {
            return WriteMapping.longMapping("number(5,0)", smallintWriteFunction());
        }
        if (type instanceof IntegerType) {
            return WriteMapping.longMapping("number(10,0)", integerWriteFunction());
        }
        if (type instanceof BigintType) {
            return WriteMapping.longMapping("number(19,0)", bigintWriteFunction());
        }
        if (type instanceof TimestampType) {
            return WriteMapping.longMapping(format("timestamp(%s)", timestampDefaultPrecision), timestampWriteFunction(session));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return WriteMapping.longMapping(format("timestamp(%s) with time zone", timestampDefaultPrecision), timestampWriteFunction(session));
        }
        if (isVarcharType(type)) {
            if (((VarcharType) type).isUnbounded()) {
                return super.toWriteMapping(session, createVarcharType(varcharMaxSize));
            }
            if (((VarcharType) type).getBoundedLength() > varcharMaxSize) {
                return WriteMapping.sliceMapping("clob", varcharWriteFunction());
            }
        }
        return super.toWriteMapping(session, type);
    }
}
