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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMappingUsingSqlTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestampWithRounding;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;

public class DruidJdbcClient
        extends BaseJdbcClient
{
    // Druid maintains its datasources related metadata by setting the catalog name as "druid"
    // Note that while a user may name the catalog name as something else, metadata queries made
    // to druid will always have the TABLE_CATALOG set to DRUID_CATALOG
    private static final String DRUID_CATALOG = "druid";
    // All the datasources in Druid are created under schema "druid"
    public static final String DRUID_SCHEMA = "druid";

    @Inject
    public DruidJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping)
    {
        super(config, "\"", connectionFactory, queryBuilder, identifierMapping);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableList.of(DRUID_SCHEMA);
    }

    //Overridden to filter out tables that don't match schemaTableName
    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String jdbcSchemaName = schemaTableName.getSchemaName();
        String jdbcTableName = schemaTableName.getTableName();
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getTables(connection, Optional.of(jdbcSchemaName), Optional.of(jdbcTableName))) {
            List<JdbcTableHandle> tableHandles = new ArrayList<>();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                String tableName = resultSet.getString("TABLE_NAME");
                if (Objects.equals(schemaName, jdbcSchemaName) && Objects.equals(tableName, jdbcTableName)) {
                    tableHandles.add(new JdbcTableHandle(
                            schemaTableName,
                            DRUID_CATALOG,
                            schemaName,
                            tableName));
                }
            }
            if (tableHandles.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(getOnlyElement(tableHandles));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    /*
     * Overridden since the {@link BaseJdbcClient#getTables(Connection, Optional, Optional)}
     * method uses character escaping that doesn't work well with Druid's Avatica handler.
     * Unfortunately, because we can't escape search characters like '_' and '%", this call
     * ends up retrieving metadata for all tables that match the search
     * pattern. For ex - LIKE some_table matches somertable, somextable and some_table.
     *
     * See getTableHandle(JdbcIdentity, SchemaTableName)} to look at
     * how tables are filtered.
     */
    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(DRUID_CATALOG,
                DRUID_SCHEMA,
                tableName.orElse(null),
                null);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        // TODO (https://github.com/trinodb/trino/issues/9215) Implement proper type mapping and add test
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.VARCHAR:
                int columnSize = typeHandle.getRequiredColumnSize();
                if (columnSize == -1) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(columnSize, true));

            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());

            case Types.TIME:
                // TODO Consider using `StandardColumnMappings.timeColumnMapping`
                return Optional.of(timeColumnMappingUsingSqlTime());

            case Types.TIMESTAMP:
                // TODO Consider using `StandardColumnMappings.timestampColumnMapping`
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return legacyToWriteMapping(type);
    }

    @Override
    protected PreparedQuery prepareQuery(ConnectorSession session, Connection connection, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, String> columnExpressions, Optional<JdbcSplit> split)
    {
        return super.prepareQuery(session, connection, prepareTableHandleForQuery(table), groupingSets, columns, columnExpressions, split);
    }

    private JdbcTableHandle prepareTableHandleForQuery(JdbcTableHandle table)
    {
        if (table.isNamedRelation()) {
            String schemaName = table.getSchemaName();
            checkArgument("druid".equals(schemaName), "Only \"druid\" schema is supported");

            table = new JdbcTableHandle(
                    new JdbcNamedRelationHandle(
                            table.getRequiredNamedRelation().getSchemaTableName(),
                            // Druid doesn't like table names to be qualified with catalog names in the SQL query, hence we null out the catalog.
                            new RemoteTableName(
                                    Optional.empty(),
                                    table.getRequiredNamedRelation().getRemoteTableName().getSchemaName(),
                                    table.getRequiredNamedRelation().getRemoteTableName().getTableName())),
                    table.getConstraint(),
                    table.getConstraintExpressions(),
                    table.getSortOrder(),
                    table.getLimit(),
                    table.getColumns(),
                    table.getOtherReferencedTables(),
                    table.getNextSyntheticColumnId());
        }

        return table;
    }

    /*
     * Overridden since the {@link BaseJdbcClient#getColumns(JdbcTableHandle, DatabaseMetaData)}
     * method uses character escaping that doesn't work well with Druid's Avatica handler.
     * Unfortunately, because we can't escape search characters like '_' and '%",
     * this call ends up retrieving columns for all tables that match the search
     * pattern. For ex - LIKE some_table matches somertable, somextable and some_table.
     *
     * See getColumns(ConnectorSession, JdbcTableHandle)} to look at tables are filtered.
     */
    @Override
    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                null);
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
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        // DELETE statement is not yet support in Druid (Avatica JDBC, see https://issues.apache.org/jira/browse/CALCITE-706)
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
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

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
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
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    private WriteMapping legacyToWriteMapping(Type type)
    {
        // TODO (https://github.com/trinodb/trino/issues/497) Implement proper type mapping and add test
        // This method is copied from deprecated BaseJdbcClient.legacyToWriteMapping()
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
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
