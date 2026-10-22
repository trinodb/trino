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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.ydb.YdbColumnMappings.dateColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.decimalColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.timestampColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedBigintColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedColumnMapping;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;

public class YdbClient
        extends BaseJdbcClient
{
    static final String DEFAULT_SCHEMA = "default";
    private static final int YDB_DEFAULT_DECIMAL_PRECISION = 22;
    private static final int YDB_DEFAULT_DECIMAL_SCALE = 9;

    @Inject
    public YdbClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("`",
                connectionFactory,
                queryBuilder,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping,
                remoteQueryModifier,
                false);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableSet.of(DEFAULT_SCHEMA);
    }

    @Override
    protected String escapeObjectNameForMetadataQuery(String name, String escape)
    {
        return name;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // default is a Trino schema; YDB JDBC has no physical schema with that name.
        return super.getTables(
                connection,
                remoteSchemaName.filter(schema -> !DEFAULT_SCHEMA.equalsIgnoreCase(schema)),
                remoteTableName);
    }

    @Override
    protected boolean filterRemoteSchema(String schemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(schemaName);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        return filterRemoteSchema(table.getSchemaName()) ? super.getTableHandle(session, table) : Optional.empty();
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
    {
        return DEFAULT_SCHEMA;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session,
            Connection connection,
            JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.jdbcTypeName().orElse("").toLowerCase(Locale.ROOT);
        if (jdbcTypeName.equals("bytes") || jdbcTypeName.equals("string")) {
            return Optional.of(varbinaryColumnMapping());
        }
        if (jdbcTypeName.equals("utf8") || jdbcTypeName.equals("text")) {
            return Optional.of(unboundedVarcharColumnMapping());
        }
        Optional<ColumnMapping> primitiveMapping = switch (jdbcTypeName) {
            case "bool" -> Optional.of(booleanColumnMapping());
            case "int8" -> Optional.of(tinyintColumnMapping());
            case "int16" -> Optional.of(smallintColumnMapping());
            case "int32" -> Optional.of(integerColumnMapping());
            case "int64" -> Optional.of(bigintColumnMapping());
            case "uint8" -> Optional.of(unsignedColumnMapping(SMALLINT, 2));
            case "uint16" -> Optional.of(unsignedColumnMapping(INTEGER, 4));
            case "uint32" -> Optional.of(unsignedColumnMapping(BIGINT, 6));
            case "uint64" -> Optional.of(unsignedBigintColumnMapping());
            case "float" -> {
                ColumnMapping valueMapping = realColumnMapping();
                yield Optional.of(ColumnMapping.mapping(REAL, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "double" -> {
                ColumnMapping valueMapping = doubleColumnMapping();
                yield Optional.of(ColumnMapping.mapping(DOUBLE, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "date", "date32" -> Optional.of(dateColumnMapping(typeHandle));
            case "datetime", "datetime64", "timestamp", "timestamp64" -> Optional.of(timestampColumnMapping(typeHandle));
            default -> Optional.empty();
        };
        if (primitiveMapping.isPresent()) {
            return primitiveMapping;
        }
        if (typeHandle.jdbcType() != Types.DECIMAL) {
            return getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR ? mapToUnboundedVarchar(typeHandle) : Optional.empty();
        }

        String typeName = typeHandle.jdbcTypeName().orElse("Decimal");
        int precision = typeHandle.columnSize().orElse(YDB_DEFAULT_DECIMAL_PRECISION);
        int scale = typeHandle.decimalDigits().orElse(YDB_DEFAULT_DECIMAL_SCALE);
        int start = typeName.indexOf('(');
        int end = typeName.indexOf(')');
        if (start >= 0 && end > start) {
            String[] parts = typeName.substring(start + 1, end).split(",");
            if (parts.length == 2) {
                Integer typeNamePrecision = Ints.tryParse(parts[0].trim());
                Integer typeNameScale = Ints.tryParse(parts[1].trim());
                if (typeNamePrecision != null && typeNameScale != null) {
                    precision = typeNamePrecision;
                    scale = typeNameScale;
                }
            }
        }

        DecimalType decimalType = createDecimalType(precision, max(scale, 0));
        ColumnMapping decimalMapping = decimalColumnMapping(decimalType);
        return Optional.of(ColumnMapping.mapping(
                decimalType,
                decimalMapping.getReadFunction(),
                decimalMapping.getWriteFunction(),
                DISABLE_PUSHDOWN));
    }

    private static ColumnMapping unboundedVarcharColumnMapping()
    {
        VarcharType varcharType = createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        // YDB doesn't use catalog & schema in table names, only the table path
        return quoted(table);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support writing");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Consumer<Runnable> rollbackActionCollector)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping not null constraints");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
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
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }
}
