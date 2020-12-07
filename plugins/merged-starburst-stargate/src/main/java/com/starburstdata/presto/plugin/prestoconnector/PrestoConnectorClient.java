/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.google.common.base.VerifyException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class PrestoConnectorClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PrestoConnectorClient.class);

    private enum FunctionsCacheKey
    {
        SINGLETON
    }

    private final boolean enableWrites;
    private final Cache<FunctionsCacheKey, Set<String>> supportedAggregateFunctions;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;

    @Inject
    public PrestoConnectorClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, @EnableWrites boolean enableWrites)
    {
        super(config, "\"", connectionFactory);
        this.enableWrites = enableWrites;

        this.supportedAggregateFunctions = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(this::quoted, Set.of(
                new PrestoAggregateFunctionRewriteRule(
                        this::getSupportedAggregateFunctions,
                        this::toTypeHandle)));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(handle.getRemoteTableName()),
                getColumnDefinitionSql(session, column, column.getName()));
        execute(session, sql);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.isPresent() ? format("'%s'", comment.get()) : "NULL");
        execute(session, sql);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
        }
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables");
        }
        super.createTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping tables");
        }
        super.dropTable(session, handle);
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming tables");
        }
        super.renameTable(session, handle, newTableName);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support renaming columns");
        }
        super.renameColumn(session, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        if (!enableWrites) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns");
        }
        super.dropColumn(session, handle, column);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> columnMapping = convertToPrestoType(session, connection, typeHandle);
        columnMapping.ifPresent(mapping -> {
            // Ensure toTypeHandle stays up to date when we add new type mappings
            Type type = mapping.getType();
            JdbcTypeHandle syntheticTypeHandle = toTypeHandle(type)
                    .orElseThrow(() -> new VerifyException(format("Cannot convert type %s [%s] back to JdbcTypeHandle", type, typeHandle)));
            ColumnMapping mappingForSyntheticHandle = convertToPrestoType(session, connection, typeHandle)
                    .orElseThrow(() -> new VerifyException(format("JdbcTypeHandle %s constructed for %s [%s] cannot be converted to type", syntheticTypeHandle, type, typeHandle)));
            verify(
                    mappingForSyntheticHandle.getType().equals(type),
                    "Type mismatch, original type is %s [%s], converted type is %s [%s]",
                    type,
                    typeHandle,
                    mappingForSyntheticHandle.getType(),
                    syntheticTypeHandle);
        });
        return columnMapping;
    }

    private Optional<ColumnMapping> convertToPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // TODO
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        // TODO
        return super.toWriteMapping(session, type);
    }

    private Optional<JdbcTypeHandle> toTypeHandle(Type type)
    {
        requireNonNull(type, "type is null");

        if (type == BOOLEAN) {
            return Optional.of(jdbcTypeHandle(Types.BOOLEAN));
        }

        if (type == TINYINT) {
            return Optional.of(jdbcTypeHandle(Types.TINYINT));
        }
        if (type == SMALLINT) {
            return Optional.of(jdbcTypeHandle(Types.SMALLINT));
        }
        if (type == INTEGER) {
            return Optional.of(jdbcTypeHandle(Types.INTEGER));
        }
        if (type == BIGINT) {
            return Optional.of(jdbcTypeHandle(Types.BIGINT));
        }

        if (type == REAL) {
            return Optional.of(jdbcTypeHandle(Types.REAL));
        }
        if (type == DOUBLE) {
            return Optional.of(jdbcTypeHandle(Types.DOUBLE));
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.empty(), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
        }

        if (type instanceof CharType) {
            return Optional.of(jdbcTypeHandleWithColumnSize(Types.CHAR, ((CharType) type).getLength()));
        }
        if (type instanceof VarcharType) {
            // See io.prestosql.connector.system.jdbc.ColumnJdbcTable#columnSize
            int columnSize = ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
            return Optional.of(jdbcTypeHandleWithColumnSize(Types.VARCHAR, columnSize));
        }

        if (type == VARBINARY) {
            return Optional.of(jdbcTypeHandle(Types.VARBINARY));
        }

        if (type == DATE) {
            return Optional.of(jdbcTypeHandle(Types.DATE));
        }

        if (type instanceof TimeType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME, ((TimeType) type).getPrecision()));
        }
        if (type instanceof TimeWithTimeZoneType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME_WITH_TIMEZONE, ((TimeWithTimeZoneType) type).getPrecision()));
        }

        if (type instanceof TimestampType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME, ((TimestampType) type).getPrecision()));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return Optional.of(jdbcTypeHandleWithDecimalDigits(Types.TIME_WITH_TIMEZONE, ((TimestampWithTimeZoneType) type).getPrecision()));
        }

        log.debug("Type cannot be converted to JdbcTypeHandle: %s", type);
        return Optional.empty();
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private Set<String> getSupportedAggregateFunctions(ConnectorSession session)
    {
        try {
            return supportedAggregateFunctions.get(FunctionsCacheKey.SINGLETON, () -> {
                try {
                    return listAggregateFunctions(session);
                }
                // Catch exceptions from the driver only. Any other exception is likely bug in the code.
                catch (SQLException e) {
                    log.warn(e, "Failed to list aggregate functions");
                    // If we reached aggregation pushdown, the remote cluster is likely up & running and so it may not
                    // be safe to retry the listing immediately. Cache the failure.
                    return Set.of();
                }
            });
        }
        catch (ExecutionException e) {
            // Impossible, as the loader does not throw checked exceptions
            throw new RuntimeException(e);
        }
    }

    private Set<String> listAggregateFunctions(ConnectorSession session)
            throws SQLException
    {
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
            ImmutableSet.Builder<String> functions = ImmutableSet.builder();
            while (resultSet.next()) {
                if ("aggregate".equals(resultSet.getString("Function Type"))) {
                    functions.add(resultSet.getString("Function"));
                }
            }
            return functions.build();
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

    private static JdbcTypeHandle jdbcTypeHandle(int jdbcType)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle jdbcTypeHandleWithColumnSize(int jdbcType, int columnSize)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.of(columnSize), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle jdbcTypeHandleWithDecimalDigits(int jdbcType, int decimalDigits)
    {
        return new JdbcTypeHandle(jdbcType, Optional.empty(), Optional.empty(), Optional.of(decimalDigits), Optional.empty(), Optional.empty());
    }
}
