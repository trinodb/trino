/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunctionUsingSqlTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestampWithRounding;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

public class SalesforceJdbcClient
        extends BaseJdbcClient
{
    private final TableScanRedirection tableScanRedirection;
    private final boolean enableWrites;
    private final Set<SystemTableProvider> systemTables;

    @Inject
    public SalesforceJdbcClient(
            BaseJdbcConfig baseJdbcConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            @EnableWrites boolean enableWrites,
            IdentifierMapping identifierMapping,
            QueryBuilder queryBuilder,
            Set<SystemTableProvider> systemTables)
    {
        super(baseJdbcConfig, "\"", connectionFactory, queryBuilder, identifierMapping);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
        this.enableWrites = enableWrites;
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
        }

        super.createSchema(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
        }

        super.renameSchema(session, schemaName, newSchemaName);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }

        try {
            // Salesforce does not support ALTER TABLE so we cannot use a temporary table then alter it
            return createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
        }

        invalidateDriverCache(session);
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
        }

        JdbcOutputTableHandle outputTableHandle = super.createTable(session, tableMetadata, tableName);
        invalidateDriverCache(session);
        return outputTableHandle;
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        dropTable(session, new JdbcTableHandle(
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                new RemoteTableName(Optional.ofNullable(handle.getCatalogName()), Optional.ofNullable(handle.getSchemaName()), handle.getTableName())));
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
        }

        super.dropTable(session, handle);
        invalidateDriverCache(session);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
        }

        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        if (!enableWrites) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
        }

        // Here we append the "__c" to the table an column names as Salesforce custom objects end in __c
        // and we need to account for this when building the insert statement into Salesforce
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);

        // For CTAS the table name and columns do not end with __c and we must append it to make Salesforce happy
        // For normal INSERTs the names already have __c
        String tableName;
        if (handle.getTemporaryTableName().endsWith("__c")) {
            tableName = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName());
        }
        else {
            tableName = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName() + "__c");
        }

        String columnNames = handle.getColumnNames().stream()
                .map(name -> {
                    if (name.endsWith("__c")) {
                        return name;
                    }

                    return name + "__c";
                })
                .map(this::quoted)
                .collect(joining(", "));

        return format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                columnNames,
                columnWriters.stream()
                        .map(WriteFunction::getBindExpression)
                        .collect(joining(",")));
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
            case Types.VARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());
            case Types.TIME:
                // CData driver does not support getObject, need to use SQL time
                // Additionally Salesforce supports millisecond precision but the driver is truncating it
                // TODO https://starburstdata.atlassian.net/browse/SEP-5893
                return Optional.of(timeColumnMappingUsingSqlTime());
            case Types.TIMESTAMP:
                // CData driver does not support getObject, need to use SQL timestamp
                // Additionally Salesforce supports millisecond precision but the driver is truncating it
                // TODO https://starburstdata.atlassian.net/browse/SEP-5893
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_SECONDS));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        verify(enableWrites, "Writes disabled");

        if (type.equals(IntegerType.INTEGER)) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }

        if (type.equals(BIGINT)) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type.equals(DOUBLE)) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
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

        if (type.equals(DATE)) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunctionUsingSqlTime());
        }

        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(TIMESTAMP_SECONDS));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(TIMESTAMP_SECONDS));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected String generateTemporaryTableName()
    {
        throw new UnsupportedOperationException("This connector does not support temporary table names");
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        // This function is overridden to ensure we have a class in the com.starburstdata.* package when running queries
        // Without it, the query would fail with a CData licensing error
        return super.getPreparedStatement(connection, sql);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return systemTables.stream()
                .filter(systemTable -> systemTable.getSchemaTableName().equals(tableName))
                .collect(toOptional())
                .map(SystemTableProvider::create);
    }

    private void invalidateDriverCache(ConnectorSession session)
    {
        // Reset CData metadata cache after creating a table so it will be visible
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            statement.execute("RESET SCHEMA CACHE");
        }
        catch (SQLException throwables) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to reset metadata cache after create table operation");
        }
    }

    /**
     * Copy of {@link io.trino.plugin.jdbc.StandardColumnMappings#timeColumnMappingUsingSqlTime} but using second precision
     */
    private static ColumnMapping timeColumnMappingUsingSqlTime()
    {
        return ColumnMapping.longMapping(
                TIME_SECONDS,
                (resultSet, columnIndex) -> {
                    Time time = resultSet.getTime(columnIndex);
                    return toLocalTime(time).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND;
                },
                timeWriteFunctionUsingSqlTime(),
                PredicatePushdownController.DISABLE_PUSHDOWN);
    }

    private static LocalTime toLocalTime(Time sqlTime)
    {
        // Time.toLocalTime() does not preserve second fraction
        return sqlTime.toLocalTime()
                .withNano(toIntExact(MILLISECONDS.toNanos(floorMod(sqlTime.getTime(), 1000L))));
    }
}
