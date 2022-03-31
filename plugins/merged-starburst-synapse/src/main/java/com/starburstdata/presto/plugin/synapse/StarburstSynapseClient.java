/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class StarburstSynapseClient
        extends StarburstSqlServerClient
{
    private static final int MAX_NVARCHAR_LENGTH = 4000;
    private static final int MAX_NCHAR_LENGTH = 4000;
    private static final int MAX_VARBINARY_LENGTH = 8000;

    private static final int MAX_SUPPORTED_TEMPORAL_PRECISION = 7;

    @Inject
    public StarburstSynapseClient(
            BaseJdbcConfig config,
            SqlServerConfig sqlServerConfig,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        super(
                config,
                sqlServerConfig,
                statisticsConfig,
                tableScanRedirection,
                connectionFactory,
                queryBuilder,
                identifierMapping);
    }

    /**
     * Table lock is a prerequisite for `minimal logging` in SQL Server only, not in Synapse.
     * In Synapse CTAS and INSERT...SELECT are both bulk load operations.
     * Synapse does not support table locking.
     *
     * @see <a href="https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-develop-best-practices-transactions#minimal-logging-with-bulk-load">minimal logging</a>
     */
    @Override
    protected boolean isTableLockNeeded(ConnectorSession session)
    {
        return false;
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        String sql = format(
                "RENAME OBJECT %s TO %s",
                quoted(catalogName, schemaName, tableName),
                newTable.getTableName());
        execute(session, sql);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.TIME:
                TimeType timeType = createTimeType(typeHandle.getRequiredDecimalDigits());
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        timeReadFunction(timeType),
                        synapseTimeWriteFunction(timeType.getPrecision())));

            // Synapse does not support text and ntext data types
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.empty();
        }

        return super.toColumnMapping(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "nvarchar(" + MAX_NVARCHAR_LENGTH + ")";
            }
            else if (varcharType.getBoundedLength() > MAX_NVARCHAR_LENGTH) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > MAX_NCHAR_LENGTH) {
                throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(" + MAX_VARBINARY_LENGTH + ")", varbinaryWriteFunction());
        }

        if (type instanceof TimeType) {
            TimeType timeType = (TimeType) type;
            int precision = min(timeType.getPrecision(), MAX_SUPPORTED_TEMPORAL_PRECISION);
            String dataType = format("time(%d)", precision);
            return WriteMapping.longMapping(dataType, synapseTimeWriteFunction(precision));
        }

        return super.toWriteMapping(session, type);
    }

    private LongWriteFunction synapseTimeWriteFunction(int precision)
    {
        return new LongWriteFunction()
        {
            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = round(picosOfDay, 12 - precision);
                if (picosOfDay == PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = fromTrinoTime(picosOfDay);
                statement.setString(index, localTime.toString());
            }
        };
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        // Overriden because Synapse doesn't support ORDER BY...FETCH from SQL Server
        return Optional.of((query, sortItems, limit) -> {
            String start = "SELECT ";
            // This is guaranteed because QueryBuilder#prepareQuery ensures all queries start with SELECT
            verify(query.startsWith(start), "Expected query to start with %s but query was %s", start, query);

            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.getColumn().getColumnName()), ordering);

                        switch (sortItem.getSortOrder()) {
                            case ASC_NULLS_FIRST:
                                // In Synapse ASC implies NULLS FIRST
                            case DESC_NULLS_LAST:
                                // In Synapse DESC implies NULLS LAST
                                return Stream.of(columnSorting);

                            case ASC_NULLS_LAST:
                                return Stream.of(
                                        format("(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) ASC", quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                            case DESC_NULLS_FIRST:
                                return Stream.of(
                                        format("(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) DESC", quoted(sortItem.getColumn().getColumnName())),
                                        columnSorting);
                        }
                        throw new UnsupportedOperationException("Unsupported sort order: " + sortItem.getSortOrder());
                    })
                    .collect(joining(", "));
            return format("SELECT TOP (%d) %s ORDER BY %s", limit, query.substring(start.length()), orderBy);
        });
    }
}
