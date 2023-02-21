/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.sqlserver.ImplementAvgBigint;
import io.trino.plugin.sqlserver.ImplementSqlServerCountBig;
import io.trino.plugin.sqlserver.ImplementSqlServerCountBigAll;
import io.trino.plugin.sqlserver.ImplementSqlServerStddevPop;
import io.trino.plugin.sqlserver.ImplementSqlServerStdev;
import io.trino.plugin.sqlserver.ImplementSqlServerVariance;
import io.trino.plugin.sqlserver.ImplementSqlServerVariancePop;
import io.trino.plugin.sqlserver.RewriteUnicodeVarcharConstant;
import io.trino.plugin.sqlserver.SqlServerClient;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
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
        extends SqlServerClient
{
    private static final int MAX_NVARCHAR_LENGTH = 4000;
    private static final int MAX_NCHAR_LENGTH = 4000;
    private static final int MAX_VARBINARY_LENGTH = 8000;

    private static final int MAX_SUPPORTED_TEMPORAL_PRECISION = 7;

    private static final Joiner DOT_JOINER = Joiner.on(".");

    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;

    @Inject
    public StarburstSynapseClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, statisticsConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);

        // TODO: Remove once https://starburstdata.atlassian.net/browse/SEP-10133 is addressed
        // Explicitly copied from SQL Server to remove some connector expression rewrites since it's unsafe for varchars due to default case-insensitive collation of Synapse
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .add(new RewriteUnicodeVarcharConstant())
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementSqlServerCountBigAll())
                        .add(new ImplementSqlServerCountBig())
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(StarburstSynapseClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementSqlServerStdev())
                        .add(new ImplementSqlServerStddevPop())
                        .add(new ImplementSqlServerVariance())
                        .add(new ImplementSqlServerVariancePop())
                        // SQL Server doesn't have covar_samp and covar_pop functions so we can't implement pushdown for them
                        .build());
    }

    // TODO: Remove once https://starburstdata.atlassian.net/browse/SEP-10133 is addressed
    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    // TODO: Remove once https://starburstdata.atlassian.net/browse/SEP-10133 is addressed
    @Override
    public Optional<String> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    // TODO: Remove once https://starburstdata.atlassian.net/browse/SEP-10133 is addressed
    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
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
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        String sql = format(
                "RENAME OBJECT %s TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                newRemoteTableName);
        execute(session, connection, sql);
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        String columnFrom = DOT_JOINER.join(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName(), remoteColumnName);

        try (CallableStatement renameColumn = connection.prepareCall("exec sp_rename ?, ?, 'COLUMN'")) {
            renameColumn.setString(1, columnFrom);
            renameColumn.setString(2, newRemoteColumnName);
            renameColumn.execute();
        }
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

    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, JdbcTableHandle tableHandle)
    {
        if (tableHandle.isSynthetic()) {
            return ImmutableMap.of();
        }
        RemoteTableName remoteTableName = tableHandle.asPlainTable().getRemoteTableName();

        return Jdbi.open(connection).createQuery("""
                        SELECT c.name AS column_name, collation_name  FROM sys.columns c
                        INNER JOIN sys.tables t on c.object_id = t.object_id
                        INNER JOIN sys.schemas s on s.schema_id = t.schema_id
                        WHERE s.name = :schema_name and t.name = :table_name
                        """)
                .bind("schema_name", remoteTableName.getSchemaName().orElseThrow())
                .bind("table_name", remoteTableName.getTableName())
                .collectRows(toImmutableMap(rowView -> rowView.getColumn("column_name", String.class),
                        rowView -> getCaseSensitivityForCollation(rowView.getColumn("collation_name", String.class))));
    }

    private static CaseSensitivity getCaseSensitivityForCollation(String collation)
    {
        if (collation == null || collation.isEmpty()) {
            return CASE_INSENSITIVE;
        }

        return collation.endsWith("BIN") || collation.endsWith("BIN2") || collation.contains("_CS_") ? CASE_SENSITIVE : CASE_INSENSITIVE;
    }
}
