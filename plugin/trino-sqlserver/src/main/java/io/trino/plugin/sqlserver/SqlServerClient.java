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
package io.trino.plugin.sqlserver;

import com.google.common.base.Enums;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.isBulkCopyForWrite;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.isBulkCopyForWriteLockDestinationTable;
import static io.trino.plugin.sqlserver.SqlServerTableProperties.DATA_COMPRESSION;
import static io.trino.plugin.sqlserver.SqlServerTableProperties.getDataCompression;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(SqlServerClient.class);

    // SqlServer supports 2100 parameters in prepared statement, let's create a space for about 4 big IN predicates
    public static final int SQL_SERVER_MAX_LIST_EXPRESSIONS = 500;

    public static final int SQL_SERVER_DEADLOCK_ERROR_CODE = 1205;

    public static final JdbcTypeHandle BIGINT_TYPE = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private static final Joiner DOT_JOINER = Joiner.on(".");

    private final boolean statisticsEnabled;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private static final int MAX_SUPPORTED_TEMPORAL_PRECISION = 7;

    private static final PredicatePushdownController SQLSERVER_CHARACTER_PUSHDOWN = (session, domain) -> {
        if (domain.isNullableSingleValue()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Push down inequality predicate
            ValueSet complement = simplifiedDomain.getValues().complement();
            if (complement.isDiscreteSet()) {
                return FULL_PUSHDOWN.apply(session, simplifiedDomain);
            }
            // Domain#simplify can turn a discrete set into a range predicate
            // Push down of range predicate for varchar/char types could lead to incorrect results
            // when the remote database is case insensitive
            return DISABLE_PUSHDOWN.apply(session, domain);
        }
        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };

    @Inject
    public SqlServerClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);

        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL)))
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementSqlServerCountBigAll())
                        .add(new ImplementSqlServerCountBig())
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(SqlServerClient::toTypeHandle))
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

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            // SQL Server doesn't support CASCADE option https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-schema-transact-sql
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
        execute(session, connection, "DROP SCHEMA " + quoted(remoteSchemaName));
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        JdbcOutputTableHandle table = super.beginCreateTable(session, tableMetadata);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        JdbcOutputTableHandle table = super.beginInsertTable(session, tableHandle, columns);
        enableTableLockOnBulkLoadTableOption(session, table);
        return table;
    }

    protected void enableTableLockOnBulkLoadTableOption(ConnectorSession session, JdbcOutputTableHandle table)
    {
        if (!isTableLockNeeded(session)) {
            return;
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            // 'table lock on bulk load' table option causes the bulk load processes on user-defined tables to obtain a bulk update lock
            // note: this is not a request to lock a table immediately
            String sql = format("EXEC sp_tableoption '%s', 'table lock on bulk load', '1'",
                    quoted(table.getCatalogName(), table.getSchemaName(), table.getTemporaryTableName().orElseGet(table::getTableName)));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    /**
     * Table lock is a prerequisite for `minimal logging` in SQL Server
     *
     * @see <a href="https://docs.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import">minimal logging</a>
     */
    protected boolean isTableLockNeeded(ConnectorSession session)
    {
        return isBulkCopyForWrite(session) && isBulkCopyForWriteLockDestinationTable(session);
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // SQL Server truncates table name to the max length silently when renaming a table
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        // SQL Server truncates table name to the max length silently when renaming a column
        // SQL Server driver doesn't communicate with a server in getMaxColumnNameLength. The cost to call this method per column is low.
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'", databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        // sp_rename treats first argument as SQL object name, so it needs to be properly quoted and escaped.
        // The second argument is treated literally.
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        String fullTableFromName = DOT_JOINER.join(
                quoted(catalogName),
                quoted(remoteSchemaName),
                quoted(remoteTableName));

        try (CallableStatement renameTable = connection.prepareCall("exec sp_rename ?, ?")) {
            renameTable.setString(1, fullTableFromName);
            renameTable.setString(2, newRemoteTableName);
            renameTable.execute();
        }
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        // sp_rename treats first argument as SQL object name, so it needs to be properly quoted and escaped.
        // The second arqgument is treated literally.
        String columnFrom = DOT_JOINER.join(
                quoted(remoteTableName.getCatalogName().orElseThrow()),
                quoted(remoteTableName.getSchemaName().orElseThrow()),
                quoted(remoteTableName.getTableName()),
                quoted(remoteColumnName));

        try (CallableStatement renameColumn = connection.prepareCall("exec sp_rename ?, ?, 'COLUMN'")) {
            renameColumn.setString(1, columnFrom);
            renameColumn.setString(2, newRemoteColumnName);
            renameColumn.execute();
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "SELECT %s INTO %s FROM %s WHERE 0 = 1",
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
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
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, JdbcTableHandle tableHandle)
    {
        if (tableHandle.isSynthetic()) {
            return ImmutableMap.of();
        }
        PreparedQuery preparedQuery = new PreparedQuery(format("SELECT * FROM %s", quoted(tableHandle.asPlainTable().getRemoteTableName())), ImmutableList.of());

        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            ImmutableMap.Builder<String, CaseSensitivity> columns = ImmutableMap.builder();
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                String name = metadata.getColumnName(column);
                columns.put(name, metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            return columns.buildOrThrow();
        }
        catch (SQLException e) {
            if (e instanceof SQLServerException sqlServerException && sqlServerException.getSQLServerError().getErrorNumber() == 208) {
                // The 208 indicates that the object doesn't exist or lack of permission.
                // Throw TableNotFoundException because users shouldn't see such tables if they don't have the permission.
                // TableNotFoundException will be suppressed when listing information_schema.
                // https://learn.microsoft.com/sql/relational-databases/errors-events/mssqlserver-208-database-engine-error
                throw new TableNotFoundException(tableHandle.asPlainTable().getSchemaTableName());
            }
            throw new TrinoException(JDBC_ERROR, "Failed to get case sensitivity for columns. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        switch (jdbcTypeName) {
            case "varbinary":
                return Optional.of(varbinaryColumnMapping());
            case "datetimeoffset":
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                // Map SQL Server TINYINT to Trino SMALLINT because SQL Server TINYINT is actually "unsigned tinyint"
                // We don't check the range of values, because SQL Server will do it for us, and this behavior has already
                // been tested in `BaseSqlServerTypeMapping#testUnsupportedTinyint`
                return Optional.of(smallintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL: {
                int columnSize = typeHandle.getRequiredColumnSize();
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                // TODO does sql server support negative scale?
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            }

            case Types.CHAR:
            case Types.NCHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize(), typeHandle.getCaseSensitivity().orElse(CASE_INSENSITIVE) == CASE_SENSITIVE));

            case Types.VARCHAR:
            case Types.NVARCHAR:
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize(), typeHandle.getCaseSensitivity().orElse(CASE_INSENSITIVE) == CASE_SENSITIVE));

            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(longVarcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        sqlServerDateReadFunction(),
                        sqlServerDateWriteFunction()));

            case Types.TIME:
                TimeType timeType = createTimeType(typeHandle.getRequiredDecimalDigits());
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        timeReadFunction(timeType),
                        sqlServerTimeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                int precision = typeHandle.getRequiredDecimalDigits();
                return Optional.of(timestampColumnMapping(createTimestampType(precision)));
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
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
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
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType charType) {
            String dataType;
            if (charType.getLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary(max)", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", sqlServerDateWriteFunction());
        }

        if (type instanceof TimeType timeType) {
            int precision = min(timeType.getPrecision(), MAX_SUPPORTED_TEMPORAL_PRECISION);
            String dataType = format("time(%d)", precision);
            return WriteMapping.longMapping(dataType, sqlServerTimeWriteFunction(precision));
        }

        if (type instanceof TimestampType timestampType) {
            int precision = min(timestampType.getPrecision(), MAX_SUPPORTED_TEMPORAL_PRECISION);
            String dataType = format("datetime2(%d)", precision);
            if (timestampType.getPrecision() <= MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(timestampType, precision));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            try (Statement statement = connection.createStatement();
                    // When FMTONLY is ON , a rowset is returned with the column names for the query
                    ResultSet resultSet = statement.executeQuery("set fmtonly on %s \nset fmtonly off".formatted(procedureQuery.query()))) {
                ResultSetMetaData metadata = resultSet.getMetaData();
                if (metadata == null) {
                    throw new TrinoException(NOT_SUPPORTED, "Procedure not supported: ResultSetMetaData not available for query: " + procedureQuery.query());
                }
                JdbcProcedureHandle procedureHandle = new JdbcProcedureHandle(procedureQuery, getColumns(session, connection, metadata));
                if (statement.getMoreResults()) {
                    throw new TrinoException(NOT_SUPPORTED, "Procedure has multiple ResultSets for query: " + procedureQuery.query());
                }
                return procedureHandle;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to get table handle for procedure query. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            String catalog = remoteTableName.getCatalogName().orElse(null);
            String schema = remoteTableName.getSchemaName().orElse(null);
            String tableName = remoteTableName.getTableName();

            StatisticsDao statisticsDao = new StatisticsDao(handle);
            Long tableObjectId = statisticsDao.getTableObjectId(catalog, schema, tableName);
            if (tableObjectId == null) {
                // Table not found
                return TableStatistics.empty();
            }

            Long rowCount = statisticsDao.getRowCount(tableObjectId);
            if (rowCount == null) {
                // Table disappeared
                return TableStatistics.empty();
            }

            if (rowCount == 0) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Map<String, String> columnNameToStatisticsName = getColumnNameToStatisticsName(table, statisticsDao, tableObjectId);

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                String statisticName = columnNameToStatisticsName.get(column.getColumnName());
                if (statisticName == null) {
                    // No statistic for column
                    continue;
                }

                double averageColumnLength;
                long notNullValues = 0;
                long nullValues = 0;
                long distinctValues = 0;

                try (CallableStatement showStatistics = handle.getConnection().prepareCall("DBCC SHOW_STATISTICS (?, ?)")) {
                    showStatistics.setString(1, format("%s.%s.%s", catalog, schema, tableName));
                    showStatistics.setString(2, statisticName);

                    boolean isResultSet = showStatistics.execute();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return a result set");
                    try (ResultSet resultSet = showStatistics.getResultSet()) {
                        checkState(resultSet.next(), "No rows in result set");

                        averageColumnLength = resultSet.getDouble("Average Key Length"); // NULL values are accounted for with length 0

                        checkState(!resultSet.next(), "More than one row in result set");
                    }

                    isResultSet = showStatistics.getMoreResults();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return second result set");
                    showStatistics.getResultSet().close();

                    isResultSet = showStatistics.getMoreResults();
                    checkState(isResultSet, "Expected SHOW_STATISTICS to return third result set");
                    try (ResultSet resultSet = showStatistics.getResultSet()) {
                        while (resultSet.next()) {
                            resultSet.getObject("RANGE_HI_KEY");
                            if (resultSet.wasNull()) {
                                // Null fraction
                                checkState(resultSet.getLong("RANGE_ROWS") == 0, "Unexpected RANGE_ROWS for null fraction");
                                checkState(resultSet.getLong("DISTINCT_RANGE_ROWS") == 0, "Unexpected DISTINCT_RANGE_ROWS for null fraction");
                                checkState(nullValues == 0, "Multiple null fraction entries");
                                nullValues += resultSet.getLong("EQ_ROWS");
                            }
                            else {
                                // TODO discover min/max from resultSet.getXxx("RANGE_HI_KEY")
                                notNullValues += resultSet.getLong("RANGE_ROWS") // rows strictly within a bucket
                                        + resultSet.getLong("EQ_ROWS"); // rows equal to RANGE_HI_KEY
                                distinctValues += resultSet.getLong("DISTINCT_RANGE_ROWS") // NDV strictly within a bucket
                                        + (resultSet.getLong("EQ_ROWS") > 0 ? 1 : 0);
                            }
                        }
                    }
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(
                                (notNullValues + nullValues == 0)
                                        ? 1
                                        : (1.0 * nullValues / (notNullValues + nullValues))))
                        .setDistinctValuesCount(Estimate.of(distinctValues))
                        .setDataSize(Estimate.of(rowCount * averageColumnLength))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static Map<String, String> getColumnNameToStatisticsName(JdbcTableHandle table, StatisticsDao statisticsDao, Long tableObjectId)
    {
        List<String> singleColumnStatistics = statisticsDao.getSingleColumnStatistics(tableObjectId);

        Map<String, String> columnNameToStatisticsName = new HashMap<>();
        for (String statisticName : singleColumnStatistics) {
            String columnName = statisticsDao.getSingleColumnStatisticsColumnName(tableObjectId, statisticName);
            if (columnName == null) {
                // Table or statistics disappeared
                continue;
            }

            if (columnNameToStatisticsName.putIfAbsent(columnName, statisticName) != null) {
                log.debug("Multiple statistics for %s in %s: %s and %s", columnName, table, columnNameToStatisticsName.get(columnName), statisticName);
            }
        }
        return columnNameToStatisticsName;
    }

    // SQL Server has non-standard LIKE semantics:
    // https://learn.microsoft.com/en-us/sql/t-sql/language-elements/like-transact-sql?redirectedfrom=MSDN&view=sql-server-ver16#arguments
    // and apparently this applies to DatabaseMetaData calls too.@Override
    @Override
    protected String escapeObjectNameForMetadataQuery(String name, String escape)
    {
        requireNonNull(name, "name is null");
        requireNonNull(escape, "escape is null");
        checkArgument(!escape.isEmpty(), "Escape string must not be empty");
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        // SQLServer also treats [ and ] as wildcard characters
        name = name.replace("]", escape + "]");
        name = name.replace("[", escape + "[");
        return name;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    private LongWriteFunction sqlServerTimeWriteFunction(int precision)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                // Binding setObject(LocalTime) can result with "The data types time and datetime are incompatible in the equal to operator."
                // when write function is used for predicate pushdown.
                return format("CAST(? AS time(%s))", precision);
            }

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

    private static LongWriteFunction sqlServerDateWriteFunction()
    {
        return (statement, index, day) -> statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static LongReadFunction sqlServerDateReadFunction()
    {
        return (resultSet, index) -> LocalDate.parse(resultSet.getString(index), DATE_FORMATTER).toEpochDay();
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Unsupported datetimeoffset precision %s", precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampWithTimeZoneType(precision),
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(
                createTimestampWithTimeZoneType(precision),
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction());
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
            ZonedDateTime zonedDateTime = offsetDateTime.toZonedDateTime();
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            TimeZoneKey.getTimeZoneKey(offsetDateTime.toZonedDateTime().getZone().getId()));
                });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochMillis = value.getEpochMillis();
                    long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
                    int nanoAdjustment = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
                    Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                    statement.setObject(index, OffsetDateTime.ofInstant(instant, zoneId));
                });
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT TOP %s * FROM (%s) o", limit, sql));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.getColumn().getColumnName()), ordering);

                        switch (sortItem.getSortOrder()) {
                            case ASC_NULLS_FIRST:
                                // In SQL Server ASC implies NULLS FIRST
                            case DESC_NULLS_LAST:
                                // In SQL Server DESC implies NULLS LAST
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
            return format("%s ORDER BY %s OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        if (joinCondition.getOperator() == JoinCondition.Operator.IS_DISTINCT_FROM) {
            // Not supported in SQL Server
            return false;
        }

        boolean isVarcharJoinColumn = Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .allMatch(type -> type instanceof CharType || type instanceof VarcharType);
        if (isVarcharJoinColumn) {
            JoinCondition.Operator operator = joinCondition.getOperator();
            return switch (operator) {
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> false;
                case EQUAL, NOT_EQUAL -> isCaseSensitiveVarchar(joinCondition.getLeftColumn()) && isCaseSensitiveVarchar(joinCondition.getRightColumn());
                default -> false;
            };
        }

        return true;
    }

    private boolean isCaseSensitiveVarchar(JdbcColumnHandle columnHandle)
    {
        return columnHandle.getJdbcTypeHandle().getCaseSensitivity().orElse(CASE_INSENSITIVE) == CASE_SENSITIVE;
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        return format(
                "CREATE TABLE %s (%s) %s",
                quoted(remoteTableName),
                join(", ", columns),
                getDataCompression(tableMetadata.getProperties())
                        .map(dataCompression -> format("WITH (DATA_COMPRESSION = %s)", dataCompression))
                        .orElse(""));
    }

    @Override
    protected String postProcessInsertTableNameClause(ConnectorSession session, String tableName)
    {
        return tableName + (isTableLockNeeded(session) ? " WITH (TABLOCK)" : "");
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (!tableHandle.isNamedRelation()) {
            return ImmutableMap.of();
        }
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            return getTableDataCompressionWithRetries(handle, tableHandle)
                    .map(dataCompression -> ImmutableMap.<String, Object>of(DATA_COMPRESSION, dataCompression))
                    .orElseGet(ImmutableMap::of);
        }
        catch (SQLException exception) {
            throw new TrinoException(JDBC_ERROR, exception);
        }
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the SQL Server driver
            // attempts to drain the connection by reading all the results.
            connection.abort(directExecutor());
        }
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, handle);
        try {
            connection.unwrap(SQLServerConnection.class)
                    .setUseBulkCopyForBatchInsert(isBulkCopyForWrite(session));
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    public static ColumnMapping varbinaryColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARBINARY,
                (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                varbinaryWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    public static SliceWriteFunction varbinaryWriteFunction()
    {
        return new SliceWriteFunction()
        {
            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setBytes(index, value.getBytes());
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setBytes(index, null);
            }
        };
    }

    private static ColumnMapping longVarcharColumnMapping(int columnSize)
    {
        // Disable pushdown to avoid "The data types ntext and nvarchar are incompatible in the equal to operator." error
        if (columnSize > VarcharType.MAX_LENGTH) {
            return ColumnMapping.sliceMapping(createUnboundedVarcharType(), varcharReadFunction(createUnboundedVarcharType()), nvarcharWriteFunction(), DISABLE_PUSHDOWN);
        }
        return ColumnMapping.sliceMapping(createVarcharType(columnSize), varcharReadFunction(createVarcharType(columnSize)), nvarcharWriteFunction(), DISABLE_PUSHDOWN);
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
                isCaseSensitive ? SQLSERVER_CHARACTER_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
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
                isCaseSensitive ? SQLSERVER_CHARACTER_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static SliceWriteFunction nvarcharWriteFunction()
    {
        return (statement, index, value) -> statement.setNString(index, value.toStringUtf8());
    }

    private static Optional<DataCompression> getTableDataCompression(Handle handle, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        return handle.createQuery("" +
                        "SELECT data_compression_desc FROM sys.partitions p " +
                        "INNER JOIN sys.tables t ON p.object_id = t.object_id " +
                        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                        "INNER JOIN sys.indexes i ON t.object_id = i.object_id " +
                        "WHERE s.name = :schema AND t.name = :table_name " +
                        "AND p.index_id = 0 " + // Heap
                        "AND i.type = 0 " + // Heap index type
                        "AND i.data_space_id NOT IN (SELECT data_space_id FROM sys.partition_schemes)")
                .bind("schema", remoteTableName.getSchemaName().orElse(null))
                .bind("table_name", remoteTableName.getTableName())
                .mapTo(String.class)
                .findOne()
                .flatMap(dataCompression -> Enums.getIfPresent(DataCompression.class, dataCompression).toJavaUtil());
    }

    private static Optional<DataCompression> getTableDataCompressionWithRetries(Handle handle, JdbcTableHandle table)
    {
        return retryOnDeadlock(() -> getTableDataCompression(handle, table), "error when getting table compression info for '%s'".formatted(table));
    }

    public static <T> T retryOnDeadlock(CheckedSupplier<T> supplier, String attemptLogMessage)
    {
        // DDL operations can take out locks against system tables causing queries against them to deadlock
        int maxAttemptCount = 3;
        RetryPolicy<T> retryPolicy = RetryPolicy.<T>builder()
                .withMaxAttempts(maxAttemptCount)
                .handleIf(throwable ->
                {
                    Throwable rootCause = Throwables.getRootCause(throwable);
                    return rootCause instanceof SQLServerException &&
                            ((SQLServerException) (rootCause)).getSQLServerError().getErrorNumber() == SQL_SERVER_DEADLOCK_ERROR_CODE;
                })
                .onFailedAttempt(event -> log.warn(event.getLastException(), "Attempt %d of %d: %s", event.getAttemptCount(), maxAttemptCount, attemptLogMessage))
                .build();

        return Failsafe
                .with(retryPolicy)
                .get(supplier);
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getTableObjectId(String catalog, String schema, String tableName)
        {
            return handle.createQuery("SELECT object_id(:table)")
                    .bind("table", format("%s.%s.%s", catalog, schema, tableName))
                    .mapTo(Long.class)
                    .one();
        }

        Long getRowCount(long tableObjectId)
        {
            return handle.createQuery("" +
                            "SELECT sum(rows) row_count " +
                            "FROM sys.partitions " +
                            "WHERE object_id = :object_id " +
                            "AND index_id IN (0, 1)") // 0 = heap, 1 = clustered index, 2 or greater = non-clustered index
                    .bind("object_id", tableObjectId)
                    .mapTo(Long.class)
                    .one();
        }

        List<String> getSingleColumnStatistics(long tableObjectId)
        {
            return handle.createQuery("" +
                            "SELECT s.name " +
                            "FROM sys.stats AS s " +
                            "JOIN sys.stats_columns AS sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id " +
                            "WHERE s.object_id = :object_id " +
                            "GROUP BY s.name " +
                            "HAVING count(*) = 1 " +
                            "ORDER BY s.name")
                    .bind("object_id", tableObjectId)
                    .mapTo(String.class)
                    .list();
        }

        String getSingleColumnStatisticsColumnName(long tableObjectId, String statisticsName)
        {
            return handle.createQuery("" +
                            "SELECT c.name " +
                            "FROM sys.stats AS s " +
                            "JOIN sys.stats_columns AS sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id " +
                            "JOIN sys.columns AS c ON sc.object_id = c.object_id AND c.column_id = sc.column_id " +
                            "WHERE s.object_id = :object_id " +
                            "AND s.name = :statistics_name")
                    .bind("object_id", tableObjectId)
                    .bind("statistics_name", statisticsName)
                    .mapTo(String.class)
                    .collect(toOptional()) // verify there is no more than 1 column name returned
                    .orElse(null);
        }
    }
}
