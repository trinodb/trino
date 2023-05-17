/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugins.snowflake.jdbc.DatabaseSchemaName.parseDatabaseSchemaName;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.REAL;
import static java.sql.Types.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SnowflakeClient
        extends BaseJdbcClient
{
    public static final String IDENTIFIER_QUOTE = "\"";

    public static final String DATABASE_SEPARATOR = ".";

    public static final int SNOWFLAKE_MAX_TIMESTAMP_PRECISION = 9;
    public static final int SNOWFLAKE_MAX_LIST_EXPRESSIONS = 1000;
    private static final DateTimeFormatter SNOWFLAKE_DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    // TODO https://starburstdata.atlassian.net/browse/SEP-9994
    // TODO https://starburstdata.atlassian.net/browse/SEP-10002
    // below formatters use `y` for years. `u` has to be used eventually.
    public static final DateTimeFormatter SNOWFLAKE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    public static final DateTimeFormatter SNOWFLAKE_TIMESTAMP_READ_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSSX");
    private static final DateTimeFormatter SNOWFLAKE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSS");

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BooleanType.BOOLEAN, WriteMapping.booleanMapping("boolean", booleanWriteFunction()))
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .put(DoubleType.DOUBLE, WriteMapping.doubleMapping("double precision", doubleWriteFunction()))
            .put(RealType.REAL, WriteMapping.longMapping("real", realWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction()))
            .put(DateType.DATE, WriteMapping.longMapping("date", dateWriteFunctionUsingString()))
            .buildOrThrow();

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final Type jsonType;
    private final Type jsonPathType;
    private final boolean statisticsEnabled;
    private final boolean distributedConnector;
    private final boolean databasePrefixForSchemaEnabled;

    public SnowflakeClient(
            BaseJdbcConfig config,
            SnowflakeConfig snowflakeConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            boolean distributedConnector,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(IDENTIFIER_QUOTE, connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.jsonPathType = typeManager.getType(new TypeSignature("JsonPath"));
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.distributedConnector = distributedConnector;
        this.databasePrefixForSchemaEnabled = requireNonNull(snowflakeConfig, "snowflakeConfig is null").getDatabasePrefixForSchemaEnabled();
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL)))
                .add(new RewriteJsonConstant(jsonType))
                .add(new RewriteJsonPath(jsonPathType))
                .add(new RewriteJsonExtract(jsonType))
                .add(new RewriteJsonExtractScalar())
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .build();
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(SnowflakeClient::decimalTypeHandle))
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        if (!databasePrefixForSchemaEnabled) {
            return super.listSchemas(connection);
        }
        try (ResultSet resultSet = connection.getMetaData().getSchemas(null, null)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(format("%s%s%s", resultSet.getString("TABLE_CATALOG"), DATABASE_SEPARATOR, schemaName));
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        if (!databasePrefixForSchemaEnabled) {
            super.createSchema(session, connection, remoteSchemaName);
            return;
        }
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        execute(session, connection, format("CREATE SCHEMA %s%s%s", quoted(databaseSchema.getDatabaseName()), DATABASE_SEPARATOR, quoted(databaseSchema.getSchemaName())));
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        if (!databasePrefixForSchemaEnabled) {
            super.dropSchema(session, connection, remoteSchemaName);
            return;
        }
        DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteSchemaName);
        execute(session, format("DROP SCHEMA %s%s%s", quoted(databaseSchema.getDatabaseName()), DATABASE_SEPARATOR, quoted(databaseSchema.getSchemaName())));
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        RemoteTableName remappedRemoteTableName = remoteTableName;
        if (databasePrefixForSchemaEnabled) {
            DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(remoteTableName.getSchemaName().orElseThrow());
            remappedRemoteTableName = new RemoteTableName(
                    Optional.of(databaseSchema.getDatabaseName()),
                    Optional.of(databaseSchema.getSchemaName()),
                    remoteTableName.getTableName());
        }
        return super.createTableSql(remappedRemoteTableName, columns, tableMetadata);
    }

    @Override
    public void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String catalog = catalogName;
        String schema = schemaName;
        if (databasePrefixForSchemaEnabled) {
            DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(schemaName);
            catalog = databaseSchema.getDatabaseName();
            schema = databaseSchema.getSchemaName();
        }
        super.copyTableSchema(session, connection, catalog, schema, tableName, newTableName, columnNames);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        if (!databasePrefixForSchemaEnabled) {
            return super.getTables(connection, remoteSchemaName, remoteTableName);
        }
        Optional<DatabaseSchemaName> databaseSchema = remoteSchemaName.map(DatabaseSchemaName::parseDatabaseSchemaName);

        DatabaseMetaData metadata = connection.getMetaData();

        return metadata.getTables(
                databaseSchema.map(DatabaseSchemaName::getDatabaseName).orElse(null),
                escapeObjectNameForMetadataQuery(databaseSchema.map(DatabaseSchemaName::getSchemaName), metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        if (databasePrefixForSchemaEnabled) {
            return resultSet.getString("TABLE_CAT") + "." + resultSet.getString("TABLE_SCHEM");
        }
        return super.getTableSchemaName(resultSet);
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType)
    {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.NUMERIC,
                        Optional.of("NUMBER"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    @Override
    public PreparedQuery applyQueryTransformations(JdbcTableHandle tableHandle, PreparedQuery query)
    {
        return super.applyQueryTransformations(tableHandle, query);
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
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        // The data returned conforms to TopN requirements, but can be returned out of order
        return !distributedConnector;
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (typeHandle.getJdbcType() == BOOLEAN) {
            return Optional.of(booleanColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.TINYINT) {
            return Optional.of(tinyintColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.SMALLINT) {
            return Optional.of(smallintColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.INTEGER) {
            return Optional.of(integerColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.BIGINT) {
            return Optional.of(bigintColumnMapping());
        }

        if (typeHandle.getJdbcType() == REAL) {
            return Optional.of(realColumnMapping());
        }

        if (typeHandle.getJdbcType() == FLOAT || typeHandle.getJdbcType() == DOUBLE) {
            return Optional.of(doubleColumnMapping());
        }

        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getRequiredDecimalDigits();
            int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
            if (precision > Decimals.MAX_PRECISION) {
                return Optional.empty();
            }
            return Optional.of(updatePushdownCotroller(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY)));
        }

        if (typeName.equals("VARIANT")) {
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), variantReadFunction(), varcharWriteFunction(), FULL_PUSHDOWN));
        }

        if (typeName.equals("OBJECT") || typeName.equals("ARRAY")) {
            // TODO: better support for OBJECT (Presto MAP/ROW) and ARRAY (Presto ARRAY)
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), varcharReadFunction(createUnboundedVarcharType()), varcharWriteFunction(), DISABLE_PUSHDOWN));
        }

        if (typeHandle.getJdbcType() == DATE) {
            return Optional.of(ColumnMapping.longMapping(
                    DateType.DATE,
                    ResultSet::getLong,
                    dateWriteFunctionUsingString()));
        }

        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(updatePushdownCotroller(timeColumnMapping()));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP_WITH_TIMEZONE || typeName.equals("TIMESTAMPLTZ")) {
            return Optional.of(timestampWithTimezoneColumnMapping(getSupportedTimestampPrecision(typeHandle.getRequiredDecimalDigits())));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            return Optional.of(timestampColumnMapping(getSupportedTimestampPrecision(typeHandle.getRequiredDecimalDigits())));
        }

        if (typeHandle.getJdbcType() == CHAR) {
            return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), true));
        }

        if (typeHandle.getJdbcType() == VARCHAR) {
            // The max varchar size is different between distributed connector and JDBC connector
            if (distributedConnector) {
                return Optional.of(varcharColumnMapping(createVarcharType(min(typeHandle.getRequiredColumnSize(), HiveVarchar.MAX_VARCHAR_LENGTH)), true));
            }
            return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));
        }

        if (typeHandle.getJdbcType() == BINARY) {
            return Optional.of(varbinaryColumnMapping());
        }

        return Optional.empty();
    }

    private int getSupportedTimestampPrecision(int precision)
    {
        // Coerce the timestamp precision to 3 in case of distributed mode because the footer of Parquet files generated by Snowflake always have TIMESTAMP_MILLIS for timestamp type
        // https://community.snowflake.com/s/article/Nano-second-precision-lost-after-Parquet-file-Unload
        return distributedConnector ? 3 : precision;
    }

    private ColumnMapping updatePushdownCotroller(ColumnMapping mapping)
    {
        verify(mapping.getPredicatePushdownController() != DISABLE_PUSHDOWN);
        return new ColumnMapping(
                mapping.getType(),
                mapping.getReadFunction(),
                mapping.getWriteFunction(),
                FULL_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            // Snowflake CHAR is an alias for VARCHAR so we need to pad value with spaces
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction((CharType) type));
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

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            checkArgument(timestampType.getPrecision() <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");
            if (timestampType.isShort()) {
                return WriteMapping.longMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), timestampWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), longTimestampWriteFunction(timestampType.getPrecision()));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
            checkArgument(timestampWithTimeZoneType.getPrecision() <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");
            if (timestampWithTimeZoneType.isShort()) {
                return WriteMapping.longMapping(format("timestamp_tz(%d)", timestampWithTimeZoneType.getPrecision()), timestampWithTimezoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp_tz(%d)", timestampWithTimeZoneType.getPrecision()), longTimestampWithTimezoneWriteFunction());
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.createTable(session, tableMetadata, tableName);
    }

    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName, Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        JdbcOutputTableHandle outputTableHandle = super.createTable(session, tableMetadata, targetTableName, pageSinkIdColumn);
        if (databasePrefixForSchemaEnabled) {
            DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(outputTableHandle.getSchemaName());
            return new JdbcOutputTableHandle(
                    databaseSchema.getDatabaseName(),
                    databaseSchema.getSchemaName(),
                    outputTableHandle.getTableName(),
                    outputTableHandle.getColumnNames(),
                    outputTableHandle.getColumnTypes(),
                    outputTableHandle.getJdbcColumnTypes(),
                    outputTableHandle.getTemporaryTableName(),
                    outputTableHandle.getPageSinkIdColumnName());
        }
        return outputTableHandle;
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        JdbcOutputTableHandle outputTableHandle = super.beginInsertTable(session, tableHandle, columns);
        if (databasePrefixForSchemaEnabled) {
            DatabaseSchemaName databaseSchema = parseDatabaseSchemaName(outputTableHandle.getSchemaName());
            return new JdbcOutputTableHandle(
                    databaseSchema.getDatabaseName(),
                    databaseSchema.getSchemaName(),
                    outputTableHandle.getTableName(),
                    outputTableHandle.getColumnNames(),
                    outputTableHandle.getColumnTypes(),
                    outputTableHandle.getJdbcColumnTypes(),
                    outputTableHandle.getTemporaryTableName(),
                    outputTableHandle.getPageSinkIdColumnName());
        }
        return outputTableHandle;
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        checkColumnsForInvalidCharacters(ImmutableList.of(column));
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            JdbcNamedRelationHandle jdbcNamedRelationHandle = handle.asPlainTable();
            RemoteTableName remoteTableName = jdbcNamedRelationHandle.getRemoteTableName();
            super.addColumn(session, connection, remoteTableName, column);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    /**
     * This override is required for FTE mode as this method is being called from {@link BaseJdbcClient#beginInsertTable}
     * to create an additional sink ID column and it needs database prefix handling.
     */
    @Override
    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        if (databasePrefixForSchemaEnabled) {
            DatabaseSchemaName databaseSchemaName = parseDatabaseSchemaName(table.getSchemaName().orElseThrow());
            table = new RemoteTableName(
                    Optional.of(databaseSchemaName.getDatabaseName()),
                    Optional.of(databaseSchemaName.getSchemaName()),
                    table.getTableName());
        }
        super.addColumn(session, connection, table, column);
    }

    public PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            List<JdbcColumnHandle> columns,
            Optional<JdbcSplit> split)
            throws SQLException
    {
        return prepareQuery(
                session,
                connection,
                table,
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                split);
    }

    public static void checkColumnsForInvalidCharacters(List<ColumnMetadata> columns)
    {
        columns.forEach(columnMetadata -> {
            if (columnMetadata.getName().contains("\"")) {
                throw new TrinoException(NOT_SUPPORTED, "Snowflake columns cannot contain quotes: " + columnMetadata.getName());
            }
        });
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
    }

    @SuppressWarnings("UnnecessaryLambda")
    private static SliceReadFunction variantReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex).replaceAll("^\"|\"$", ""));
    }

    @SuppressWarnings("UnnecessaryLambda")
    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static LongWriteFunction dateWriteFunctionUsingString()
    {
        return (statement, index, day) -> statement.setString(index, SNOWFLAKE_DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static ColumnMapping timestampWithTimezoneColumnMapping(int precision)
    {
        checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");

        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampWithTimeZoneType(precision),
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return packDateTimeWithZone(
                                timestamp.toInstant().toEpochMilli(),
                                timestamp.getZone().getId());
                    },
                    timestampWithTimezoneWriteFunction(),
                    FULL_PUSHDOWN);
        }

        return ColumnMapping.objectMapping(
                createTimestampWithTimeZoneType(precision),
                longTimestampWithTimezoneReadFunction(),
                longTimestampWithTimezoneWriteFunction());
    }

    private static LongWriteFunction timestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(time.atZone(zone)));
        };
    }

    private static ColumnMapping timestampColumnMapping(int precision)
    {
        checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");

        if (precision <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampType(precision),
                    (resultSet, columnIndex) -> toTrinoTimestamp(createTimestampType(precision), toLocalDateTime(resultSet, columnIndex)),
                    timestampWriteFunction());
        }

        return ColumnMapping.objectMapping(
                createTimestampType(precision),
                longTimestampReadFunction(precision),
                longTimestampWriteFunction(precision));
    }

    private static ObjectReadFunction longTimestampReadFunction(int precision)
    {
        TimestampType timestampType = createTimestampType(precision);
        return ObjectReadFunction.of(LongTimestamp.class, (resultSet, columnIndex) ->
                toLongTrinoTimestamp(timestampType, toLocalDateTime(resultSet, columnIndex)));
    }

    private static ObjectWriteFunction longTimestampWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setString(index, SNOWFLAKE_TIMESTAMP_FORMATTER.format(fromLongTrinoTimestamp(value, precision))));
    }

    private static ObjectReadFunction longTimestampWithTimezoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            timestamp.toEpochSecond(),
                            (long) timestamp.getNano() * PICOSECONDS_PER_NANOSECOND,
                            TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
                });
    }

    private static ObjectWriteFunction longTimestampWithTimezoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochMillis = value.getEpochMillis();
                    long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
                    int nanoAdjustment = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
                    Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                    statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(ZonedDateTime.ofInstant(instant, zoneId)));
                });
    }

    private static LongWriteFunction timestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, fromTrinoTimestamp(value).toString());
    }

    private static ColumnMapping timeColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIME_MILLIS,
                (resultSet, columnIndex) -> toPrestoTime(resultSet.getTime(columnIndex)),
                timeWriteFunction());
    }

    private static LongWriteFunction timeWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, fromTrinoTime(value).toString());
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        String string = resultSet.getString(columnIndex);
        return LocalDateTime.parse(string, SNOWFLAKE_TIMESTAMP_READ_FORMATTER);
    }

    public static long toPrestoTime(Time sqlTime)
    {
        return PICOSECONDS_PER_MILLISECOND * sqlTime.getTime();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
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
            throwIfInvalidWarehouse(e);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            Long rowCount = handle.createQuery("" +
                            "SELECT (" + // Verify we do not ignore second result row, should there be any
                            "  SELECT ROW_COUNT " +
                            "  FROM information_schema.tables " +
                            "  WHERE table_catalog = :table_catalog " +
                            "  AND table_schema = :table_schema " +
                            "  AND table_name = :table_name " +
                            ")")
                    .bind("table_catalog", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_schema", remoteTableName.getSchemaName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOnly();

            if (rowCount == null) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private static void throwIfInvalidWarehouse(Throwable throwable)
    {
        Throwable rootCause = getRootCause(throwable);
        if (rootCause instanceof SQLException && ((SQLException) rootCause).getErrorCode() == 606) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, "Could not query Snowflake due to invalid warehouse configuration. " +
                    "Fix configuration or select an active Snowflake warehouse with 'warehouse' catalog session property.", throwable);
        }
    }
}
