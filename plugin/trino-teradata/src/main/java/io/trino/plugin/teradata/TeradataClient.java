/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import com.google.inject.Inject;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.weakref.jmx.$internal.guava.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;

/**
 * TeradataClient is a JDBC client implementation for the Teradata database.
 * It extends BaseJdbcClient to provide Teradata-specific handling for
 * data types, case sensitivity, and SQL expression rewriting.
 * <p>
 * This client supports reading data from Teradata but does not support
 * schema creation or delete operations.
 * </p>
 * <p>
 * It includes custom mappings for Teradata-specific types such as
 * TIMESTAMP WITH TIME ZONE and TIME WITH TIME ZONE, as well as JSON.
 * </p>
 * <p>
 * The client also defines how case sensitivity is handled based on the Teradata
 * JDBC case sensitivity setting.
 * </p>
 *
 * <p>Unpublished work. Copyright 2025 by Teradata Corporation. All rights reserved.</p>
 */
public class TeradataClient
        extends BaseJdbcClient
{
    private final TeradataConfig.TeradataCaseSensitivity teradataJDBCCaseSensitivity;
    private ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    /**
     * Constructs a new TeradataClient instance.
     *
     * @param config base JDBC configuration
     * @param teradataConfig Teradata-specific configuration
     * @param connectionFactory factory to create JDBC connections
     * @param queryBuilder query builder for SQL queries
     * @param identifierMapping mapping for identifiers such as column names
     * @param remoteQueryModifier optional modifier for remote queries
     */
    @Inject
    public TeradataClient(BaseJdbcConfig config, TeradataConfig teradataConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.teradataJDBCCaseSensitivity = teradataConfig.getTeradataCaseSensitivity();
        buildExpressionRewriter();
        // TODO         this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
    }

    /**
     * Creates a ColumnMapping for Teradata TIME type with specified precision.
     *
     * @param precision fractional seconds precision for the TIME column
     * @return ColumnMapping instance for TIME type
     */
    public static ColumnMapping timeColumnMapping(int precision)
    {
        TimeType timeType = createTimeType(precision);
        return ColumnMapping.longMapping(timeType, timeReadFunction(timeType), timeWriteFunction(precision), DISABLE_PUSHDOWN);
    }

    /**
     * Returns a function to read TIME values from JDBC result set,
     * converting SQL Timestamp to Trino's internal representation.
     *
     * @param timeType Trino TimeType
     * @return LongReadFunction for TIME values
     */
    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        return (resultSet, columnIndex) -> {
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex);
            LocalTime localTime = sqlTimestamp.toLocalDateTime().toLocalTime();
            long nsOfDay = localTime.toNanoOfDay();
            long picosOfDay = nsOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        };
    }

    /**
     * Returns a function to write TIME values to JDBC PreparedStatement,
     * converting Trino internal representation to JDBC object.
     *
     * @param precision fractional seconds precision
     * @return LongWriteFunction for TIME values
     */
    public static LongWriteFunction timeWriteFunction(int precision)
    {
        return LongWriteFunction.of(Types.TIME, (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromTrinoTime(picosOfDay));
        });
    }

    /**
     * Creates a ColumnMapping for Teradata TIME WITH TIME ZONE type with specified precision.
     *
     * @param precision fractional seconds precision
     * @return ColumnMapping instance for TIME WITH TIME ZONE type
     */
    public static ColumnMapping timeWithTimeZoneColumnMapping(int precision)
    {
        return ColumnMapping.longMapping(createTimeWithTimeZoneType(precision), shortTimeWithTimeZoneReadFunction(), shortTimeWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    // TODO
    // public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    // public Optional<PreparedQuery> implementJoin(
    // public Optional<PreparedQuery> legacyImplementJoin(
    // public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    // public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    // public boolean isLimitGuaranteed(ConnectorSession session)
    // public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    // public boolean isTopNGuaranteed(ConnectorSession session)
    // public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments)

    /**
     * Reads TIME WITH TIME ZONE values from JDBC ResultSet.
     *
     * @return LongReadFunction for TIME WITH TIME ZONE values
     */
    private static LongReadFunction shortTimeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            LocalDateTime localDateTime = sqlTimestamp.toLocalDateTime();
            ZoneId zone = ZoneId.of(calendar.getTimeZone().getID());
            ZonedDateTime zdt = ZonedDateTime.of(localDateTime, zone);
            int offsetMinutes = zdt.getOffset().getTotalSeconds() / 60;
            long nanos = localDateTime.getLong(ChronoField.NANO_OF_DAY);
            return packTimeWithTimeZone(nanos, offsetMinutes);
        };
    }

    /**
     * Writes TIME WITH TIME ZONE values to JDBC PreparedStatement.
     *
     * @return LongWriteFunction for TIME WITH TIME ZONE values
     */
    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    /**
     * Creates a ColumnMapping for Teradata TIMESTAMP WITH TIME ZONE type with specified precision.
     *
     * @param precision fractional seconds precision
     * @return ColumnMapping instance for TIMESTAMP WITH TIME ZONE type
     */
    public static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(createTimestampWithTimeZoneType(precision), shortTimestampWithTimeZoneReadFunction(), shortTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(createTimestampWithTimeZoneType(precision), longTimestampWithTimeZoneReadFunction(), longTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    /**
     * Reads TIMESTAMP WITH TIME ZONE values with short precision from JDBC ResultSet.
     *
     * @return LongReadFunction for short precision TIMESTAMP WITH TIME ZONE values
     */
    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    // --- Static utility methods for Teradata time and timestamp types ---

    /**
     * Writes TIMESTAMP WITH TIME ZONE values with short precision to JDBC PreparedStatement.
     *
     * @return LongWriteFunction for short precision TIMESTAMP WITH TIME ZONE values
     */
    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    /**
     * Reads TIMESTAMP WITH TIME ZONE values with long precision from JDBC ResultSet.
     *
     * @return ObjectReadFunction for long precision TIMESTAMP WITH TIME ZONE values
     */
    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime dateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            OffsetDateTime offsetDateTime = dateTime.toOffsetDateTime();
            long picosOfSecond = offsetDateTime.getNano() * ((long) PICOSECONDS_PER_NANOSECOND);

            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(offsetDateTime.toEpochSecond(), picosOfSecond, getTimeZoneKey(offsetDateTime.toZonedDateTime().getZone().getId()));
        });
    }

    /**
     * Writes TIMESTAMP WITH TIME ZONE values with long precision to JDBC PreparedStatement.
     *
     * @return ObjectWriteFunction for long precision TIMESTAMP WITH TIME ZONE values
     */
    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMillis = value.getEpochMillis();
            long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
            ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant instant = Instant.ofEpochSecond(epochSeconds);
            statement.setObject(index, OffsetDateTime.ofInstant(instant, zoneId));
        });
    }

    /**
     * Teradata does not support creating schemas through this connector.
     *
     * @param session connector session
     * @param schemaName schema name to create
     * @throws TrinoException always thrown with NOT_SUPPORTED error code
     */
    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    /**
     * Delete operations are not supported by the Teradata connector.
     *
     * @param session connector session
     * @param handle table handle identifying the target table
     * @return empty optional indicating no deletion occurred
     * @throws TrinoException always thrown with NOT_SUPPORTED error code
     */
    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support delete operations");
    }

    /**
     * Builds the expression rewriter for translating connector expressions
     * into SQL fragments understood by Teradata.
     * Currently supports numeric equality expressions and quoted identifiers.
     */
    private void buildExpressionRewriter()
    {
        // TODO add additional rules with test cases (see sqlserver's)
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder().addStandardRules(this::quoted).map("$equal(left: numeric_type, right: numeric_type)").to("left = right").build();
    }

    /**
     * Converts a predicate expression to a parameterized JDBC expression,
     * suitable for pushdown to the Teradata database.
     *
     * @param session connector session
     * @param expression connector expression representing the predicate
     * @param assignments mapping of column names to handles
     * @return optional parameterized expression if conversion is possible
     */
    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return this.connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    /**
     * Returns a mapping of column names to their case sensitivity,
     * derived from the metadata of a query "SELECT * FROM schema.table WHERE 0=1".
     *
     * @param session connector session
     * @param connection JDBC connection to the Teradata database
     * @param schemaTableName schema and table name within the connector
     * @param remoteTableName the fully qualified remote table name
     * @return map of column name to case sensitivity (case sensitive or insensitive)
     */
    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        // try to use result set metadata from select * from table to populate the mapping
        try {
            HashMap<String, CaseSensitivity> caseMap = new HashMap<>();
            String sql = String.format("select * from %s.%s where 0=1", schemaTableName.getSchemaName(), schemaTableName.getTableName());
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

    /**
     * Determines the case sensitivity for a type based on Teradata configuration.
     *
     * @param typeHandleCaseSensitivity optional case sensitivity from type metadata
     * @return true if case sensitive, false otherwise
     */
    private boolean deriveCaseSensitivity(Optional<CaseSensitivity> typeHandleCaseSensitivity)
    {
        switch (teradataJDBCCaseSensitivity) {
            case NOT_CASE_SPECIFIC:
                return false;
            case CASE_SPECIFIC:
                return true;
            case AS_DEFINED:
            default:
                return typeHandleCaseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE;
        }
    }

    /**
     * Maps JDBC types and Teradata-specific types to Trino column mappings.
     * Handles standard types as well as Teradata-specific types like
     * TIMESTAMP WITH TIME ZONE and JSON.
     *
     * @param session connector session
     * @param connection JDBC connection
     * @param typeHandle JDBC type handle describing the column type
     * @return optional column mapping for the given type
     */
    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // this method should ultimately encompass all the expected teradata data types

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        // switch by names as some types overlap other types going by jdbc type alone
        String jdbcTypeName = typeHandle.jdbcTypeName().orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        switch (jdbcTypeName.toUpperCase()) {
            case "TIMESTAMP WITH TIME ZONE":
                // TODO review correctness
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "TIME WITH TIME ZONE":
                // TODO review correctness
                return Optional.of(timeWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "JSON":
                // TODO map to trino json value
                return mapToUnboundedVarchar(typeHandle);
        }

        // switch by jdbc type
        // TODO missing types interval, array, etc
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
                // also applies to teradata number type
                // this is roughly logic see used by sql server
                int precision = typeHandle.requiredColumnSize();
                int scale = typeHandle.requiredDecimalDigits();
                if (precision > Decimals.MAX_PRECISION) {
                    // this will trigger for number(*) as precision is 40
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
            case Types.CHAR:
                return Optional.of(charColumnMapping(createCharType(typeHandle.requiredColumnSize()), deriveCaseSensitivity(typeHandle.caseSensitivity())));
            case Types.VARCHAR:
                // see prior note on trino case sensitivity
                return Optional.of(varcharColumnMapping(createVarcharType(typeHandle.requiredColumnSize()), deriveCaseSensitivity(typeHandle.caseSensitivity())));
            case Types.BINARY:
            case Types.VARBINARY:
                // trino only has varbinary
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.requiredDecimalDigits()));
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(TimestampType.createTimestampType(typeHandle.requiredDecimalDigits())));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    /**
     * This connector is read-only and does not support writing to Teradata.
     * This method always throws a NOT_SUPPORTED exception.
     *
     * @param session connector session
     * @param type Trino type for the column
     * @return never returns normally
     * @throws TrinoException always thrown indicating unsupported operation
     */
    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        // connector is read-only
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
