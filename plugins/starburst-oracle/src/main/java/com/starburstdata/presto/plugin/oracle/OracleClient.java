/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleWriteFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Chars;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.getMaxSplitsPerScan;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.getNumberDefaultScale;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.getNumberRoundingMode;
import static com.starburstdata.presto.plugin.oracle.OracleSessionProperties.getParallelismType;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcPropertiesProvider.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class OracleClient
        extends BaseJdbcClient
{
    private static final int DEFAULT_ROW_FETCH_SIZE = 1000;

    // single UTF char may require at most 4 bytes of storage
    private static final int MAX_BYTES_PER_CHAR = 4;

    private static final int ORACLE_CHAR_MAX_BYTES = 2000;
    private static final int ORACLE_CHAR_MAX_CHARS = ORACLE_CHAR_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int ORACLE_NVARCHAR2_MAX_BYTES = 4000;
    private static final int ORACLE_VARCHAR2_MAX_CHARS = ORACLE_NVARCHAR2_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int ORACLE_MAX_LIST_EXPRESSIONS = 1000;
    private static final int PRECISION_OF_UNSPECIFIED_NUMBER = 127;

    public static final UnaryOperator<Domain> SIMPLIFY_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= ORACLE_MAX_LIST_EXPRESSIONS) {
            return domain;
        }
        return domain.simplify();
    };

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BOOLEAN, oracleBooleanWriteMapping())
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("binary_double", oracleDoubleWriteFunction()))
            .put(REAL, WriteMapping.longMapping("binary_float", oracleRealWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("blob", varbinaryWriteFunction()))
            .put(DATE, WriteMapping.longMapping("date", oracleDateWriteFunction()))
            .put(TIMESTAMP_WITH_TIME_ZONE, WriteMapping.longMapping("timestamp(3) with time zone", oracleTimestampWithTimezoneWriteFunction()))
            .build();

    private final boolean synonymsEnabled;
    private final OracleSplitManager splitManager;
    private final TableStatisticsClient tableStatisticsClient;

    @Inject
    public OracleClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            OracleConfig oracleConfig,
            OracleSplitManager oracleSplitManager,
            ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
        synonymsEnabled = oracleConfig.isSynonymsEnabled();
        splitManager = oracleSplitManager;
        tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return splitManager.getSplitSource(
                JdbcIdentity.from(session),
                tableHandle,
                getParallelismType(session),
                getMaxSplitsPerScan(session));
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return new OracleQueryBuilder(identifierQuote, (OracleSplit) split).buildSql(
                this,
                session,
                connection,
                table.getCatalogName(),
                table.getSchemaName(),
                table.getTableName(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    protected String generateTemporaryTableName()
    {
        // Oracle before 12.2 doesn't allow identifiers over 30 characters
        String id = super.generateTemporaryTableName();
        return id.substring(0, min(30, id.length()));
    }

    @Override
    protected String quoted(String name)
    {
        if (name.contains("\"")) {
            // ORA-03001: unimplemented feature
            throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, "Oracle does not support escaping '\"' in identifiers");
        }
        return identifierQuote + name + identifierQuote;
    }

    @Override
    public OracleConnection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        OracleConnection connection = (OracleConnection) super.getConnection(identity, split);
        try {
            connection.setDefaultRowPrefetch(DEFAULT_ROW_FETCH_SIZE);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName().toUpperCase(ENGLISH))) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported");
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
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

    private String[] getTableTypes()
    {
        if (synonymsEnabled) {
            return new String[] {"TABLE", "VIEW", "SYNONYM"};
        }
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (!synonymsEnabled) {
            return super.getColumns(session, tableHandle);
        }
        // MAJOR HACK ALERT!!!
        // We had to introduce the hack because of bug in Oracle JDBC client where
        // BaseJdbcClient#getColumns is not working when openProxySession is used and setIncludeSynonym(true) are used.
        // Below we are forcing to use oracle.jdbc.driver.OracleDatabaseMetaData.getColumnsWithWildcardsPlsql,
        // this method was used when setIncludeSynonym(false) is set, then openProxySession is also working as expected
        // Forcing is done by using wildcard '%' at the end of table name. And so we have to filter rows with columns from other tables.
        // Whenever you change this method make sure TestOracleIntegrationSmokeTest.testGetColumns covers your changes.
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData(), "%")) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    if (!resultSet.getString("TABLE_NAME").equals(tableHandle.getTableName())) {
                        continue;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType()));
                    }
                }
                if (columns.isEmpty()) {
                    // Table has no supported columns, but such table is not supported in Presto
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata, String tableNameSuffix)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse("") + tableNameSuffix,
                null);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle type)
    {
        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(type);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }
        int columnSize = type.getColumnSize();
        // OracleType enum would be more appropriate here, but it doesn't have a
        // value that corresponds to type.getJdbcType() for Oracle's RAW type.
        switch (type.getJdbcType()) {
            case OracleTypes.BINARY_FLOAT:
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        oracleRealWriteFunction(),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));

            case OracleTypes.BINARY_DOUBLE:
            case OracleTypes.FLOAT:
                return Optional.of(ColumnMapping.doubleMapping(
                        DOUBLE,
                        ResultSet::getDouble,
                        oracleDoubleWriteFunction(),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));

            case OracleTypes.NUMBER:
                int decimalDigits = type.getDecimalDigits();
                // Map negative scale to decimal(p+s, 0).
                int precision = columnSize + max(-decimalDigits, 0);
                int scale = max(decimalDigits, 0);
                Optional<Integer> numberDefaultScale = getNumberDefaultScale(session);
                RoundingMode roundingMode = getNumberRoundingMode(session);
                if (precision < scale) {
                    if (roundingMode == RoundingMode.UNNECESSARY) {
                        break;
                    }
                    scale = min(Decimals.MAX_PRECISION, scale);
                    precision = scale;
                }
                else if (numberDefaultScale.isPresent() && precision == PRECISION_OF_UNSPECIFIED_NUMBER) {
                    precision = Decimals.MAX_PRECISION;
                    scale = numberDefaultScale.get();
                }
                else if (precision > Decimals.MAX_PRECISION || columnSize <= 0) {
                    break;
                }
                DecimalType decimalType = createDecimalType(precision, scale);
                int finalScale = scale;
                // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
                if (decimalType.isShort()) {
                    return Optional.of(ColumnMapping.longMapping(
                            decimalType,
                            (resultSet, columnIndex) -> encodeShortScaledValue(resultSet.getBigDecimal(columnIndex), finalScale, roundingMode),
                            shortDecimalWriteFunction(decimalType),
                            SIMPLIFY_UNSUPPORTED_PUSHDOWN));
                }
                return Optional.of(ColumnMapping.sliceMapping(
                        decimalType,
                        (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex), finalScale, roundingMode),
                        longDecimalWriteFunction(decimalType),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));

            case OracleTypes.CHAR:
            case OracleTypes.NCHAR:
                CharType charType = createCharType(columnSize);
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(),
                        oracleCharWriteFunction(charType),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));

            case OracleTypes.VARCHAR:
            case OracleTypes.NVARCHAR:
                return Optional.of(ColumnMapping.sliceMapping(
                        createVarcharType(columnSize),
                        (varcharResultSet, varcharColumnIndex) -> utf8Slice(varcharResultSet.getString(varcharColumnIndex)),
                        varcharWriteFunction(),
                        SIMPLIFY_UNSUPPORTED_PUSHDOWN));

            case OracleTypes.CLOB:
            case OracleTypes.NCLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));

            case OracleTypes.VARBINARY: // Oracle's RAW(n)
            case OracleTypes.BLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        VARBINARY,
                        (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                        varbinaryWriteFunction(),
                        DISABLE_PUSHDOWN));

            // This mapping covers both DATE and TIMESTAMP, as Oracle's DATE has second precision.
            case OracleTypes.TIMESTAMP:
                return Optional.of(oracleTimestampColumnMapping(session));
            case OracleTypes.TIMESTAMPTZ:
                return Optional.of(oracleTimestampWithTimeZoneColumnMapping());
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return Optional.of(ColumnMapping.sliceMapping(
                    VARCHAR,
                    (varcharResultSet, varcharColumnIndex) -> utf8Slice(varcharResultSet.getString(varcharColumnIndex)),
                    (statement, index, value) -> {
                        // TODO this should be handled during planning phase
                        throw new PrestoException(
                                NOT_SUPPORTED,
                                "Underlying unsupported type that is mapped to VARCHAR is not supported for INSERT: " + type);
                    },
                    DISABLE_PUSHDOWN));
        }
        return Optional.empty();
    }

    private static WriteMapping oracleBooleanWriteMapping()
    {
        return WriteMapping.booleanMapping("number(1)", (statement, index, value) -> {
            statement.setInt(index, value ? 1 : 0);
        });
    }

    private SliceWriteFunction oracleCharWriteFunction(CharType charType)
    {
        return (statement, index, value) -> {
            statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
        };
    }

    public static ColumnMapping oracleTimestampColumnMapping(ConnectorSession session)
    {
        return ColumnMapping.longMapping(
                TIMESTAMP,
                (resultSet, columnIndex) -> {
                    LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
                    if (session.isLegacyTimestamp()) {
                        return timestamp.atZone(ZoneId.of(session.getTimeZoneKey().getId())).toInstant().toEpochMilli();
                    }
                    return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
                },
                oracleTimestampWriteFunction(session),
                SIMPLIFY_UNSUPPORTED_PUSHDOWN);
    }

    public static ColumnMapping oracleTimestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = resultSet.getObject(columnIndex, ZonedDateTime.class);
                    return packDateTimeWithZone(
                            timestamp.toInstant().toEpochMilli(),
                            timestamp.getZone().getId());
                },
                oracleTimestampWithTimezoneWriteFunction(),
                SIMPLIFY_UNSUPPORTED_PUSHDOWN);
    }

    public static LongWriteFunction oracleTimestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setObject(index, time.atZone(zone));
        };
    }

    public static LongWriteFunction oracleRealWriteFunction()
    {
        return (statement, index, value) -> ((OraclePreparedStatement) statement).setBinaryFloat(index, intBitsToFloat(toIntExact(value)));
    }

    public static DoubleWriteFunction oracleDoubleWriteFunction()
    {
        return ((statement, index, value) -> ((OraclePreparedStatement) statement).setBinaryDouble(index, value));
    }

    public static LongWriteFunction oracleDateWriteFunction()
    {
        return (statement, index, value) -> {
            long utcMillis = DAYS.toMillis(value);
            ZonedDateTime date = Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC);
            // because of how JDBC works with dates we need to use the ZonedDataTime object and not a LocalDateTime
            statement.setObject(index, date);
        };
    }

    public static LongWriteFunction oracleTimestampWriteFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return (statement, index, utcMillis) -> {
                long dateTimeAsUtcMillis = Instant.ofEpochMilli(utcMillis)
                        .atZone(ZoneId.of(session.getTimeZoneKey().getId()))
                        .withZoneSameLocal(ZoneOffset.UTC)
                        .toInstant().toEpochMilli();
                statement.setObject(index, new oracle.sql.TIMESTAMP(new Timestamp(dateTimeAsUtcMillis), Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
            };
        }
        return (statement, index, utcMillis) -> {
            statement.setObject(index, new oracle.sql.TIMESTAMP(new Timestamp(utcMillis), Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
        };
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (isVarcharType(type)) {
            String dataType;
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() ||
                    varcharType.getBoundedLength() > ORACLE_VARCHAR2_MAX_CHARS) {
                dataType = "nclob";
            }
            else {
                dataType = "varchar2(" + varcharType.getBoundedLength() + " CHAR)";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (isCharType(type)) {
            String dataType;
            if (((CharType) type).getLength() > ORACLE_CHAR_MAX_CHARS) {
                dataType = "nclob";
            }
            else {
                dataType = "char(" + ((CharType) type).getLength() + " CHAR)";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }
        if (type instanceof DecimalType) {
            String dataType = format("number(%s, %s)",
                    ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
            if (((DecimalType) type).isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction((DecimalType) type));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction((DecimalType) type));
        }
        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("timestamp(3)", oracleTimestampWriteFunction(session));
        }
        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new PrestoException(NOT_SUPPORTED,
                "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT * FROM (%s) WHERE ROWNUM <= %s", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return OracleSessionProperties.getParallelismType(session) == OracleParallelismType.NO_PARALLELISM;
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableStatisticsClient.getTableStatistics(session, handle, tupleDomain);
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table.getSchemaName(), table.getTableName());
            if (rowCount == null) {
                return Optional.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return Optional.of(tableStatistics.build());
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(table.getSchemaName(), table.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsCount()
                                .map(nullsCount -> Estimate.of(1.0 * nullsCount / rowCount))
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesCount()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                /*
                                 * ALL_TAB_COLUMNS.AVG_COL_LEN is hard to interpret precisely:
                                 * - it can be `0` for all-null column
                                 * - it can be `len+1` for varchar column filled with constant of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `len/2+1` for varchar column half-filled with constant (or random) of length `len`, as if each row contained a is-null byte or length
                                 * - it can be `2` for varchar column with single non-null value of length 10, as if ... (?)
                                 * - it looks storage size does not directly depend on `IS NULL` column attribute
                                 *
                                 * Since the interpretation of the value is not obvious, we do not deduce is-null bytes. They will be accounted for second time in
                                 * `PlanNodeStatsEstimate.getOutputSizeForSymbol`, but this is the safer thing to do.
                                 */
                                .map(averageColumnLength -> Estimate.of(1.0 * averageColumnLength * rowCount))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return Optional.of(tableStatistics.build());
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getRowCount(String schema, String tableName)
        {
            return handle.createQuery("SELECT NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER = :schema AND TABLE_NAME = :table_name and PARTITION_NAME IS NULL")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findFirst()
                    .orElse(null);
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            // [PRESTO-3425] we are not using ALL_TAB_COL_STATISTICS, here because we observed queries which took multiple minutes when obtaining statistics for partitioned tables.
            //               It adds slight risk, because the statistics-related columns in ALL_TAB_COLUMNS are marked as deprecated and present only for backward
            //               compatibility with Oracle 7 (see: https://docs.oracle.com/cd/B14117_01/server.101/b10755/statviews_1180.htm)
            return handle.createQuery("SELECT COLUMN_NAME, NUM_NULLS, NUM_DISTINCT, AVG_COL_LEN FROM ALL_TAB_COLUMNS WHERE OWNER = :schema AND TABLE_NAME = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("COLUMN_NAME"), "COLUMN_NAME is null"),
                            Optional.ofNullable(rs.getObject("NUM_NULLS", Long.class)),
                            Optional.ofNullable(rs.getObject("NUM_DISTINCT", Long.class)),
                            Optional.ofNullable(rs.getObject("AVG_COL_LEN", Long.class))))
                    .list();
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Long> nullsCount;
        private final Optional<Long> distinctValuesCount;
        private final Optional<Long> averageColumnLength;

        ColumnStatisticsResult(String columnName, Optional<Long> nullsCount, Optional<Long> distinctValuesCount, Optional<Long> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsCount = nullsCount;
            this.distinctValuesCount = distinctValuesCount;
            this.averageColumnLength = averageColumnLength;
        }

        String getColumnName()
        {
            return columnName;
        }

        Optional<Long> getNullsCount()
        {
            return nullsCount;
        }

        Optional<Long> getDistinctValuesCount()
        {
            return distinctValuesCount;
        }

        Optional<Long> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }

    private static class OracleQueryBuilder
            extends QueryBuilder
    {
        private final OracleSplit split;

        public OracleQueryBuilder(String identifierQuote, OracleSplit split)
        {
            super(identifierQuote);
            this.split = requireNonNull(split, "split is null");
        }

        @Override
        protected String getRelation(String catalog, String schema, String table)
        {
            String tableName = super.getRelation(catalog, schema, table);
            return split.getPartitionNames()
                    .map(batch -> batch.stream()
                            .map(partitionName -> format("SELECT * FROM %s PARTITION (%s)", tableName, partitionName))
                            .collect(joining(" UNION ALL ", "(", ")"))) // wrap subquery in parentheses
                    .orElse(tableName);
        }
    }
}
