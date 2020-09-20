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
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleWriteFunction;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.PredicatePushdownController;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Chars;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
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
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.oracle.OracleSessionProperties.getNumberDefaultScale;
import static io.prestosql.plugin.oracle.OracleSessionProperties.getNumberRoundingMode;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
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
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.epochMicrosToMillisWithRounding;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public class OracleClient
        extends BaseJdbcClient
{
    private static final int MAX_BYTES_PER_CHAR = 4;

    private static final int ORACLE_VARCHAR2_MAX_BYTES = 4000;
    private static final int ORACLE_VARCHAR2_MAX_CHARS = ORACLE_VARCHAR2_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int ORACLE_CHAR_MAX_BYTES = 2000;
    private static final int ORACLE_CHAR_MAX_CHARS = ORACLE_CHAR_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int PRECISION_OF_UNSPECIFIED_NUMBER = 127;

    private static final int ORACLE_MAX_LIST_EXPRESSIONS = 1000;

    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("ctxsys")
            .add("flows_files")
            .add("mdsys")
            .add("outln")
            .add("sys")
            .add("system")
            .add("xdb")
            .add("xs$null")
            .build();

    private final boolean synonymsEnabled;

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
            .put(TIMESTAMP_TZ_MILLIS, WriteMapping.longMapping("timestamp(3) with time zone", oracleTimestampWithTimezoneWriteFunction()))
            .build();

    @Inject
    public OracleClient(
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);

        requireNonNull(oracleConfig, "oracle config is null");
        this.synonymsEnabled = oracleConfig.isSynonymsEnabled();
    }

    private String[] getTableTypes()
    {
        if (synonymsEnabled) {
            return new String[] {"TABLE", "VIEW", "SYNONYM"};
        }
        return new String[] {"TABLE", "VIEW"};
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

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INTERNAL_SCHEMAS.contains(schemaName.toLowerCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }

        String newTableName = newTable.getTableName().toUpperCase(ENGLISH);
        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTableName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        // ORA-02420: missing schema authorization clause
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();

        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(typeHandle);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.SMALLINT:
                return Optional.of(ColumnMapping.longMapping(
                        SMALLINT,
                        ResultSet::getShort,
                        smallintWriteFunction(),
                        OracleClient::fullPushdownIfSupported));
            case OracleTypes.BINARY_FLOAT:
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        oracleRealWriteFunction(),
                        OracleClient::fullPushdownIfSupported));

            case OracleTypes.BINARY_DOUBLE:
            case OracleTypes.FLOAT:
                return Optional.of(ColumnMapping.doubleMapping(
                        DOUBLE,
                        ResultSet::getDouble,
                        oracleDoubleWriteFunction(),
                        OracleClient::fullPushdownIfSupported));
            case OracleTypes.NUMBER:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
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
                            OracleClient::fullPushdownIfSupported));
                }
                return Optional.of(ColumnMapping.sliceMapping(
                        decimalType,
                        (resultSet, columnIndex) -> encodeScaledValue(resultSet.getBigDecimal(columnIndex), finalScale, roundingMode),
                        longDecimalWriteFunction(decimalType),
                        OracleClient::fullPushdownIfSupported));

            case OracleTypes.CHAR:
            case OracleTypes.NCHAR:
                CharType charType = createCharType(columnSize);
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(charType),
                        oracleCharWriteFunction(charType),
                        OracleClient::fullPushdownIfSupported));

            case OracleTypes.VARCHAR:
            case OracleTypes.NVARCHAR:
                return Optional.of(ColumnMapping.sliceMapping(
                        createVarcharType(columnSize),
                        (varcharResultSet, varcharColumnIndex) -> utf8Slice(varcharResultSet.getString(varcharColumnIndex)),
                        varcharWriteFunction(),
                        OracleClient::fullPushdownIfSupported));

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
                return Optional.of(oracleTimestampColumnMapping());
            case OracleTypes.TIMESTAMPTZ:
                return Optional.of(oracleTimestampWithTimeZoneColumnMapping());
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static PredicatePushdownController.DomainPushdownResult fullPushdownIfSupported(Domain domain)
    {
        if (domain.getValues().getRanges().getRangeCount() > ORACLE_MAX_LIST_EXPRESSIONS) {
            // pushdown simplified domain
            Domain pushedDown = domain.simplify();
            return new PredicatePushdownController.DomainPushdownResult(pushedDown, domain);
        }
        else {
            // full pushdown
            return new PredicatePushdownController.DomainPushdownResult(domain, Domain.all(domain.getType()));
        }
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

    public static LongWriteFunction oracleTimestampWriteFunction()
    {
        return (statement, index, utcMillis) -> {
            statement.setObject(index, new oracle.sql.TIMESTAMP(new Timestamp(epochMicrosToMillisWithRounding(utcMillis)), Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
        };
    }

    public static ColumnMapping oracleTimestampColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_MILLIS,
                (resultSet, columnIndex) -> {
                    LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
                    return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
                },
                oracleTimestampWriteFunction(),
                OracleClient::fullPushdownIfSupported);
    }

    public static ColumnMapping oracleTimestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_TZ_MILLIS,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = resultSet.getObject(columnIndex, ZonedDateTime.class);
                    return packDateTimeWithZone(
                            timestamp.toInstant().toEpochMilli(),
                            timestamp.getZone().getId());
                },
                oracleTimestampWithTimezoneWriteFunction(),
                OracleClient::fullPushdownIfSupported);
    }

    public static LongWriteFunction oracleTimestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setObject(index, time.atZone(zone));
        };
    }

    private static WriteMapping oracleBooleanWriteMapping()
    {
        return WriteMapping.booleanMapping("number(1)", (statement, index, value) -> {
            statement.setInt(index, value ? 1 : 0);
        });
    }

    public static LongWriteFunction oracleRealWriteFunction()
    {
        return (statement, index, value) -> ((OraclePreparedStatement) statement).setBinaryFloat(index, intBitsToFloat(toIntExact(value)));
    }

    public static DoubleWriteFunction oracleDoubleWriteFunction()
    {
        return ((statement, index, value) -> ((OraclePreparedStatement) statement).setBinaryDouble(index, value));
    }

    private SliceWriteFunction oracleCharWriteFunction(CharType charType)
    {
        return (statement, index, value) -> {
            statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
        };
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            String dataType;
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > ORACLE_VARCHAR2_MAX_CHARS) {
                dataType = "nclob";
            }
            else {
                dataType = "varchar2(" + varcharType.getBoundedLength() + " CHAR)";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType) {
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
            String dataType = format("number(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
            if (((DecimalType) type).isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction((DecimalType) type));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction((DecimalType) type));
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return WriteMapping.longMapping("timestamp(3)", oracleTimestampWriteFunction());
        }
        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
