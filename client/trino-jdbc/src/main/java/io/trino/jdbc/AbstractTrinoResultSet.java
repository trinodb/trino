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
package io.trino.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.IntervalDayTime;
import io.trino.client.IntervalYearMonth;
import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.jdbc.ColumnInfo.Nullable;
import io.trino.jdbc.TypeConversions.NoConversionRegisteredException;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.jdbc.ColumnInfo.setTypeInfo;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeConstants.SECONDS_PER_DAY;

abstract class AbstractTrinoResultSet
        implements ResultSet
{
    private static final Pattern DATETIME_PATTERN = Pattern.compile("" +
            "(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
            "(?: (?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?" +
            "\\s*(?<timezone>.+)?");

    private static final Pattern TIME_PATTERN = Pattern.compile("(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?");
    private static final Pattern TIME_WITH_TIME_ZONE_PATTERN = Pattern.compile("" +
            "(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?" +
            "[ ]?(?<sign>[+-])(?<offsetHour>\\d\\d):(?<offsetMinute>\\d\\d)");

    private static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L
    };

    private static final int MAX_DATETIME_PRECISION = 12;

    private static final int MILLISECONDS_PER_SECOND = 1000;
    private static final int MILLISECONDS_PER_MINUTE = 60 * MILLISECONDS_PER_SECOND;
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000;
    private static final int PICOSECONDS_PER_NANOSECOND = 1_000;

    static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date();
    static final DateTimeFormatter TIME_FORMATTER = DateTimeFormat.forPattern("HH:mm:ss.SSS");
    static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // Before 1900, Java Time and Joda Time are not consistent with java.sql.Date and java.util.Calendar
    // Since January 1, 1900 UTC is still December 31, 1899 in other zones, we are adding a 1 year margin.
    private static final long START_OF_MODERN_ERA_SECONDS = java.time.LocalDate.of(1901, 1, 1).toEpochDay() * SECONDS_PER_DAY;

    @VisibleForTesting
    static final Map<String, Class<?>> DEFAULT_OBJECT_REPRESENTATION = ImmutableMap.<String, Class<?>>builder()
            .put("decimal", BigDecimal.class)
            .put("date", java.sql.Date.class)
            .put("time", java.sql.Time.class)
            .put("time with time zone", java.sql.Time.class)
            .put("timestamp", java.sql.Timestamp.class)
            .put("timestamp with time zone", java.sql.Timestamp.class)
            .put("interval year to month", TrinoIntervalYearMonth.class)
            .put("interval day to second", TrinoIntervalDayTime.class)
            .put("map", Map.class)
            .put("row", Row.class)
            .buildOrThrow();

    @VisibleForTesting
    static final TypeConversions TYPE_CONVERSIONS =
            TypeConversions.builder()
                    .add("decimal", String.class, BigDecimal.class, AbstractTrinoResultSet::parseBigDecimal)
                    .add("varbinary", byte[].class, String.class, value -> "0x" + BaseEncoding.base16().encode(value))
                    .add("date", String.class, Date.class, string -> {
                        try {
                            return parseDate(string, DateTimeZone.forID(ZoneId.systemDefault().getId()));
                        }
                        // TODO (https://github.com/trinodb/trino/issues/6242) this should never fail
                        catch (IllegalArgumentException e) {
                            throw new SQLException("Expected value to be a date but is: " + string, e);
                        }
                    })
                    .add("time", String.class, Time.class, string -> parseTime(string, ZoneId.systemDefault()))
                    .add("time with time zone", String.class, Time.class, AbstractTrinoResultSet::parseTimeWithTimeZone)
                    .add("timestamp", String.class, Timestamp.class, string -> parseTimestampAsSqlTimestamp(string, ZoneId.systemDefault()))
                    .add("timestamp with time zone", String.class, Timestamp.class, AbstractTrinoResultSet::parseTimestampWithTimeZoneAsSqlTimestamp)
                    .add("timestamp with time zone", String.class, ZonedDateTime.class, AbstractTrinoResultSet::parseTimestampWithTimeZone)
                    .add("interval year to month", String.class, TrinoIntervalYearMonth.class, AbstractTrinoResultSet::parseIntervalYearMonth)
                    .add("interval day to second", String.class, TrinoIntervalDayTime.class, AbstractTrinoResultSet::parseIntervalDayTime)
                    .add("array", List.class, List.class, (type, list) -> (List<?>) convertFromClientRepresentation(type, list))
                    .add("map", Map.class, Map.class, (type, map) -> (Map<?, ?>) convertFromClientRepresentation(type, map))
                    .add("row", io.trino.client.Row.class, Row.class, (type, clientRow) -> (Row) convertFromClientRepresentation(type, clientRow))
                    .add("row", io.trino.client.Row.class, Map.class, (type, clientRow) -> {
                        Row row = (Row) convertFromClientRepresentation(type, clientRow);
                        Map<String, Object> result = new HashMap<>();
                        for (RowField field : row.getFields()) {
                            String name = field.getName()
                                    .orElseGet(() -> "field" + field.getOrdinal());
                            if (result.containsKey(name)) {
                                throw new SQLException("Duplicate field name: " + name);
                            }
                            result.put(name, field.getValue());
                        }
                        return result;
                    })
                    .build();

    private final DateTimeZone resultTimeZone;
    protected final Iterator<List<Object>> results;
    private final Map<String, Integer> fieldMap;
    private final List<ColumnInfo> columnInfoList;
    private final ResultSetMetaData resultSetMetaData;
    private final AtomicReference<List<Object>> row = new AtomicReference<>();
    private final AtomicLong currentRowNumber = new AtomicLong(); // Index into 'rows' of our current row (1-based)
    private final AtomicBoolean wasNull = new AtomicBoolean();
    protected final AtomicBoolean closed = new AtomicBoolean();
    private final Optional<Statement> statement;

    AbstractTrinoResultSet(Optional<Statement> statement, List<Column> columns, Iterator<List<Object>> results)
    {
        this.statement = requireNonNull(statement, "statement is null");
        this.resultTimeZone = DateTimeZone.forID(ZoneId.systemDefault().getId());

        requireNonNull(columns, "columns is null");
        this.fieldMap = getFieldMap(columns);
        this.columnInfoList = getColumnInfo(columns);
        this.resultSetMetaData = new TrinoResultSetMetaData(columnInfoList);

        this.results = requireNonNull(results, "results is null");
    }

    @Override
    public boolean next()
            throws SQLException
    {
        checkOpen();
        try {
            if (!results.hasNext()) {
                row.set(null);
                currentRowNumber.set(0);
                return false;
            }
            row.set(results.next());
            currentRowNumber.incrementAndGet();
            return true;
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new SQLException("Error fetching results", e);
        }
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return wasNull.get();
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }
        ClientTypeSignature columnTypeSignature = columnInfo(columnIndex).getColumnTypeSignature();
        if (TYPE_CONVERSIONS.hasConversion(columnTypeSignature.getRawType(), String.class)) {
            return TYPE_CONVERSIONS.convert(columnTypeSignature, value, String.class);
        }
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        return (value != null) ? (Boolean) value : false;
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).byteValue();
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).shortValue();
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).intValue();
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).longValue();
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).floatValue();
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        return toNumber(column(columnIndex)).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        BigDecimal bigDecimal = getBigDecimal(columnIndex);
        if (bigDecimal != null) {
            bigDecimal = bigDecimal.setScale(scale, ROUND_HALF_UP);
        }
        return bigDecimal;
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        return (byte[]) column(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        return getDate(columnIndex, resultTimeZone);
    }

    private Date getDate(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        try {
            return parseDate(String.valueOf(value), localTimeZone);
        }
        catch (IllegalArgumentException e) {
            throw new SQLException("Expected value to be a date but is: " + value, e);
        }
    }

    private static Date parseDate(String value, DateTimeZone localTimeZone)
    {
        long millis = DATE_FORMATTER.withZone(localTimeZone).parseMillis(String.valueOf(value));
        if (millis >= START_OF_MODERN_ERA_SECONDS * MILLISECONDS_PER_SECOND) {
            return new Date(millis);
        }

        // The chronology used by default by Joda is not historically accurate for dates
        // preceding the introduction of the Gregorian calendar and is not consistent with
        // java.sql.Date (the same millisecond value represents a different year/month/day)
        // before the 20th century. For such dates we are falling back to using the more
        // expensive GregorianCalendar; note that Joda also has a chronology that works for
        // older dates, but it uses a slightly different algorithm and yields results that
        // are not compatible with java.sql.Date.
        LocalDate localDate = DATE_FORMATTER.parseLocalDate(String.valueOf(value));
        Calendar calendar = new GregorianCalendar(localDate.getYear(), localDate.getMonthOfYear() - 1, localDate.getDayOfMonth());
        calendar.setTimeZone(TimeZone.getTimeZone(ZoneId.of(localTimeZone.getID())));

        return new Date(calendar.getTimeInMillis());
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        return getTime(columnIndex, resultTimeZone);
    }

    private Time getTime(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        ColumnInfo columnInfo = columnInfo(columnIndex);
        if (columnInfo.getColumnTypeSignature().getRawType().equalsIgnoreCase("time")) {
            try {
                return parseTime((String) value, ZoneId.of(localTimeZone.getID()));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid time from server: " + value, e);
            }
        }

        if (columnInfo.getColumnTypeSignature().getRawType().equalsIgnoreCase("time with time zone")) {
            try {
                return parseTimeWithTimeZone((String) value);
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid time from server: " + value, e);
            }
        }

        throw new IllegalArgumentException("Expected column to be a time type but is " + columnInfo.getColumnTypeName());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        return getTimestamp(columnIndex, resultTimeZone);
    }

    private Timestamp getTimestamp(int columnIndex, DateTimeZone localTimeZone)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        ColumnInfo columnInfo = columnInfo(columnIndex);
        if (columnInfo.getColumnTypeSignature().getRawType().equalsIgnoreCase("timestamp")) {
            try {
                return parseTimestampAsSqlTimestamp((String) value, ZoneId.of(localTimeZone.getID()));
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid timestamp from server: " + value, e);
            }
        }

        if (columnInfo.getColumnTypeSignature().getRawType().equalsIgnoreCase("timestamp with time zone")) {
            try {
                return parseTimestampWithTimeZoneAsSqlTimestamp((String) value);
            }
            catch (IllegalArgumentException e) {
                throw new SQLException("Invalid timestamp from server: " + value, e);
            }
        }

        throw new IllegalArgumentException("Expected column to be a timestamp type but is " + columnInfo.getColumnTypeName());
    }

    private static ZonedDateTime parseTimestampWithTimeZone(String value)
    {
        ParsedTimestamp parsed = parseTimestamp(value);
        return toZonedDateTime(parsed, timezone -> ZoneId.of(timezone.orElseThrow(() -> new IllegalArgumentException("Time zone missing: " + value))));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getBinaryStream");
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        return getString(columnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        return getBoolean(columnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        return getByte(columnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        return getShort(columnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        return getInt(columnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        return getLong(columnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        return getFloat(columnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        return getDouble(columnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        return getBigDecimal(columnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        return getBytes(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getBinaryStream");
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        ColumnInfo columnInfo = columnInfo(columnIndex);

        if (columnInfo.getColumnType() == Types.ARRAY) {
            // Array requires special treatment due to element metadata provided by the Array object
            // TODO (https://github.com/trinodb/trino/issues/6049) consider returning List instead
            return getArray(columnIndex);
        }

        Class<?> defaultRepresentation = DEFAULT_OBJECT_REPRESENTATION.get(columnInfo.getColumnTypeSignature().getRawType());
        if (defaultRepresentation != null) {
            return getObject(columnIndex, defaultRepresentation);
        }

        return column(columnIndex);
    }

    @javax.annotation.Nullable
    private static Object convertFromClientRepresentation(ClientTypeSignature columnType, @javax.annotation.Nullable Object value)
            throws SQLException
    {
        requireNonNull(columnType, "columnType is null");

        if (value == null) {
            return null;
        }

        switch (columnType.getRawType()) {
            case "array": {
                ClientTypeSignature elementType = getOnlyElement(columnType.getArgumentsAsTypeSignatures());
                List<Object> converted = Lists.newArrayListWithExpectedSize(((List<?>) value).size());
                for (Object element : (List<?>) value) {
                    converted.add(convertFromClientRepresentation(elementType, element));
                }
                return unmodifiableList(converted);
            }

            case "map": {
                List<ClientTypeSignature> typeSignatures = columnType.getArgumentsAsTypeSignatures();
                verify(typeSignatures.size() == 2, "Unexpected map parameters: %s", typeSignatures);
                ClientTypeSignature keyType = typeSignatures.get(0);
                ClientTypeSignature valueType = typeSignatures.get(1);
                Map<Object, Object> converted = new HashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    converted.put(convertFromClientRepresentation(keyType, entry.getKey()), convertFromClientRepresentation(valueType, entry.getValue()));
                }
                return unmodifiableMap(converted);
            }

            case "row": {
                io.trino.client.Row row = (io.trino.client.Row) value;
                List<io.trino.client.RowField> fields = row.getFields();
                List<ClientTypeSignatureParameter> typeArguments = columnType.getArguments();
                Row.Builder builder = Row.builder();
                verify(fields.size() == typeArguments.size(), "Type mismatch: %s, %s", row, columnType);
                for (int i = 0; i < fields.size(); i++) {
                    io.trino.client.RowField field = fields.get(i);
                    ClientTypeSignatureParameter clientTypeSignatureParameter = typeArguments.get(i);
                    verify(clientTypeSignatureParameter.getKind() == ClientTypeSignatureParameter.ParameterKind.NAMED_TYPE, "Not a NAMED_TYPE: %s", clientTypeSignatureParameter);
                    verify(field.getName().equals(clientTypeSignatureParameter.getNamedTypeSignature().getName()), "Name mismatch: %s, %s", field, clientTypeSignatureParameter);
                    Object converted = convertFromClientRepresentation(clientTypeSignatureParameter.getNamedTypeSignature().getTypeSignature(), field.getValue());
                    builder.addField(field.getName(), converted);
                }
                return builder.build();
            }
        }

        Class<?> defaultRepresentation = DEFAULT_OBJECT_REPRESENTATION.get(columnType.getRawType());
        if (defaultRepresentation != null) {
            return TYPE_CONVERSIONS.convert(columnType, value, defaultRepresentation);
        }

        return value;
    }

    private static TrinoIntervalYearMonth parseIntervalYearMonth(String value)
    {
        return new TrinoIntervalYearMonth(IntervalYearMonth.parseMonths(value));
    }

    private static TrinoIntervalDayTime parseIntervalDayTime(String value)
    {
        return new TrinoIntervalDayTime(IntervalDayTime.parseMillis(value));
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        return getObject(columnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        checkOpen();
        return columnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new NotImplementedException("ResultSet", "getCharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        return parseBigDecimal(String.valueOf(value));
    }

    private static BigDecimal parseBigDecimal(String value)
            throws SQLException
    {
        return toBigDecimal(String.valueOf(value))
                .orElseThrow(() -> new SQLException("Value is not a number: " + value));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        return getBigDecimal(columnIndex(columnLabel));
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isBeforeFirst");
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isAfterLast");
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isFirst");
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isLast");
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("beforeFirst");
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("afterLast");
    }

    @Override
    public boolean first()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("first");
    }

    @Override
    public boolean last()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("last");
    }

    @Override
    public int getRow()
            throws SQLException
    {
        checkOpen();

        long rowNumber = currentRowNumber.get();
        if (rowNumber < 0 || rowNumber > Integer.MAX_VALUE) {
            throw new SQLException("Current row exceeds limit of 2147483647");
        }

        return (int) rowNumber;
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("absolute");
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("relative");
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("previous");
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        checkOpen();
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Fetch direction must be FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        checkOpen();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Rows is negative");
        }
        // fetch size is ignored
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        checkOpen();
        // fetch size is ignored
        return 0;
    }

    @Override
    public int getType()
            throws SQLException
    {
        checkOpen();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        checkOpen();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowUpdated");
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowInserted");
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowDeleted");
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void insertRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("insertRow");
    }

    @Override
    public void updateRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRow");
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("deleteRow");
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("refreshRow");
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow");
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        if (statement.isPresent()) {
            return statement.get();
        }

        throw new SQLException("Statement not available");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        ColumnInfo columnInfo = columnInfo(columnIndex);
        ClientTypeSignature columnTypeSignature = columnInfo.getColumnTypeSignature();
        String elementTypeName = getOnlyElement(columnTypeSignature.getArguments()).toString();
        int elementType = getOnlyElement(columnInfo.getColumnParameterTypes());
        return new TrinoArray(elementTypeName, elementType, (List<?>) convertFromClientRepresentation(columnTypeSignature, value));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        return getArray(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getDate(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getTime(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        return getTimestamp(columnIndex, DateTimeZone.forTimeZone(cal.getTimeZone()));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return closed.get();
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        if (type == null) {
            throw new SQLException("type is null");
        }
        Object object = column(columnIndex);
        if (object == null) {
            return null;
        }

        ClientTypeSignature columnTypeSignature = columnInfo(columnIndex).getColumnTypeSignature();
        if (type.isInstance(object) && !TYPE_CONVERSIONS.hasConversion(columnTypeSignature.getRawType(), type)) {
            return type.cast(object);
        }

        try {
            T converted = TYPE_CONVERSIONS.convert(columnTypeSignature, object, type);
            verify(converted != null, "Conversion cannot return null for non-null input, as this breaks wasNull()");
            return converted;
        }
        catch (NoConversionRegisteredException e) {
            throw new SQLException(format("Cannot convert from %s to %s", columnTypeSignature, type));
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        return getObject(columnIndex(columnLabel), type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkValidRow()
            throws SQLException
    {
        if (row.get() == null) {
            throw new SQLException("Not on a valid row");
        }
    }

    private Object column(int index)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        if ((index <= 0) || (index > resultSetMetaData.getColumnCount())) {
            throw new SQLException("Invalid column index: " + index);
        }
        Object value = row.get().get(index - 1);
        wasNull.set(value == null);
        return value;
    }

    private ColumnInfo columnInfo(int index)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        if ((index <= 0) || (index > columnInfoList.size())) {
            throw new SQLException("Invalid column index: " + index);
        }
        return columnInfoList.get(index - 1);
    }

    private int columnIndex(String label)
            throws SQLException
    {
        if (label == null) {
            throw new SQLException("Column label is null");
        }
        Integer index = fieldMap.get(label.toLowerCase(ENGLISH));
        if (index == null) {
            throw new SQLException("Invalid column label: " + label);
        }
        return index;
    }

    private static Number toNumber(Object value)
            throws SQLException
    {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        if (value instanceof String) {
            Optional<BigDecimal> bigDecimal = toBigDecimal((String) value);
            if (bigDecimal.isPresent()) {
                return bigDecimal.get();
            }
        }
        throw new SQLException("Value is not a number: " + value);
    }

    private static Optional<BigDecimal> toBigDecimal(String value)
    {
        try {
            return Optional.of(new BigDecimal(value));
        }
        catch (NumberFormatException ne) {
            return Optional.empty();
        }
    }

    static SQLException resultsException(QueryStatusInfo results)
    {
        QueryError error = requireNonNull(results.getError());
        String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
        Throwable cause = (error.getFailureInfo() == null) ? null : error.getFailureInfo().toException();
        return new SQLException(message, error.getSqlState(), error.getErrorCode(), cause);
    }

    private static Map<String, Integer> getFieldMap(List<Column> columns)
    {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName().toLowerCase(ENGLISH);
            if (!map.containsKey(name)) {
                map.put(name, i + 1);
            }
        }
        return ImmutableMap.copyOf(map);
    }

    private static List<ColumnInfo> getColumnInfo(List<Column> columns)
    {
        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        for (Column column : columns) {
            ColumnInfo.Builder builder = new ColumnInfo.Builder()
                    .setCatalogName("") // TODO
                    .setSchemaName("") // TODO
                    .setTableName("") // TODO
                    .setColumnLabel(column.getName())
                    .setColumnName(column.getName()) // TODO
                    .setColumnTypeSignature(column.getTypeSignature())
                    .setNullable(Nullable.UNKNOWN)
                    .setCurrency(false);
            setTypeInfo(builder, column.getTypeSignature());
            list.add(builder.build());
        }
        return list.build();
    }

    private static Timestamp parseTimestampWithTimeZoneAsSqlTimestamp(String value)
    {
        ParsedTimestamp parsed = parseTimestamp(value);
        return toTimestamp(value, parsed, timezone ->
                ZoneId.of(timezone.orElseThrow(() -> new IllegalArgumentException("Time zone missing: " + value))));
    }

    private static Timestamp parseTimestampAsSqlTimestamp(String value, ZoneId localTimeZone)
    {
        requireNonNull(localTimeZone, "localTimeZone is null");

        ParsedTimestamp parsed = parseTimestamp(value);
        return toTimestamp(value, parsed, timezone -> {
            if (timezone.isPresent()) {
                throw new IllegalArgumentException("Invalid timestamp: " + value);
            }
            return localTimeZone;
        });
    }

    private static ParsedTimestamp parseTimestamp(String value)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        int year = Integer.parseInt(matcher.group("year"));
        int month = Integer.parseInt(matcher.group("month"));
        int day = Integer.parseInt(matcher.group("day"));
        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = Integer.parseInt(matcher.group("second"));
        String fraction = matcher.group("fraction");
        Optional<String> timezone = Optional.ofNullable(matcher.group("timezone"));

        long picosOfSecond = 0;
        if (fraction != null) {
            int precision = fraction.length();
            verify(precision <= 12, "Unsupported timestamp precision %s: %s", precision, value);
            long fractionValue = Long.parseLong(fraction);
            picosOfSecond = rescale(fractionValue, precision, 12);
        }

        return new ParsedTimestamp(year, month, day, hour, minute, second, picosOfSecond, timezone);
    }

    private static Timestamp toTimestamp(String originalValue, ParsedTimestamp parsed, Function<Optional<String>, ZoneId> timeZoneParser)
    {
        int year = parsed.year;
        int month = parsed.month;
        int day = parsed.day;
        int hour = parsed.hour;
        int minute = parsed.minute;
        int second = parsed.second;
        long picosOfSecond = parsed.picosOfSecond;
        ZoneId zoneId = timeZoneParser.apply(parsed.timezone);

        long epochSecond = LocalDateTime.of(year, month, day, hour, minute, second, 0)
                .atZone(zoneId)
                .toEpochSecond();

        if (epochSecond < START_OF_MODERN_ERA_SECONDS) {
            // slower path, but accurate for historical dates
            GregorianCalendar calendar = new GregorianCalendar(year, month - 1, day, hour, minute, second);
            calendar.setTimeZone(TimeZone.getTimeZone(zoneId));
            verify(calendar.getTimeInMillis() % MILLISECONDS_PER_SECOND == 0, "Fractional second when recalculating epochSecond of a historical date: %s", originalValue);
            epochSecond = calendar.getTimeInMillis() / MILLISECONDS_PER_SECOND;
        }

        int nanoOfSecond = (int) rescale(picosOfSecond, 12, 9);
        if (nanoOfSecond == NANOSECONDS_PER_SECOND) {
            epochSecond++;
            nanoOfSecond = 0;
        }

        Timestamp timestamp = new Timestamp(epochSecond * MILLISECONDS_PER_SECOND);
        timestamp.setNanos(nanoOfSecond);
        return timestamp;
    }

    private static ZonedDateTime toZonedDateTime(ParsedTimestamp parsed, Function<Optional<String>, ZoneId> timeZoneParser)
    {
        int year = parsed.year;
        int month = parsed.month;
        int day = parsed.day;
        int hour = parsed.hour;
        int minute = parsed.minute;
        int second = parsed.second;
        long picosOfSecond = parsed.picosOfSecond;
        ZoneId zoneId = timeZoneParser.apply(parsed.timezone);

        ZonedDateTime zonedDateTime = LocalDateTime.of(year, month, day, hour, minute, second, 0)
                .atZone(zoneId);

        int nanoOfSecond = (int) rescale(picosOfSecond, 12, 9);
        zonedDateTime = zonedDateTime.plusNanos(nanoOfSecond);
        return zonedDateTime;
    }

    private static Time parseTime(String value, ZoneId localTimeZone)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));

        if (hour > 23 || minute > 59 || second > 59) {
            throw new IllegalArgumentException("Invalid time: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");
        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        long picosOfSecond = rescale(fractionValue, precision, 12); // maximum precision
        // We eventually truncate to millis, so truncate picos to nanos for consistency TODO (https://github.com/trinodb/trino/issues/6205) reconsider
        int nanosOfSecond = toIntExact(picosOfSecond / PICOSECONDS_PER_NANOSECOND);
        long epochMilli = ZonedDateTime.of(1970, 1, 1, hour, minute, second, nanosOfSecond, localTimeZone)
                .toInstant()
                .toEpochMilli();

        return new Time(epochMilli);
    }

    private static Time parseTimeWithTimeZone(String value)
    {
        Matcher matcher = TIME_WITH_TIME_ZONE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time: " + value);
        }

        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));
        int offsetSign = matcher.group("sign").equals("+") ? 1 : -1;
        int offsetHour = Integer.parseInt((matcher.group("offsetHour")));
        int offsetMinute = Integer.parseInt((matcher.group("offsetMinute")));

        if (hour > 23 || minute > 59 || second > 59 || !isValidOffset(offsetHour, offsetMinute)) {
            throw new IllegalArgumentException("Invalid time with time zone: " + value);
        }

        int precision = 0;
        String fraction = matcher.group("fraction");

        long fractionValue = 0;
        if (fraction != null) {
            precision = fraction.length();

            if (precision > MAX_DATETIME_PRECISION) {
                throw new IllegalArgumentException(format("Precision must be <= %s: %s", MAX_DATETIME_PRECISION, value));
            }

            fractionValue = Long.parseLong(fraction);
        }

        long epochMilli = (hour * 3600 + minute * 60 + second) * MILLISECONDS_PER_SECOND + rescale(fractionValue, precision, 3);

        epochMilli -= calculateOffsetMinutes(offsetSign, offsetHour, offsetMinute) * MILLISECONDS_PER_MINUTE;

        return new Time(epochMilli);
    }

    public static int calculateOffsetMinutes(int sign, int offsetHour, int offsetMinute)
    {
        return sign * (offsetHour * 60 + offsetMinute);
    }

    private static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (value < 0) {
            throw new IllegalArgumentException("value must be >= 0");
        }

        if (fromPrecision <= toPrecision) {
            value *= scaleFactor(fromPrecision, toPrecision);
        }
        else {
            value = roundDiv(value, scaleFactor(toPrecision, fromPrecision));
        }

        return value;
    }

    private static long scaleFactor(int fromPrecision, int toPrecision)
    {
        if (fromPrecision > toPrecision) {
            throw new IllegalArgumentException("fromPrecision must be <= toPrecision");
        }

        return POWERS_OF_TEN[toPrecision - fromPrecision];
    }

    private static long roundDiv(long value, long factor)
    {
        checkArgument(factor > 0, "factor must be positive");

        if (value >= 0) {
            return (value + (factor / 2)) / factor;
        }

        return (value - (factor / 2)) / factor;
    }

    private static boolean isValidOffset(int hour, int minute)
    {
        return (hour == 14 && minute == 0) ||
                (hour >= 0 && hour < 14 && minute >= 0 && minute <= 59);
    }

    private static class ParsedTimestamp
    {
        private final int year;
        private final int month;
        private final int day;
        private final int hour;
        private final int minute;
        private final int second;
        private final long picosOfSecond;
        private final Optional<String> timezone;

        public ParsedTimestamp(int year, int month, int day, int hour, int minute, int second, long picosOfSecond, Optional<String> timezone)
        {
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
            this.picosOfSecond = picosOfSecond;
            this.timezone = requireNonNull(timezone, "timezone is null");
        }
    }
}
