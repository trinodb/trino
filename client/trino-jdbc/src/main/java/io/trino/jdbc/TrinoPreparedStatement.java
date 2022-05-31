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
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import org.joda.time.DateTimeZone;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH;
import static io.trino.jdbc.AbstractTrinoResultSet.DATE_FORMATTER;
import static io.trino.jdbc.AbstractTrinoResultSet.TIMESTAMP_FORMATTER;
import static io.trino.jdbc.AbstractTrinoResultSet.TIME_FORMATTER;
import static io.trino.jdbc.ColumnInfo.setTypeInfo;
import static io.trino.jdbc.ObjectCasts.castToBigDecimal;
import static io.trino.jdbc.ObjectCasts.castToBinary;
import static io.trino.jdbc.ObjectCasts.castToBoolean;
import static io.trino.jdbc.ObjectCasts.castToByte;
import static io.trino.jdbc.ObjectCasts.castToDouble;
import static io.trino.jdbc.ObjectCasts.castToFloat;
import static io.trino.jdbc.ObjectCasts.castToInt;
import static io.trino.jdbc.ObjectCasts.castToLong;
import static io.trino.jdbc.ObjectCasts.castToShort;
import static io.trino.jdbc.ObjectCasts.invalidConversion;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.Objects.requireNonNull;

public class TrinoPreparedStatement
        extends TrinoStatement
        implements PreparedStatement
{
    private static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(ISO_LOCAL_TIME)
                    .toFormatter();

    private static final DateTimeFormatter OFFSET_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(ISO_LOCAL_TIME)
                    .appendOffset("+HH:mm", "+00:00")
                    .toFormatter();

    private static final Pattern TOP_LEVEL_TYPE_PATTERN = Pattern.compile("(.+?)\\((.+)\\)");
    private static final Pattern TIMESTAMP_WITH_TIME_ZONE_PRECISION_PATTERN = Pattern.compile("timestamp\\((\\d+)\\) with time zone");
    private static final Pattern TIME_WITH_TIME_ZONE_PRECISION_PATTERN = Pattern.compile("time\\((\\d+)\\) with time zone");

    private final Map<Integer, String> parameters = new HashMap<>();
    private final List<List<String>> batchValues = new ArrayList<>();
    private final String statementName;
    private final String originalSql;
    private boolean isBatch;

    TrinoPreparedStatement(TrinoConnection connection, Consumer<TrinoStatement> onClose, String statementName, String sql)
            throws SQLException
    {
        super(connection, onClose);
        this.statementName = requireNonNull(statementName, "statementName is null");
        this.originalSql = requireNonNull(sql, "sql is null");
        super.execute(format("PREPARE %s FROM %s", statementName, sql));
    }

    @Override
    public void close()
            throws SQLException
    {
        optionalConnection().ifPresent(x -> x.removePreparedStatement(statementName));
        super.close();
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        requireNonBatchStatement();
        if (!super.execute(getExecuteSql(statementName, toValues(parameters)))) {
            throw new SQLException("Prepared SQL statement is not a query: " + originalSql);
        }
        return getResultSet();
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        requireNonBatchStatement();
        return Ints.saturatedCast(executeLargeUpdate());
    }

    @Override
    public long executeLargeUpdate()
            throws SQLException
    {
        requireNonBatchStatement();
        if (super.execute(getExecuteSql(statementName, toValues(parameters)))) {
            throw new SQLException("Prepared SQL is not an update statement: " + originalSql);
        }
        return getLargeUpdateCount();
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        requireNonBatchStatement();
        return super.execute(getExecuteSql(statementName, toValues(parameters)));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, typedNull(sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatBooleanLiteral(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("TINYINT", Byte.toString(x)));
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("SMALLINT", Short.toString(x)));
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("INTEGER", Integer.toString(x)));
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("BIGINT", Long.toString(x)));
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("REAL", Float.toString(x)));
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        checkOpen();
        setParameter(parameterIndex, formatLiteral("DOUBLE", Double.toString(x)));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.DECIMAL);
        }
        else {
            setParameter(parameterIndex, formatLiteral("DECIMAL", x.toString()));
        }
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.VARCHAR);
        }
        else {
            setParameter(parameterIndex, formatStringLiteral(x));
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.VARBINARY);
        }
        else {
            setParameter(parameterIndex, formatBinaryLiteral(x));
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.DATE);
        }
        else {
            setAsDate(parameterIndex, x);
        }
    }

    private void setAsDate(int parameterIndex, Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");

        String literal = toDateLiteral(value);
        setParameter(parameterIndex, formatLiteral("DATE", literal));
    }

    private String toDateLiteral(Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");
        if (value instanceof java.util.Date) {
            return DATE_FORMATTER.print(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalDate) {
            return ISO_LOCAL_DATE.format(((LocalDate) value));
        }
        if (value instanceof LocalDateTime) {
            return ISO_LOCAL_DATE.format(((LocalDateTime) value));
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "date");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
        }
        else {
            setAsTime(parameterIndex, x);
        }
    }

    private void setAsTime(int parameterIndex, Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");

        String literal = toTimeLiteral(value);
        setParameter(parameterIndex, formatLiteral("TIME", literal));
    }

    private String toTimeLiteral(Object value)
            throws SQLException
    {
        if (value instanceof java.util.Date) {
            return TIME_FORMATTER.print(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalTime) {
            return ISO_LOCAL_TIME.format((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return ISO_LOCAL_TIME.format((LocalDateTime) value);
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "time");
    }

    private void setAsTimeWithTimeZone(int parameterIndex, Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");

        String literal = toTimeWithTimeZoneLiteral(value);
        setParameter(parameterIndex, formatLiteral("TIME", literal));
    }

    private String toTimeWithTimeZoneLiteral(Object value)
            throws SQLException
    {
        if (value instanceof OffsetTime) {
            return OFFSET_TIME_FORMATTER.format((OffsetTime) value);
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "time with time zone");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        }
        else {
            setAsTimestamp(parameterIndex, x);
        }
    }

    private void setAsTimestamp(int parameterIndex, Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");

        String literal = toTimestampLiteral(value);
        setParameter(parameterIndex, formatLiteral("TIMESTAMP", literal));
    }

    private String toTimestampLiteral(Object value)
            throws SQLException
    {
        if (value instanceof java.util.Date) {
            return TIMESTAMP_FORMATTER.print(((java.util.Date) value).getTime());
        }
        if (value instanceof LocalDateTime) {
            return LOCAL_DATE_TIME_FORMATTER.format(((LocalDateTime) value));
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "timestamp");
    }

    private void setAsTimestampWithTimeZone(int parameterIndex, Object value)
            throws SQLException
    {
        requireNonNull(value, "value is null");

        String literal = toTimestampWithTimeZoneLiteral(value);
        setParameter(parameterIndex, formatLiteral("TIMESTAMP", literal));
    }

    private String toTimestampWithTimeZoneLiteral(Object value)
            throws SQLException
    {
        // TODO (https://github.com/trinodb/trino/issues/6299) support ZonedDateTime
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "timestamp with time zone");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        checkOpen();
        if (x == null || cal == null) {
            setTimestamp(parameterIndex, x);
        }
        else {
            String formattedDateTime = TIMESTAMP_FORMATTER.withZone(DateTimeZone.forTimeZone(cal.getTimeZone())).print(x.getTime());
            setParameter(parameterIndex, formatLiteral("TIMESTAMP", formattedDateTime));
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        checkOpen();
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                setBoolean(parameterIndex, castToBoolean(x, targetSqlType));
                return;
            case Types.TINYINT:
                setByte(parameterIndex, castToByte(x, targetSqlType));
                return;
            case Types.SMALLINT:
                setShort(parameterIndex, castToShort(x, targetSqlType));
                return;
            case Types.INTEGER:
                setInt(parameterIndex, castToInt(x, targetSqlType));
                return;
            case Types.BIGINT:
                setLong(parameterIndex, castToLong(x, targetSqlType));
                return;
            case Types.FLOAT:
            case Types.REAL:
                setFloat(parameterIndex, castToFloat(x, targetSqlType));
                return;
            case Types.DOUBLE:
                setDouble(parameterIndex, castToDouble(x, targetSqlType));
                return;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, castToBigDecimal(x, targetSqlType));
                return;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                setString(parameterIndex, x.toString());
                return;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                setBytes(parameterIndex, castToBinary(x, targetSqlType));
                return;
            case Types.DATE:
                setAsDate(parameterIndex, x);
                return;
            case Types.TIME:
                setAsTime(parameterIndex, x);
                return;
            case Types.TIME_WITH_TIMEZONE:
                setAsTimeWithTimeZone(parameterIndex, x);
                return;
            case Types.TIMESTAMP:
                setAsTimestamp(parameterIndex, x);
                return;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                setAsTimestampWithTimeZone(parameterIndex, x);
                return;
        }
        throw new SQLException("Unsupported target SQL type: " + targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType)
            throws SQLException
    {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber());
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        checkOpen();
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        }
        else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        }
        else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        }
        else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        }
        else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        }
        else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        }
        else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        }
        else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        }
        else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        }
        else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        }
        else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        }
        else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        }
        else if (x instanceof LocalDate) {
            setAsDate(parameterIndex, x);
        }
        else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        }
        // TODO (https://github.com/trinodb/trino/issues/6299) LocalTime -> setAsTime
        else if (x instanceof OffsetTime) {
            setAsTimeWithTimeZone(parameterIndex, x);
        }
        else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        }
        else {
            throw new SQLException("Unsupported object type: " + x.getClass().getName());
        }
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        checkOpen();
        batchValues.add(toValues(parameters));
        isBatch = true;
    }

    @Override
    public void clearBatch()
            throws SQLException
    {
        checkOpen();
        batchValues.clear();
        isBatch = false;
    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        try {
            int[] batchUpdateCounts = new int[batchValues.size()];
            for (int i = 0; i < batchValues.size(); i++) {
                super.execute(getExecuteSql(statementName, batchValues.get(i)));
                batchUpdateCounts[i] = getUpdateCount();
            }
            return batchUpdateCounts;
        }
        finally {
            clearBatch();
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        try (Statement statement = connection().createStatement(); ResultSet resultSet = statement.executeQuery("DESCRIBE OUTPUT " + statementName)) {
            return new TrinoResultSetMetaData(getDescribeOutputColumnInfoList(resultSet));
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setTime");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        try (Statement statement = connection().createStatement(); ResultSet resultSet = statement.executeQuery("DESCRIBE INPUT " + statementName)) {
            return new TrinoParameterMetaData(getParamerters(resultSet));
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        throw new SQLException("This method cannot be called on PreparedStatement");
    }

    private void setParameter(int parameterIndex, String value)
            throws SQLException
    {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index out of bounds: " + parameterIndex);
        }
        parameters.put(parameterIndex - 1, value);
    }

    private static List<String> toValues(Map<Integer, String> parameters)
            throws SQLException
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();
        for (int index = 0; index < parameters.size(); index++) {
            if (!parameters.containsKey(index)) {
                throw new SQLException("No value specified for parameter " + (index + 1));
            }
            values.add(parameters.get(index));
        }
        return values.build();
    }

    private void requireNonBatchStatement()
            throws SQLException
    {
        if (isBatch) {
            throw new SQLException("Batch prepared statement must be executed using executeBatch method");
        }
    }

    private static String getExecuteSql(String statementName, List<String> values)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("EXECUTE ").append(statementName);
        if (!values.isEmpty()) {
            sql.append(" USING ");
            Joiner.on(", ").appendTo(sql, values);
        }
        return sql.toString();
    }

    private static String formatLiteral(String type, String x)
    {
        return type + " " + formatStringLiteral(x);
    }

    private static String formatBooleanLiteral(boolean x)
    {
        return Boolean.toString(x);
    }

    private static String formatStringLiteral(String x)
    {
        return "'" + x.replace("'", "''") + "'";
    }

    private static String formatBinaryLiteral(byte[] x)
    {
        return "X'" + base16().encode(x) + "'";
    }

    private static String typedNull(int targetSqlType)
            throws SQLException
    {
        switch (targetSqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return typedNull("BOOLEAN");
            case Types.TINYINT:
                return typedNull("TINYINT");
            case Types.SMALLINT:
                return typedNull("SMALLINT");
            case Types.INTEGER:
                return typedNull("INTEGER");
            case Types.BIGINT:
                return typedNull("BIGINT");
            case Types.FLOAT:
            case Types.REAL:
                return typedNull("REAL");
            case Types.DOUBLE:
                return typedNull("DOUBLE");
            case Types.DECIMAL:
            case Types.NUMERIC:
                return typedNull("DECIMAL");
            case Types.CHAR:
            case Types.NCHAR:
                return typedNull("CHAR");
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return typedNull("VARCHAR");
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return typedNull("VARBINARY");
            case Types.DATE:
                return typedNull("DATE");
            case Types.TIME:
                return typedNull("TIME");
            case Types.TIMESTAMP:
                return typedNull("TIMESTAMP");
            // TODO Types.TIME_WITH_TIMEZONE
            // TODO Types.TIMESTAMP_WITH_TIMEZONE
            case Types.NULL:
                return "NULL";
        }
        throw new SQLException("Unsupported target SQL type: " + targetSqlType);
    }

    private static String typedNull(String type)
    {
        return format("CAST(NULL AS %s)", type);
    }

    private static List<ColumnInfo> getParamerters(ResultSet resultSet)
            throws SQLException
    {
        ImmutableList.Builder<ColumnInfo> builder = ImmutableList.builder();
        while (resultSet.next()) {
            ClientTypeSignature clientTypeSignature = getClientTypeSignatureFromTypeString(resultSet.getString("Type"));
            ColumnInfo.Builder columnInfoBuilder = new ColumnInfo.Builder()
                    .setCatalogName("")
                    .setSchemaName("")
                    .setTableName("")
                    .setColumnLabel(resultSet.getString("Position"))
                    .setColumnName(resultSet.getString("Position"))
                    .setColumnTypeSignature(clientTypeSignature)
                    .setNullable(ColumnInfo.Nullable.UNKNOWN);
            setTypeInfo(columnInfoBuilder, clientTypeSignature);
            builder.add(columnInfoBuilder.build());
        }
        return builder.build();
    }

    private static List<ColumnInfo> getDescribeOutputColumnInfoList(ResultSet resultSet)
            throws SQLException
    {
        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builder();
        while (resultSet.next()) {
            String columnName = resultSet.getString("Column Name");
            String catalog = resultSet.getString("Catalog");
            String schema = resultSet.getString("Schema");
            String table = resultSet.getString("Table");
            ClientTypeSignature clientTypeSignature = getClientTypeSignatureFromTypeString(resultSet.getString("Type"));
            ColumnInfo.Builder builder = new ColumnInfo.Builder()
                    .setColumnName(columnName)
                    .setColumnLabel(columnName)
                    .setCatalogName(catalog)
                    .setSchemaName(schema)
                    .setTableName(table)
                    .setColumnTypeSignature(clientTypeSignature)
                    .setNullable(ColumnInfo.Nullable.UNKNOWN);
            setTypeInfo(builder, clientTypeSignature);
            list.add(builder.build());
        }
        return list.build();
    }

    @VisibleForTesting
    static ClientTypeSignature getClientTypeSignatureFromTypeString(String type)
    {
        String topLevelType;
        List<ClientTypeSignatureParameter> arguments = new ArrayList<>();
        Matcher topLevelMatcher = TOP_LEVEL_TYPE_PATTERN.matcher(type);
        if (topLevelMatcher.matches()) {
            topLevelType = topLevelMatcher.group(1);
            String typeParameters = topLevelMatcher.group(2);
            if (topLevelType.equals("decimal")) {
                List<String> precisionAndScale = Splitter.on(',').splitToList(typeParameters);
                checkArgument(precisionAndScale.size() == 2, "Invalid decimal parameters: %s", typeParameters);
                arguments.add(ClientTypeSignatureParameter.ofLong(parseLong(precisionAndScale.get(0))));
                arguments.add(ClientTypeSignatureParameter.ofLong(parseLong(precisionAndScale.get(1))));
            }
            else if (topLevelType.equals("char") || topLevelType.equals("varchar")) {
                long precision = parseLong(typeParameters);
                arguments.add(ClientTypeSignatureParameter.ofLong(precision));
            }
            // TODO support array, map, row etc top level types' parameters using recursive parser, current behavior is their parameter list will be empty
        }
        else {
            Matcher timestampMatcher = TIMESTAMP_WITH_TIME_ZONE_PRECISION_PATTERN.matcher(type);
            Matcher timeMatcher = TIME_WITH_TIME_ZONE_PRECISION_PATTERN.matcher(type);
            if (timestampMatcher.matches()) {
                topLevelType = "timestamp with time zone";
                arguments.add(ClientTypeSignatureParameter.ofLong(parseLong(timestampMatcher.group(1))));
            }
            else if (timeMatcher.matches()) {
                topLevelType = "time with time zone";
                arguments.add(ClientTypeSignatureParameter.ofLong(parseLong(timeMatcher.group(1))));
            }
            else {
                topLevelType = type;
                if (topLevelType.equals("varchar")) {
                    arguments.add(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH));
                }
            }
        }
        return new ClientTypeSignature(topLevelType, arguments);
    }
}
