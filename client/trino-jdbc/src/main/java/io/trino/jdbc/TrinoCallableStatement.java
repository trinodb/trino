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
import java.sql.CallableStatement;
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

public class TrinoCallableStatement
        extends TrinoStatement
        implements CallableStatement
{
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public boolean execute() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new NotImplementedException("CallableStatement", "xxxxxx");
    }
}
