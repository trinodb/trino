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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Consumer;

public class TrinoCallableStatement
        extends TrinoPreparedStatement
        implements CallableStatement
{
    public TrinoCallableStatement(TrinoConnection connection, Consumer<TrinoStatement> onClose, String statementName, String sql)
            throws SQLException
    {
        super(connection, onClose, statementName, sql);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "wasNull");
    }

    @Override
    public String getString(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getString");
    }

    @Override
    public boolean getBoolean(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBoolean");
    }

    @Override
    public byte getByte(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getByte");
    }

    @Override
    public short getShort(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getShort");
    }

    @Override
    public int getInt(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getInt");
    }

    @Override
    public long getLong(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getLong");
    }

    @Override
    public float getFloat(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getFloat");
    }

    @Override
    public double getDouble(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDouble");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBigDecimal");
    }

    @Override
    public byte[] getBytes(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBytes");
    }

    @Override
    public Date getDate(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDate");
    }

    @Override
    public Time getTime(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTime");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTimestamp");
    }

    @Override
    public Object getObject(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBigDecimal");
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public Ref getRef(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getRef");
    }

    @Override
    public Blob getBlob(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBlob");
    }

    @Override
    public Clob getClob(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getClob");
    }

    @Override
    public Array getArray(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getArray");
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDate");
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTime");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTimestamp");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "registerOutParameter");
    }

    @Override
    public URL getURL(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getURL");
    }

    @Override
    public void setURL(String parameterName, URL val)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setURL");
    }

    @Override
    public void setNull(String parameterName, int sqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNull");
    }

    @Override
    public void setBoolean(String parameterName, boolean x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBoolean");
    }

    @Override
    public void setByte(String parameterName, byte x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setByte");
    }

    @Override
    public void setShort(String parameterName, short x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setShort");
    }

    @Override
    public void setInt(String parameterName, int x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setInt");
    }

    @Override
    public void setLong(String parameterName, long x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setLong");
    }

    @Override
    public void setFloat(String parameterName, float x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setFloat");
    }

    @Override
    public void setDouble(String parameterName, double x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDouble");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBigDecimal");
    }

    @Override
    public void setString(String parameterName, String x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setString");
    }

    @Override
    public void setBytes(String parameterName, byte[] x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBytes");
    }

    @Override
    public void setDate(String parameterName, Date x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDate");
    }

    @Override
    public void setTime(String parameterName, Time x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTime");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTimestamp");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    @Override
    public void setObject(String parameterName, Object x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDate");
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTime");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTimestamp");
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNull");
    }

    @Override
    public String getString(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getString");
    }

    @Override
    public boolean getBoolean(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBoolean");
    }

    @Override
    public byte getByte(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getByte");
    }

    @Override
    public short getShort(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getShort");
    }

    @Override
    public int getInt(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getInt");
    }

    @Override
    public long getLong(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getLong");
    }

    @Override
    public float getFloat(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getFloat");
    }

    @Override
    public double getDouble(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDouble");
    }

    @Override
    public byte[] getBytes(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBytes");
    }

    @Override
    public Date getDate(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDate");
    }

    @Override
    public Time getTime(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTime");
    }

    @Override
    public Timestamp getTimestamp(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTimestamp");
    }

    @Override
    public Object getObject(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBigDecimal");
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public Ref getRef(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getRef");
    }

    @Override
    public Blob getBlob(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getBlob");
    }

    @Override
    public Clob getClob(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getClob");
    }

    @Override
    public Array getArray(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getArray");
    }

    @Override
    public Date getDate(String parameterName, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getDate");
    }

    @Override
    public Time getTime(String parameterName, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTime");
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getTimestamp");
    }

    @Override
    public URL getURL(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getURL");
    }

    @Override
    public RowId getRowId(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getRowId");
    }

    @Override
    public RowId getRowId(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getRowId");
    }

    @Override
    public void setRowId(String parameterName, RowId x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setRowId");
    }

    @Override
    public void setNString(String parameterName, String value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNString");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNCharacterStream");
    }

    @Override
    public void setNClob(String parameterName, NClob value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }

    @Override
    public NClob getNClob(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNClob");
    }

    @Override
    public NClob getNClob(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNClob");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setSQLXML");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getSQLXML");
    }

    @Override
    public String getNString(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNString");
    }

    @Override
    public String getNString(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNString");
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getNCharacterStream");
    }

    @Override
    public Reader getCharacterStream(int parameterIndex)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String parameterName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getCharacterStream");
    }

    @Override
    public void setBlob(String parameterName, Blob x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setClob(String parameterName, Clob x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNCharacterStream");
    }

    @Override
    public void setClob(String parameterName, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setNClob(String parameterName, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getObject");
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "executeQuery");
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "executeUpdate");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "executeQuery");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBoolean");
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setByte");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setShort");
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setInt");
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setLong");
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setFloat");
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDouble");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBigDecimal");
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setString");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTimestamp");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "clearParameters");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    /*
    @Override
    public boolean execute()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "execute");
    }
     */

    @Override
    public void addBatch()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getMetaData");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNull");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setURL");
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "getParameterMetaData");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setObject");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        throw new NotImplementedException("CallableStatement", "setNClob");
    }
}
