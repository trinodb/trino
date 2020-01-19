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
package io.prestosql.plugin.salesforce.driver.resultset;

import com.sforce.ws.ConnectionException;
import io.prestosql.plugin.salesforce.driver.delegates.PartnerService;
import io.prestosql.plugin.salesforce.driver.metadata.ColumnMap;
import io.prestosql.plugin.salesforce.driver.statement.ForcePreparedStatement;
import io.prestosql.plugin.salesforce.driver.statement.QueryWrapper;

import javax.sql.rowset.serial.SerialBlob;

import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

public class ForceResultSet
        implements ResultSet, Serializable
{
    private static final long serialVersionUID = 1L;

    private transient Integer index;
    private ResultSetMetaData metadata;
    private PartnerService service;
    private Deque<ColumnMap<String, Object>> rows = new LinkedList<>();
    private ColumnMap<String, Object> currentRecord;
    private QueryWrapper queryWrapper;
    private int lastColumnIndex;

    public ForceResultSet(List<ColumnMap<String, Object>> columnMaps)
    {
        this.rows.addAll(columnMaps);
    }

    public ForceResultSet(QueryWrapper queryWrapper)
    {
        this.queryWrapper = queryWrapper;
    }

    @Override
    public Object getObject(String columnName)
            throws SQLException
    {
        return currentRecord.get(columnName.toUpperCase());
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        lastColumnIndex = columnIndex;
        return currentRecord.getByIndex(columnIndex);
    }

    @Override
    public String getString(String columnName)
            throws SQLException
    {
        return (String) getObject(columnName);
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        return (String) getObject(columnIndex);
    }

    @Override
    public boolean next()
            throws SQLException
    {
        try {
            currentRecord = rows.pop();
            return true;
        }
        catch (NoSuchElementException e) {
            try {
                if (queryWrapper == null) {
                    return false;
                }

                rows.addAll(queryWrapper.next());
                currentRecord = rows.pop();
                return true;
            }
            catch (NoSuchElementException e2) {
                return false;
            }
            catch (ConnectionException e2) {
                ForcePreparedStatement.rethrowAsNonChecked(e2);
            }
        }

        return false;
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return metadata != null ? metadata : new ForceResultSetMetaData();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Date getDate(String columnName, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(BigDecimal::new).parse(columnIndex).orElse(null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(BigDecimal::new).parse(columnName).orElse(null);
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        return false;
    }

    protected java.util.Date parseDate(String dateRepr)
    {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(dateRepr);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(this::parseDate).parse(columnIndex).map(d -> new java.sql.Date(d.getTime())).orElse(null);
    }

    @Override
    public Date getDate(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(this::parseDate).parse(columnName).map(d -> new java.sql.Date(d.getTime())).orElse(null);
    }

    private java.util.Date parseDateTime(String dateRepr)
    {
        try {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(dateRepr);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        Object value = currentRecord.getByIndex(columnIndex);
        if (value instanceof GregorianCalendar) {
            return new java.sql.Timestamp(((GregorianCalendar) value).getTime().getTime());
        }
        else {
            return new ColumnValueParser<>(this::parseDateTime).parse(columnIndex).map(d -> new java.sql.Timestamp(d.getTime())).orElse(null);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnName)
            throws SQLException
    {
        Object value = currentRecord.get(columnName);
        if (value instanceof GregorianCalendar) {
            return new java.sql.Timestamp(((GregorianCalendar) value).getTime().getTime());
        }
        else {
            return new ColumnValueParser<>((v) -> parseDateTime(v)).parse(columnName).map(d -> new java.sql.Timestamp(d.getTime())).orElse(null);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private java.util.Date parseTime(String dateRepr)
    {
        try {
            return new SimpleDateFormat("HH:mm:ss.SSSX").parse(dateRepr);
        }
        catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Time getTime(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(this::parseTime).parse(columnName).map(d -> new Time(d.getTime())).orElse(null);
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(this::parseTime).parse(columnIndex).map(d -> new Time(d.getTime())).orElse(null);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
    {
        Optional<BigDecimal> result = new ColumnValueParser<>(BigDecimal::new).parse(columnIndex);
        result.ifPresent(v -> v.setScale(scale));
        return result.orElse(null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnName, int scale)
    {
        Optional<BigDecimal> result = new ColumnValueParser<>(BigDecimal::new).parse(columnName);
        result.ifPresent(v -> v.setScale(scale));
        return result.orElse(null);
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Float::new).parse(columnIndex).orElse(0f);
    }

    @Override
    public float getFloat(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Float::new).parse(columnName).orElse(0f);
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Double::new).parse(columnIndex).orElse(0d);
    }

    @Override
    public double getDouble(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Double::new).parse(columnName).orElse(0d);
    }

    @Override
    public long getLong(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Long::new).parse(columnName).orElse(0L);
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<Long>(Long::new).parse(columnIndex).orElse(0L);
    }

    @Override
    public int getInt(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Integer::new).parse(columnName).orElse(0);
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Integer::new).parse(columnIndex).orElse(0);
    }

    @Override
    public short getShort(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Short::new).parse(columnName).orElse((short) 0);
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Short::new).parse(columnIndex).orElse((short) 0);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public InputStream getBinaryStream(String columnName)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private Blob createBlob(byte[] data)
    {
        try {
            return new SerialBlob(data);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>((v) -> Base64.getDecoder().decode(v)).parse(columnIndex).map(this::createBlob).orElse(null);
    }

    @Override
    public Blob getBlob(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>((v) -> Base64.getDecoder().decode(v)).parse(columnName).map(this::createBlob).orElse(null);
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Boolean::new).parse(columnIndex).orElse(false);
    }

    @Override
    public boolean getBoolean(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Boolean::new).parse(columnName).orElse(false);
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        return new ColumnValueParser<>(Byte::new).parse(columnIndex).orElse((byte) 0);
    }

    @Override
    public byte getByte(String columnName)
            throws SQLException
    {
        return new ColumnValueParser<>(Byte::new).parse(columnName).orElse((byte) 0);
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public byte[] getBytes(String columnName)
            throws SQLException
    {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        return false;
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        System.out.println("after last check");
    }

    //
    // Not implemented below here
    //

    @Override
    public boolean first()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean last()
            throws SQLException
    {
        return false;
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
    }

    @Override
    public void close()
            throws SQLException
    {
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
    }

    @Override
    public int findColumn(String columnName)
            throws SQLException
    {
        return 0;
    }

    @Override
    public Array getArray(int i)
            throws SQLException
    {
        return null;
    }

    @Override
    public Array getArray(String colName)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getAsciiStream(String columnName)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnName)
            throws SQLException
    {
        return null;
    }

    @Override
    public Clob getClob(int i)
            throws SQLException
    {
        return null;
    }

    @Override
    public Clob getClob(String colName)
            throws SQLException
    {
        return null;
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        return 0;
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        return null;
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map)
            throws SQLException
    {
        return null;
    }

    @Override
    public Object getObject(String colName, Map<String, Class<?>> map)
            throws SQLException
    {
        return null;
    }

    @Override
    public Ref getRef(int i)
            throws SQLException
    {
        return null;
    }

    @Override
    public Ref getRef(String colName)
            throws SQLException
    {
        return null;
    }

    @Override
    public int getRow()
            throws SQLException
    {
        return 0;
    }

    @Override
    public ForcePreparedStatement getStatement()
            throws SQLException
    {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public Time getTime(String columnName, Calendar cal)
            throws SQLException
    {
        return null;
    }

    @Override
    public int getType()
            throws SQLException
    {
        return 0;
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public URL getURL(String columnName)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnName)
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        return null;
    }

    @Override
    public void insertRow()
            throws SQLException
    {
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        return false;
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        return false;
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
    }

    @Override
    public void updateArray(String columnName, Array x)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnName, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnName, InputStream x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnName, Blob x)
            throws SQLException
    {
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
    }

    @Override
    public void updateBoolean(String columnName, boolean x)
            throws SQLException
    {
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
    }

    @Override
    public void updateByte(String columnName, byte x)
            throws SQLException
    {
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
    }

    @Override
    public void updateBytes(String columnName, byte[] x)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, int length)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnName, Clob x)
            throws SQLException
    {
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
    }

    @Override
    public void updateDate(String columnName, Date x)
            throws SQLException
    {
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
    }

    @Override
    public void updateDouble(String columnName, double x)
            throws SQLException
    {
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
    }

    @Override
    public void updateFloat(String columnName, float x)
            throws SQLException
    {
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
    }

    @Override
    public void updateInt(String columnName, int x)
            throws SQLException
    {
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
    }

    @Override
    public void updateLong(String columnName, long x)
            throws SQLException
    {
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
    }

    @Override
    public void updateNull(String columnName)
            throws SQLException
    {
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
    }

    @Override
    public void updateObject(String columnName, Object x)
            throws SQLException
    {
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException
    {
    }

    @Override
    public void updateObject(String columnName, Object x, int scale)
            throws SQLException
    {
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
    }

    @Override
    public void updateRef(String columnName, Ref x)
            throws SQLException
    {
    }

    @Override
    public void updateRow()
            throws SQLException
    {
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
    }

    @Override
    public void updateShort(String columnName, short x)
            throws SQLException
    {
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
    }

    @Override
    public void updateString(String columnName, String x)
            throws SQLException
    {
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
    }

    @Override
    public void updateTime(String columnName, Time x)
            throws SQLException
    {
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
    }

    @Override
    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException
    {
    }

    @Override
    public boolean wasNull()
            throws SQLException
    {
        return getObject(lastColumnIndex) == null;
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return false;
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    private class ColumnValueParser<T>
    {
        private Function<String, T> conversion;

        public ColumnValueParser(Function<String, T> parser)
        {
            this.conversion = parser;
        }

        public Optional<T> parse(int columnIndex)
        {
            Object value = currentRecord.getByIndex(columnIndex);
            return parse(value);
        }

        public Optional<T> parse(String columnName)
        {
            Object value = currentRecord.get(columnName.toUpperCase());
            return parse(value);
        }

        private Optional<T> parse(Object o)
        {
            if (o == null) {
                return Optional.empty();
            }
            if (!(o instanceof String)) {
                return (Optional<T>) Optional.of(o);
            }
            return Optional.of(conversion.apply((String) o));
        }
    }
}
