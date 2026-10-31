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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TrinoArray
        implements Array
{
    private final String elementTypeName;
    private final ClientTypeSignature elementTypeSignature;
    private final int elementType;
    private final Object[] clientValues;
    private final Object[] array;

    TrinoArray(String elementTypeName, ClientTypeSignature elementTypeSignature, int elementType, List<?> clientValues, List<?> array)
    {
        this.elementTypeName = requireNonNull(elementTypeName, "elementTypeName is null");
        this.elementTypeSignature = requireNonNull(elementTypeSignature, "elementTypeSignature is null");
        this.elementType = elementType;
        this.clientValues = clientValues.toArray();
        this.array = array.toArray();
    }

    @Override
    public String getBaseTypeName()
    {
        return elementTypeName;
    }

    @Override
    public int getBaseType()
    {
        return elementType;
    }

    @Override
    public Object getArray()
    {
        return array.clone();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Object getArray(long index, int count)
            throws SQLException
    {
        int arrayOffset = Ints.saturatedCast(index - 1);
        if (index < 1 || count < 0 || (arrayOffset + count) > array.length) {
            throw new SQLException("Index out of bounds");
        }
        return Arrays.copyOfRange(array, arrayOffset, arrayOffset + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        return getResultSet(1, clientValues.length);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count)
            throws SQLException
    {
        if (index < 1 || count < 0 || index - 1 > clientValues.length - count) {
            throw new SQLException("Index out of bounds");
        }
        int arrayOffset = toIntExact(index - 1);
        List<Column> columns = ImmutableList.of(
                new Column("INDEX", BIGINT, new ClientTypeSignature(BIGINT)),
                new Column("VALUE", elementTypeSignature.toString(), elementTypeSignature));
        // The result set converts values from the client representation, so give it the values before conversion
        List<List<Object>> rows = new ArrayList<>(count);
        for (int i = arrayOffset; i < arrayOffset + count; i++) {
            rows.add(Arrays.asList((long) i + 1, clientValues[i]));
        }
        return new InMemoryTrinoResultSet(columns, rows);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getResultSet not supported");
    }

    @Override
    public void free()
    {
        // no-op
    }

    @Override
    public String toString()
    {
        return Arrays.toString(array);
    }
}
