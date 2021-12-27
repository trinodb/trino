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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import static io.trino.jdbc.TrinoResultSetMetaData.getType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoParameterMetaData
        implements ParameterMetaData
{
    private final List<ColumnInfo> parameterInfo;

    TrinoParameterMetaData(List<ColumnInfo> parameterInfo)
    {
        this.parameterInfo = ImmutableList.copyOf(requireNonNull(parameterInfo, "parameterInfo is null"));
    }

    @Override
    public int getParameterCount()
            throws SQLException
    {
        return parameterInfo.size();
    }

    @Override
    public int isNullable(int param)
            throws SQLException
    {
        switch (getParameter(param).getNullable()) {
            case NO_NULLS:
                return parameterNoNulls;
            case NULLABLE:
                return parameterNullable;
            case UNKNOWN:
                return parameterNullableUnknown;
            default:
                throw new SQLException(format("Invaliad Nullable Value %s", getParameter(param).getNullable()));
        }
    }

    @Override
    public boolean isSigned(int param)
            throws SQLException
    {
        return getParameter(param).isSigned();
    }

    @Override
    public int getPrecision(int param)
            throws SQLException
    {
        return getParameter(param).getPrecision();
    }

    @Override
    public int getScale(int param)
            throws SQLException
    {
        return getParameter(param).getScale();
    }

    @Override
    public int getParameterType(int param)
            throws SQLException
    {
        return getParameter(param).getColumnType();
    }

    @Override
    public String getParameterTypeName(int param)
            throws SQLException
    {
        return getParameter(param).getColumnTypeSignature().getRawType();
    }

    @Override
    public String getParameterClassName(int param)
            throws SQLException
    {
        return getType(getParameter(param).getColumnType());
    }

    @Override
    public int getParameterMode(int param)
            throws SQLException
    {
        return ParameterMetaData.parameterModeUnknown;
    }

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

    private ColumnInfo getParameter(int column)
            throws SQLException
    {
        if ((column <= 0) || (column > parameterInfo.size())) {
            throw new SQLException(format("Invalid column index: %s (only allow between 1 to %s)", column, parameterInfo.size()));
        }
        return parameterInfo.get(column - 1);
    }
}
