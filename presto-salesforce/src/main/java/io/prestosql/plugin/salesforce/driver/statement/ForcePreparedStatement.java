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
package io.prestosql.plugin.salesforce.driver.statement;

import com.sforce.soap.partner.QueryResult;
import com.sforce.ws.ConnectionException;
import io.prestosql.plugin.salesforce.driver.connection.ForceConnection;
import io.prestosql.plugin.salesforce.driver.delegates.ForceResultField;
import io.prestosql.plugin.salesforce.driver.delegates.PartnerService;
import io.prestosql.plugin.salesforce.driver.metadata.ColumnMap;
import io.prestosql.plugin.salesforce.driver.metadata.ForceDatabaseMetaData;
import io.prestosql.plugin.salesforce.driver.resultset.ForceResultSet;
import org.apache.commons.lang3.StringUtils;
import org.mule.tools.soql.exception.SOQLParsingException;

import javax.sql.rowset.RowSetMetaDataImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ForcePreparedStatement
        implements PreparedStatement
{
    private static final DateFormat SF_DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static Map<Class<?>, Function<Object, String>> paramConverters = new HashMap<>();
    private String soqlQuery;
    private QueryResult queryResult;
    private ForceConnection connection;
    private PartnerService partnerService;
    private ResultSetMetaData metadata;
    private int fetchSize;
    private int maxRows;
    private List<Object> parameters = new ArrayList<>();
    private List<FieldDef> fieldDefinitions;
    private SoqlQueryAnalyzer queryAnalyzer;

    public ForcePreparedStatement(ForceConnection connection, String soql)
    {
        this.connection = connection;
        this.soqlQuery = soql;
    }

    public static <T extends Throwable> RuntimeException rethrowAsNonChecked(Throwable throwable)
            throws T
    {
        throw (T) throwable; // rely on vacuous cast
    }

    protected static String toSoqlStringParam(Object param)
    {
        return "'" + param.toString().replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
    }

    protected static String convertToSoqlParam(Object paramValue)
    {
        Class<?> paramClass = getParamClass(paramValue);
        return paramConverters.get(paramClass).apply(paramValue);
    }

    protected static Class<?> getParamClass(Object paramValue)
    {
        Class<?> paramClass = paramValue != null ? paramValue.getClass() : null;
        if (!paramConverters.containsKey(paramClass)) {
            paramClass = Object.class;
        }
        return paramClass;
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException
    {
        return query();
    }

    private ResultSet query()
            throws SQLException
    {
        try {
            String preparedSoql = prepareQuery();
            QueryWrapper queryWrapper = new QueryWrapper(preparedSoql, getFieldDefinitions(), getPartnerService());
            return new ForceResultSet(queryWrapper);
        }
        catch (ConnectionException | SOQLParsingException e) {
            throw new SQLException(e);
        }
    }

    private String prepareQuery()
    {
        return setParams(soqlQuery);
    }

    private ColumnMap<String, Object> convertToColumnMap(List<ForceResultField> record)
    {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        record.stream().map(field -> field == null ? new ForceResultField(null, null, null, null) : field).forEach(field -> {
            columnMap.put(field.getFullName(), field.getValue());
        });
        return columnMap;
    }

    public List<Object> getParameters()
    {
        int paramsCountInQuery = StringUtils.countMatches(soqlQuery, '?');
        if (parameters.size() < paramsCountInQuery) {
            parameters.addAll(Collections.nCopies(paramsCountInQuery - parameters.size(), null));
        }
        return parameters;
    }

    protected String setParams(String soql)
    {
        String result = soql;
        for (Object param : getParameters()) {
            String paramRepresentation = convertToSoqlParam(param);
            result = result.replaceFirst("\\?", paramRepresentation);
        }
        return result;
    }

    private ResultSetMetaData loadMetaData()
            throws SQLException
    {
        try {
            if (metadata == null) {
                RowSetMetaDataImpl result = new RowSetMetaDataImpl();
                SoqlQueryAnalyzer queryAnalyzer = getQueryAnalyzer();
                List<FieldDef> resultFieldDefinitions = flatten(getFieldDefinitions());
                int columnsCount = resultFieldDefinitions.size();
                result.setColumnCount(columnsCount);
                for (int i = 1; i <= columnsCount; i++) {
                    FieldDef field = resultFieldDefinitions.get(i - 1);
                    result.setAutoIncrement(i, false);
                    result.setColumnName(i, field.getName());
                    result.setColumnLabel(i, field.getName());
                    String forceTypeName = field.getType();
                    ForceDatabaseMetaData.TypeInfo typeInfo = ForceDatabaseMetaData.lookupTypeInfo(forceTypeName);
                    result.setColumnType(i, typeInfo.sqlDataType);
                    result.setColumnTypeName(i, typeInfo.typeName);
                    result.setPrecision(i, typeInfo.precision);
                    result.setSchemaName(i, "Salesforce");
                    result.setTableName(i, queryAnalyzer.getFromObjectName());
                }
                metadata = result;
            }
            return metadata;
        }
        catch (RuntimeException e) {
            throw new SQLException(e.getCause() != null ? e.getCause() : e);
        }
    }

    private List<FieldDef> flatten(List fieldDefinitions)
    {
        return (List<FieldDef>) fieldDefinitions.stream().flatMap(def -> def instanceof List ? ((List) def).stream() : Stream.of(def)).collect(Collectors.toList());
    }

    private List<FieldDef> getFieldDefinitions()
    {
        if (fieldDefinitions == null) {
            fieldDefinitions = getQueryAnalyzer().getFieldDefinitions();
        }
        return fieldDefinitions;
    }

    private SoqlQueryAnalyzer getQueryAnalyzer()
    {
        if (queryAnalyzer == null) {
            queryAnalyzer = new SoqlQueryAnalyzer(prepareQuery(), (objName) -> {
                try {
                    return getPartnerService().describeSObject(objName);
                }
                catch (ConnectionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return queryAnalyzer;
    }

    @Override
    public ParameterMetaData getParameterMetaData()
            throws SQLException
    {
        return new ForceParameterMetadata(parameters, soqlQuery);
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return loadMetaData();
    }

    public PartnerService getPartnerService()
            throws ConnectionException
    {
        if (partnerService == null) {
            partnerService = new PartnerService(connection.getPartnerConnection());
        }
        return partnerService;
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        this.fetchSize = rows;
    }

    @Override
    public int getMaxRows()
            throws SQLException
    {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max)
            throws SQLException
    {
        this.maxRows = max;
    }

    @Override
    public void setArray(int i, Array x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    protected void addParameter(int parameterIndex, Object x)
    {
        parameterIndex--;
        if (parameters.size() < parameterIndex) {
            parameters.addAll(Collections.nCopies(parameterIndex - parameters.size(), null));
        }
        parameters.add(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setBlob(int i, Blob x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setClob(int i, Clob x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setDate(int parameterIndex, Date x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setDouble(int parameterIndex, double x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType)
            throws SQLException
    {
        addParameter(parameterIndex, null);
    }

    @Override
    public void setNull(int paramIndex, int sqlType, String typeName)
            throws SQLException
    {
        addParameter(paramIndex, null);
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setRef(int i, Ref x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setShort(int parameterIndex, short x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x)
            throws SQLException
    {
        addParameter(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setURL(int parameterIndex, URL x)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException
    {
        String methodName = this.getClass().getSimpleName() + "." + new Object()
        {
        }.getClass().getEnclosingMethod().getName();
        throw new UnsupportedOperationException("The " + methodName + " is not implemented yet.");
    }

    @Override
    public ResultSet executeQuery(String sql)
            throws SQLException
    {
        throw new RuntimeException("Not yet implemented.");
    }

    @Override
    public int executeUpdate(String sql)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    // Not required to implement below

    @Override
    public void close()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getMaxFieldSize()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setEscapeProcessing(boolean enable)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getQueryTimeout()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void cancel()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setCursorName(String name)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean execute(String sql)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getResultSet()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getUpdateCount()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean getMoreResults()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getResultSetConcurrency()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResultSetType()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addBatch(String sql)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearBatch()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int[] executeBatch()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isPoolable()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setPoolable(boolean poolable)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void closeOnCompletion()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean isCloseOnCompletion()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int executeUpdate()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void clearParameters()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean execute()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void addBatch()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setRowId(int parameterIndex, RowId x)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNClob(int parameterIndex, NClob value)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    static {
        paramConverters.put(String.class, ForcePreparedStatement::toSoqlStringParam);
        paramConverters.put(Object.class, ForcePreparedStatement::toSoqlStringParam);
        paramConverters.put(Boolean.class, Object::toString);
        paramConverters.put(Double.class, Object::toString);
        paramConverters.put(BigDecimal.class, Object::toString);
        paramConverters.put(Float.class, Object::toString);
        paramConverters.put(Integer.class, Object::toString);
        paramConverters.put(Long.class, Object::toString);
        paramConverters.put(Short.class, Object::toString);
        paramConverters.put(java.util.Date.class, SF_DATETIME_FORMATTER::format);
        paramConverters.put(Timestamp.class, SF_DATETIME_FORMATTER::format);
        paramConverters.put(null, p -> "NULL");
    }
}
