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
package io.prestosql.plugin.salesforce.driver.connection;

import com.sforce.soap.partner.PartnerConnection;
import io.prestosql.plugin.salesforce.driver.metadata.ForceDatabaseMetaData;
import io.prestosql.plugin.salesforce.driver.metadata.ForceDatabaseMetadataCache;
import io.prestosql.plugin.salesforce.driver.statement.ForcePreparedStatement;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

public class ForceConnection
        implements Connection
{
    private static final String SF_JDBC_DRIVER_NAME = "SF JDBC driver";
    @Inject @ForceDatabaseMetadataCache private static Provider<Map<String, ForceDatabaseMetaData>> metadataCacheProvider;
    private final PartnerConnection partnerConnection;
    private ForceDatabaseMetaData metadata;

    private Map connectionCache = new HashMap<>();

    public ForceConnection(PartnerConnection partnerConnection)
    {
        this.partnerConnection = partnerConnection;

        Map<String, ForceDatabaseMetaData> metadataCache = metadataCacheProvider.get();
        metadata = metadataCache.get(this.partnerConnection.getConfig().getUsername());

        if (this.metadata == null) {
            this.metadata = new ForceDatabaseMetaData();
            this.metadata.setConnection(this);
            metadataCache.put(this.partnerConnection.getConfig().getUsername(), this.metadata);
        }

        this.metadata.setConnection(this);
    }

    public PartnerConnection getPartnerConnection()
    {
        return partnerConnection;
    }

    @Override
    public DatabaseMetaData getMetaData()
    {
        return metadata;
    }

    @Override
    public PreparedStatement prepareStatement(String soql)
    {
        return new ForcePreparedStatement(this, soql);
    }

    @Override
    public String getSchema()
    {
        return "Salesforce";
    }

    @Override
    public void setSchema(String schema)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    public Map getCache()
    {
        return connectionCache;
    }

    @Override
    public <T> T unwrap(Class<T> iface)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Statement createStatement()
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql)
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public String nativeSQL(String sql)
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public boolean getAutoCommit()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit)
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void commit()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void rollback()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void close()
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean isClosed()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public String getCatalog()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setCatalog(String catalog)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getTransactionIsolation()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setTransactionIsolation(int level)
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap()
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setHoldability(int holdability)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public Savepoint setSavepoint()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException
    {
        Logger.getLogger(SF_JDBC_DRIVER_NAME).info(Object.class.getEnclosingMethod().getName());
        return null;
    }

    @Override
    public Clob createClob()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Blob createBlob()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NClob createNClob()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isValid(int timeout)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public String getClientInfo(String name)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getClientInfo()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void abort(Executor executor)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public int getNetworkTimeout()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }
}
