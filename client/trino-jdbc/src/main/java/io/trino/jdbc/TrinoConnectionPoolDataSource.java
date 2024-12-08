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

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Logger;

public class TrinoConnectionPoolDataSource
        implements ConnectionPoolDataSource, Referenceable, Serializable, ObjectFactory
{
    private static final String KEY_URL = "url";
    private static final String KEY_USER = "user";
    private static final String KEY_PASSWORD = "password";

    private String url;
    private String user;
    private String password;
    private PrintWriter printWriter;

    private final Properties connectionProperties = new Properties();
    private final TrinoDriver trinoDriver;

    public TrinoConnectionPoolDataSource()
    {
        trinoDriver = new TrinoDriver();
    }

    @Override
    public PooledConnection getPooledConnection()
            throws SQLException
    {
        return getPooledConnection(getUser(), getPassword());
    }

    @Override
    public PooledConnection getPooledConnection(String user, String password)
            throws SQLException
    {
        update(connectionProperties, KEY_USER, user);
        update(connectionProperties, KEY_PASSWORD, password);

        Connection connection = trinoDriver.connect(url, connectionProperties);
        return new TrinoPooledConnection(connection);
    }

    @Override
    public PrintWriter getLogWriter()
            throws SQLException
    {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out)
            throws SQLException
    {
        this.printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds)
            throws SQLException
    {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout()
            throws SQLException
    {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("Parent logger not supported.");
    }

    @Override
    public Reference getReference()
            throws NamingException
    {
        Reference ref = new Reference(getClass().getName(), getClass().getName(), null);

        ref.add(new StringRefAddr(KEY_URL, getUrl()));
        ref.add(new StringRefAddr(KEY_USER, getUser()));
        ref.add(new StringRefAddr(KEY_PASSWORD, getPassword()));

        return ref;
    }

    @Override
    public Object getObjectInstance(Object object, Name name, Context nameCtx, Hashtable<?, ?> environment)
            throws Exception
    {
        // The spec says to return null if we can't create an instance of the reference
        TrinoConnectionPoolDataSource trinoConnectionPoolDataSource = null;
        if (object instanceof Reference) {
            final Reference reference = (Reference) object;
            if (reference.getClassName().equals(getClass().getName())) {
                RefAddr refAddr = reference.get(KEY_URL);
                if (isNotEmpty(refAddr)) {
                    setUrl(getStringContent(refAddr));
                }
                refAddr = reference.get(KEY_USER);
                if (isNotEmpty(refAddr)) {
                    setUser(getStringContent(refAddr));
                }
                refAddr = reference.get(KEY_PASSWORD);
                if (isNotEmpty(refAddr)) {
                    setPassword(getStringContent(refAddr));
                }

                trinoConnectionPoolDataSource = this;
            }
        }
        return trinoConnectionPoolDataSource;
    }

    public Driver getDriver()
    {
        return trinoDriver;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
        update(connectionProperties, KEY_USER, url);
    }

    public void setPassword(String password)
    {
        this.password = password;
        update(connectionProperties, KEY_PASSWORD, password);
    }

    public String getUrl()
    {
        return url;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    private void update(Properties properties, String key, String value)
    {
        if (properties != null && key != null) {
            if (value == null) {
                properties.remove(key);
            }
            else {
                properties.setProperty(key, value);
            }
        }
    }

    private boolean isNotEmpty(RefAddr refAddr)
    {
        return refAddr != null && refAddr.getContent() != null;
    }

    private String getStringContent(RefAddr refAddr)
    {
        return refAddr.getContent().toString();
    }
}
