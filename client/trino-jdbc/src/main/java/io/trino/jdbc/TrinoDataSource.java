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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.client.uri.PropertyName;
import jakarta.annotation.Nullable;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.trino.client.uri.PropertyName.CATALOG;
import static io.trino.client.uri.PropertyName.PASSWORD;
import static io.trino.client.uri.PropertyName.SCHEMA;
import static io.trino.client.uri.PropertyName.SESSION_USER;
import static io.trino.client.uri.PropertyName.SQL_PATH;
import static io.trino.client.uri.PropertyName.USER;
import static java.lang.String.format;

public class TrinoDataSource
        implements DataSource
{
    @GuardedBy("this")
    private String url;
    @GuardedBy("this")
    private PrintWriter printWriter;
    @GuardedBy("this")
    private final Properties connectionProperties = new Properties();

    private final Driver driver;

    public TrinoDataSource()
    {
        driver = new NonRegisteringTrinoDriver();
    }

    @Override
    public synchronized Connection getConnection()
            throws SQLException
    {
        return driver.connect(url, connectionProperties);
    }

    @Override
    public synchronized Connection getConnection(String user, String password)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.putAll(this.connectionProperties);

        if (user != null) {
            properties.put(USER.toString(), user);
        }
        if (password != null) {
            properties.put(PASSWORD.toString(), password);
        }
        return driver.connect(url, properties);
    }

    @Override
    public synchronized PrintWriter getLogWriter()
    {
        return printWriter;
    }

    @Override
    public synchronized void setLogWriter(PrintWriter out)
    {
        this.printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds)
    {
        throw new UnsupportedOperationException("setLoginTimeout is not supported");
    }

    @Override
    public int getLoginTimeout()
    {
        return 0;
    }

    @Override
    public Logger getParentLogger()
    {
        return Logger.getLogger(TrinoDriver.class.getPackage().getName());
    }

    @VisibleForTesting
    Driver getDriver()
    {
        return driver;
    }

    public synchronized DataSource setUrl(String url)
    {
        this.url = url;
        return this;
    }

    public synchronized String getUrl()
    {
        return url;
    }

    public synchronized Properties getConnectionProperties()
    {
        return connectionProperties;
    }

    public synchronized void setConnectionProperties(Properties properties)
    {
        connectionProperties.clear();
        connectionProperties.putAll(properties);
    }

    public TrinoDataSource setUser(@Nullable String user)
    {
        update(USER, user);
        return this;
    }

    public String getUser()
    {
        return get(USER);
    }

    public TrinoDataSource setPassword(@Nullable String password)
    {
        update(PASSWORD, password);
        return this;
    }

    public String getPassword()
    {
        return get(PASSWORD);
    }

    public TrinoDataSource setCatalog(@Nullable String catalog)
    {
        update(CATALOG, catalog);
        return this;
    }

    public String getCatalog()
    {
        return get(CATALOG);
    }

    public TrinoDataSource setSchema(@Nullable String schema)
    {
        update(SCHEMA, schema);
        return this;
    }

    public String getSchema()
    {
        return get(CATALOG);
    }

    public TrinoDataSource setSessionUser(@Nullable String sessionUser)
    {
        update(SESSION_USER, sessionUser);
        return this;
    }

    public String getSessionUser()
    {
        return get(SESSION_USER);
    }

    public TrinoDataSource setSqlPath(@Nullable String sqlPath)
    {
        update(SQL_PATH, sqlPath);
        return this;
    }

    public String getSqlPath()
    {
        return get(SQL_PATH);
    }

    private synchronized void update(PropertyName key, String value)
    {
        if (value == null) {
            connectionProperties.remove(key);
        }
        else {
            connectionProperties.setProperty(key.toString(), value);
        }
    }

    private synchronized String get(PropertyName key)
    {
        if (connectionProperties == null) {
            return null;
        }
        return connectionProperties.getProperty(key.toString());
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException(format("DataSource of type '%s' cannot be unwrapped to '%s'", getClass().getName(), iface.getName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
    {
        return iface.isInstance(this);
    }
}
