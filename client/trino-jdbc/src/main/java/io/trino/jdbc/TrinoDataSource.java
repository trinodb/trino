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
import io.trino.client.uri.PropertyName;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private String url;
    private PrintWriter printWriter;

    private final Properties connectionProperties = new Properties();
    private final TrinoDriver trinoDriver;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public TrinoDataSource()
    {
        trinoDriver = new TrinoDriver();
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return trinoDriver.connect(url, connectionProperties);
    }

    @Override
    public Connection getConnection(String user, String password)
            throws SQLException
    {
        Properties properties = new Properties();
        try (var ignored = withLock(lock.readLock())) {
            properties.putAll(this.connectionProperties);
        }
        catch (Exception e) {
            throw new SQLException("Failed to get connection", e);
        }
        if (user != null) {
            properties.put(USER.toString(), user);
        }
        if (password != null) {
            properties.put(PASSWORD.toString(), password);
        }
        return trinoDriver.connect(url, properties);
    }

    @Override
    public PrintWriter getLogWriter()
            throws SQLException
    {
        try (var ignored = withLock(lock.readLock())) {
            return printWriter;
        }
        catch (Exception e) {
            throw new SQLException("Failed to get log writer", e);
        }
    }

    @Override
    public void setLogWriter(PrintWriter out)
            throws SQLException
    {
        try (var ignored = withLock(lock.writeLock())) {
            this.printWriter = out;
        }
        catch (Exception e) {
            throw new SQLException("Failed to set log writer", e);
        }
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
        return trinoDriver;
    }

    public DataSource setUrl(String url)
            throws SQLException
    {
        try (var ignored = withLock(lock.writeLock())) {
            this.url = url;
            return this;
        }
        catch (Exception e) {
            throw new SQLException("Failed to set url", e);
        }
    }

    public String getUrl()
            throws Exception
    {
        try (var ignored = withLock(lock.writeLock())) {
            return url;
        }
        catch (Exception e) {
            throw new SQLException("Failed to get url", e);
        }
    }

    public Properties getConnectionProperties()
            throws SQLException
    {
        try (var ignored = withLock(lock.writeLock())) {
            return connectionProperties;
        }
        catch (Exception e) {
            throw new SQLException("Failed to get connection properties");
        }
    }

    public void setConnectionProperties(Properties properties)
            throws SQLException
    {
        try (var ignored = withLock(lock.writeLock())) {
            connectionProperties.clear();
            connectionProperties.putAll(properties);
        }
        catch (Exception e) {
            throw new SQLException("Failed to set connection properties", e);
        }
    }

    public TrinoDataSource setUser(String user)
            throws SQLException
    {
        update(connectionProperties, USER, user);
        return this;
    }

    public String getUser()
            throws SQLException
    {
        return get(connectionProperties, USER);
    }

    public TrinoDataSource setPassword(String password)
            throws SQLException
    {
        update(connectionProperties, PASSWORD, password);
        return this;
    }

    public String getPassword()
            throws SQLException
    {
        return get(connectionProperties, PASSWORD);
    }

    public TrinoDataSource setCatalog(String catalog)
            throws SQLException
    {
        update(connectionProperties, CATALOG, catalog);
        return this;
    }

    public String getCatalog()
            throws SQLException
    {
        return get(connectionProperties, CATALOG);
    }

    public TrinoDataSource setSchema(String schema)
            throws SQLException
    {
        update(connectionProperties, SCHEMA, schema);
        return this;
    }

    public String getSchema()
            throws SQLException
    {
        return get(connectionProperties, CATALOG);
    }

    public TrinoDataSource setSessionUser(String sessionUser)
            throws SQLException
    {
        update(connectionProperties, SESSION_USER, sessionUser);
        return this;
    }

    public String getSessionUser()
            throws SQLException
    {
        return get(connectionProperties, SESSION_USER);
    }

    public TrinoDataSource setSqlPath(String sqlPath)
            throws SQLException
    {
        update(connectionProperties, SQL_PATH, sqlPath);
        return this;
    }

    public String getSqlPath()
            throws SQLException
    {
        return get(connectionProperties, SQL_PATH);
    }

    private void update(Properties properties, PropertyName key, String value)
            throws SQLException
    {
        try (var ignored = withLock(lock.writeLock())) {
            if (value == null) {
                properties.remove(key);
            }
            else {
                properties.setProperty(key.toString(), value);
            }
        }
        catch (Exception e) {
            throw new SQLException("Failed to set connection property " + key);
        }
    }

    private String get(Properties properties, PropertyName key)
            throws SQLException
    {
        try (var ignored = withLock(lock.readLock())) {
            if (properties == null) {
                return null;
            }
            return properties.getProperty(key.toString());
        }
        catch (Exception e) {
            throw new SQLException("Failed to get connection property " + key);
        }
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

    private static Closeable withLock(Lock lock)
    {
        lock.lock();
        return lock::unlock;
    }
}
