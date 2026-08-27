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
package io.trino.plugin.resourcegroups.db;

import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class DriverDataSource
        implements DataSource
{
    static DataSource create(String url, String user, String password)
    {
        Driver driver = ServiceLoader.load(Driver.class, DriverDataSource.class.getClassLoader())
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(d -> {
                    try {
                        return d.acceptsURL(url);
                    }
                    catch (SQLException e) {
                        return false;
                    }
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(format("No JDBC driver found for URL: %s", url)));
        return new DriverDataSource(driver, url, user, password);
    }

    private final Driver driver;
    private final String url;
    private final Properties properties = new Properties();

    DriverDataSource(Driver driver, String url, String user, String password)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.url = requireNonNull(url, "url is null");
        if (user != null) {
            properties.setProperty("user", user);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return driver.connect(url, properties);
    }

    @Override
    public Connection getConnection(String username, String password)
            throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrintWriter getLogWriter()
    {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout()
    {
        return 0;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        return driver.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
    {
        return iface.isInstance(this);
    }
}
