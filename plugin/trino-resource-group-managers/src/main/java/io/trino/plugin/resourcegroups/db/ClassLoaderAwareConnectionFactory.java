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

import org.jdbi.v3.core.ConnectionFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ServiceLoader;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class ClassLoaderAwareConnectionFactory
        implements ConnectionFactory
{
    static ConnectionFactory create(String url, String user, String password)
    {
        Driver driver = ServiceLoader.load(Driver.class, ClassLoaderAwareConnectionFactory.class.getClassLoader())
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
        return new ClassLoaderAwareConnectionFactory(driver, url, user, password);
    }

    private final Driver driver;
    private final String url;
    private final Properties properties = new Properties();

    private ClassLoaderAwareConnectionFactory(Driver driver, String url, String user, String password)
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
    public Connection openConnection()
            throws SQLException
    {
        return driver.connect(url, properties);
    }
}
