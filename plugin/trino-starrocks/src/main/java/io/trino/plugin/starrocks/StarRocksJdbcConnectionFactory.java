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
package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class StarRocksJdbcConnectionFactory
{
    private final Driver driver;
    private final String jdbcUrl;
    private final Properties connectionProperties;

    @Inject
    public StarRocksJdbcConnectionFactory(StarRocksConfig config)
    {
        this(createDriver(),
                requireNonNull(config, "config is null").getJdbcUrl()
                        .orElseThrow(() -> new IllegalArgumentException("starrocks.jdbc-url must be set")),
                createConnectionProperties(config));
    }

    StarRocksJdbcConnectionFactory(Driver driver, String jdbcUrl, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
    }

    public Connection openConnection()
            throws SQLException
    {
        return openPhysicalConnection();
    }

    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return openConnection();
    }

    private Connection openPhysicalConnection()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.putAll(connectionProperties);

        Connection connection = driver.connect(jdbcUrl, properties);
        if (connection == null) {
            throw new SQLException("StarRocks JDBC driver refused configured URL");
        }
        connection.setReadOnly(true);
        return connection;
    }

    private static Driver createDriver()
    {
        try {
            return new com.starrocks.cj.jdbc.Driver();
        }
        catch (SQLException e) {
            throw jdbcOperationFailed("Failed to initialize StarRocks JDBC driver", e);
        }
    }

    static TrinoException jdbcOperationFailed(String message, SQLException cause)
    {
        return new TrinoException(GENERIC_INTERNAL_ERROR, message, cause);
    }

    private static Properties createConnectionProperties(StarRocksConfig config)
    {
        Properties connectionProperties = new Properties();
        config.getUsername().ifPresent(value -> connectionProperties.setProperty("user", value));
        config.getPassword().ifPresent(value -> connectionProperties.setProperty("password", value));
        return connectionProperties;
    }
}
