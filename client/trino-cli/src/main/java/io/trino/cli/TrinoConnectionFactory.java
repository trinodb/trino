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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.jdbc.NonRegisteringTrinoDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static io.trino.cli.TrinoConnectionProperties.ENCODING;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class TrinoConnectionFactory
        implements ConnectionFactory
{
    // A dedicated driver instance, so closing it releases the HTTP threads it created
    private final NonRegisteringTrinoDriver driver = new NonRegisteringTrinoDriver("trino-cli") {};
    private final String url;
    private final Properties properties;
    private final Duration requestTimeout;

    public TrinoConnectionFactory(String url, Properties properties, Duration requestTimeout)
    {
        this.url = requireNonNull(url, "url is null");
        this.properties = new Properties();
        this.properties.putAll(requireNonNull(properties, "properties is null"));
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
    }

    /**
     * Returns the encodings of the spooling protocol supported by the driver, in the order of preference.
     */
    public static List<String> getSupportedEncodings()
    {
        try (NonRegisteringTrinoDriver driver = new NonRegisteringTrinoDriver() {}) {
            for (DriverPropertyInfo property : driver.getPropertyInfo("jdbc:trino://localhost", new Properties())) {
                if (property.name.equals(ENCODING) && property.choices != null) {
                    return ImmutableList.copyOf(property.choices);
                }
            }
            return ImmutableList.of();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Connection connection = driver.connect(url, properties);
        // the driver uses its network timeout as the client request timeout
        connection.setNetworkTimeout(Runnable::run, toIntExact(requestTimeout.toMillis()));
        return connection;
    }

    @Override
    public void close()
    {
        try {
            driver.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
