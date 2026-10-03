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
package io.trino.plugin.session.db;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

/**
 * JDBC connection properties shared by the DAO and the Flyway migration.
 */
final class JdbcConnectionProperties
{
    private static final String CONNECT_TIMEOUT = "connectTimeout";
    private static final String SOCKET_TIMEOUT = "socketTimeout";

    private JdbcConnectionProperties() {}

    /**
     * Credentials are only set when configured, so credentials embedded in the JDBC URL and passwordless accounts keep working.
     */
    static Properties forJdbi(DbSessionPropertyManagerConfig config)
    {
        Properties properties = new Properties();
        if (config.getConfigDbUser() != null) {
            properties.setProperty("user", config.getConfigDbUser());
        }
        if (config.getConfigDbPassword() != null) {
            properties.setProperty("password", config.getConfigDbPassword());
        }
        properties.putAll(timeouts(config));
        return properties;
    }

    /**
     * Connect and socket read timeouts in the unit each driver expects: seconds for PgJDBC, milliseconds for MySQL Connector/J.
     * A timeout that the JDBC URL already sets is left out, so the URL value applies.
     */
    static Map<String, String> timeouts(DbSessionPropertyManagerConfig config)
    {
        String url = requireNonNull(config.getConfigDbUrl(), "session-property-manager.config-db-url is not set");
        TimeUnit unit = driverTimeoutUnit(url);
        ImmutableMap.Builder<String, String> timeouts = ImmutableMap.builder();
        if (!urlSetsProperty(url, CONNECT_TIMEOUT)) {
            timeouts.put(CONNECT_TIMEOUT, toDriverValue(config.getConnectTimeout(), unit));
        }
        if (!urlSetsProperty(url, SOCKET_TIMEOUT)) {
            timeouts.put(SOCKET_TIMEOUT, toDriverValue(config.getSocketTimeout(), unit));
        }
        return timeouts.buildOrThrow();
    }

    private static TimeUnit driverTimeoutUnit(String url)
    {
        if (url.startsWith("jdbc:postgresql")) {
            return SECONDS;
        }
        if (url.startsWith("jdbc:mysql")) {
            return MILLISECONDS;
        }
        throw new IllegalArgumentException(format("Invalid JDBC URL: %s. Only PostgreSQL and MySQL are supported.", url));
    }

    private static boolean urlSetsProperty(String url, String property)
    {
        return Pattern.compile("[?&]" + property + "=", CASE_INSENSITIVE).matcher(url).find();
    }

    private static String toDriverValue(Duration timeout, TimeUnit unit)
    {
        return String.valueOf((long) Math.ceil(timeout.getValue(unit)));
    }
}
