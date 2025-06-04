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

import io.trino.client.uri.ConnectionProperty;
import io.trino.client.uri.TrinoUri;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Parses and extracts parameters from a Trino JDBC URL.
 */
public final class TrinoDriverUri
        extends TrinoUri
{
    private static final String JDBC_URL_PREFIX = "jdbc:";
    private static final String JDBC_URL_START = JDBC_URL_PREFIX + "trino:";

    private TrinoDriverUri(String uri, Properties driverProperties)
            throws SQLException
    {
        super(parseDriverUrl(uri), driverProperties);
    }

    static TrinoDriverUri createDriverUri(String url, Properties properties)
            throws SQLException
    {
        try {
            return new TrinoDriverUri(url, firstNonNull(properties, new Properties()));
        }
        catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e.getCause());
        }
    }

    public static boolean acceptsURL(String url)
    {
        return url.startsWith(JDBC_URL_START);
    }

    private static URI parseDriverUrl(String url)
            throws SQLException
    {
        validatePrefix(url);
        URI uri = parseUrl(url);

        if (isNullOrEmpty(uri.getHost())) {
            throw new SQLException("No host specified: " + url);
        }
        if (uri.getPort() == 0 || uri.getPort() > 65535) {
            throw new SQLException("Invalid port number: " + url);
        }
        return uri;
    }

    private static URI parseUrl(String url)
            throws SQLException
    {
        try {
            return new URI(url.substring(JDBC_URL_PREFIX.length()));
        }
        catch (URISyntaxException e) {
            throw new SQLException("Invalid JDBC URL: " + url, e);
        }
    }

    private static void validatePrefix(String url)
            throws SQLException
    {
        if (!url.startsWith(JDBC_URL_START)) {
            throw new SQLException("Invalid JDBC URL: " + url);
        }

        if (url.equals(JDBC_URL_START)) {
            throw new SQLException("Empty JDBC URL: " + url);
        }
    }

    public static DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
    {
        Properties properties = urlProperties(url, info);
        return allProperties().stream()
                .filter(property -> property.isValid(properties))
                .map(property -> getDriverPropertyInfo(property, properties))
                .toArray(DriverPropertyInfo[]::new);
    }

    /*
     * Combines properties extracted from url with provided ones
     */
    private static Properties urlProperties(String url, Properties info)
    {
        try {
            return create(url, info).getProperties();
        }
        catch (RuntimeException e) {
            return info;
        }
    }

    private static DriverPropertyInfo getDriverPropertyInfo(ConnectionProperty<?, ?> property, Properties properties)
    {
        String currentValue = properties.getProperty(property.getKey());
        DriverPropertyInfo result = new DriverPropertyInfo(property.getKey(), currentValue);
        result.required = property.isRequired(properties);
        result.choices = property.getChoices();
        property.getValue(properties).ifPresent(value -> result.value = value.toString());
        return result;
    }
}
