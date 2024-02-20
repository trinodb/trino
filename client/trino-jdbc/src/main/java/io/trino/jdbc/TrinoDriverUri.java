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

import io.trino.client.uri.TrinoUri;

import java.net.URI;
import java.net.URISyntaxException;
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

    public static TrinoDriverUri create(String url, Properties properties)
            throws SQLException
    {
        return new TrinoDriverUri(url, firstNonNull(properties, new Properties()));
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
        if (uri.getPort() == -1) {
            throw new SQLException("No port number specified: " + url);
        }
        if ((uri.getPort() < 1) || (uri.getPort() > 65535)) {
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
}
