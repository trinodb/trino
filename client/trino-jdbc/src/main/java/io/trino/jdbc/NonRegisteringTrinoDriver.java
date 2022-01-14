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

import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.trino.client.OkHttpUtil.userAgent;
import static io.trino.jdbc.DriverInfo.DRIVER_NAME;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MINOR;

public class NonRegisteringTrinoDriver
        implements Driver, Closeable
{
    private final OkHttpClient httpClient = newHttpClient();

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        TrinoDriverUri uri = TrinoDriverUri.create(url, info);

        OkHttpClient.Builder builder = httpClient.newBuilder();
        uri.setupClient(builder);

        return new TrinoConnection(uri, builder.build());
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        return TrinoDriverUri.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        Properties properties = urlProperties(url, info);

        return ConnectionProperties.allProperties().stream()
                .filter(property -> property.isAllowed(properties))
                .map(property -> property.getDriverPropertyInfo(properties))
                .toArray(DriverPropertyInfo[]::new);
    }

    private static Properties urlProperties(String url, Properties info)
    {
        try {
            return TrinoDriverUri.create(url, info).getProperties();
        }
        catch (SQLException e) {
            return info;
        }
    }

    @Override
    public int getMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }

    private static OkHttpClient newHttpClient()
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .addInterceptor(userAgent(DRIVER_NAME + "/" + DRIVER_VERSION));
        return builder.build();
    }
}
