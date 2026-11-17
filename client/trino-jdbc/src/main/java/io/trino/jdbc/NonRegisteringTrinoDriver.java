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

import io.trino.client.uri.HttpClientFactory;
import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static io.trino.client.UserAgentBuilder.createUserAgent;
import static io.trino.jdbc.DriverInfo.DRIVER_NAME;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MINOR;
import static java.util.Objects.requireNonNull;

public class NonRegisteringTrinoDriver
        implements Driver, Closeable
{
    private static final String USER_AGENT = createUserAgent(DRIVER_NAME, DRIVER_VERSION);

    private final String userAgent;
    private final Dispatcher dispatcher = new Dispatcher();
    private final ConnectionPool pool = new ConnectionPool();

    protected NonRegisteringTrinoDriver()
    {
        this.userAgent = USER_AGENT;
    }

    /**
     * Allows applications embedding the driver to identify themselves to the server,
     * using the given product name instead of the driver name in the user agent.
     */
    protected NonRegisteringTrinoDriver(String product)
    {
        this.userAgent = createUserAgent(requireNonNull(product, "product is null"));
    }

    @Override
    public void close()
            throws IOException
    {
        // Close dispatcher and pool shared between multiple clients
        dispatcher.executorService().shutdown();
        pool.evictAll();
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        try {
            TrinoDriverUri uri = TrinoDriverUri.createDriverUri(url, info);
            OkHttpClient.Builder httpClientBuilder = HttpClientFactory.toHttpClientBuilder(uri, userAgent);
            httpClientBuilder.connectionPool(pool);
            httpClientBuilder.dispatcher(dispatcher);

            OkHttpClient.Builder segmentHttpClientBuilder = HttpClientFactory.unauthenticatedClientBuilder(uri, userAgent);
            segmentHttpClientBuilder.connectionPool(pool);
            segmentHttpClientBuilder.dispatcher(dispatcher);

            return new TrinoConnection(
                    uri,
                    wrapClient(httpClientBuilder.build()),
                    wrapClient(segmentHttpClientBuilder.build()));
        }
        catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    protected Call.Factory wrapClient(OkHttpClient client)
    {
        return client;
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
    {
        return TrinoDriverUri.getPropertyInfo(url, info);
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
}
