package io.trino.adbc;

import io.trino.client.uri.HttpClientFactory;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class TrinoDriver implements AdbcDriver, Closeable {
    public static final String USER_AGENT = "trino-adbc/0.0.1";
    private final Dispatcher dispatcher;
    private final ConnectionPool pool;
    public TrinoDriver() {
        dispatcher = new Dispatcher();
        pool = new ConnectionPool();
    }

    @Override
    public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
        String url = PARAM_URI.get(parameters);
        if (url == null) {
            Object target = parameters.get(AdbcDriver.PARAM_URL);
            if (!(target instanceof String)) {
                throw AdbcException.invalidArgument(
                        "[Trino] Must provide String " + PARAM_URI + " parameter");
            }
            url = (String) target;
        }

        Properties driverProperties = new Properties();
        driverProperties.putAll(parameters);
        TrinoDriverUri uri = null;
        try {
            uri = TrinoDriverUri.createDriverUri(url, driverProperties);
        } catch (SQLException e) {
            throw AdbcException.invalidArgument("failed to create driver uri: " + uri)
                    .withCause(e);
        }
        OkHttpClient.Builder httpClientBuilder = HttpClientFactory.toHttpClientBuilder(uri, USER_AGENT);//TODO agent
        httpClientBuilder.connectionPool(pool);
        httpClientBuilder.dispatcher(dispatcher);
        OkHttpClient httpClient = httpClientBuilder.build();

        OkHttpClient.Builder segmentHttpClientBuilder = HttpClientFactory.unauthenticatedClientBuilder(uri, USER_AGENT);
        segmentHttpClientBuilder.connectionPool(pool);
        segmentHttpClientBuilder.dispatcher(dispatcher);
        OkHttpClient segmentClient = segmentHttpClientBuilder.build();

        return new TrinoDatabase(uri, httpClient, segmentClient);

    }

    @Override
    public void close()
            throws IOException
    {
        // Close dispatcher and pool shared between multiple clients
        dispatcher.executorService().shutdown();
        pool.evictAll();
    }
}
