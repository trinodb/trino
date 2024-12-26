package io.trino.adbc;

import io.trino.client.uri.HttpClientFactory;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;

import java.io.IOException;

public class TrinoDatabase implements AdbcDatabase {
    private final TrinoDriverUri uri;
    private final OkHttpClient httpClient;
    private final OkHttpClient segmentClient;

    public TrinoDatabase(TrinoDriverUri uri, OkHttpClient httpClient, OkHttpClient segmentClient) {
        this.uri = uri;//TODO should this be shared at the db level?
        this.httpClient = httpClient;
        this.segmentClient = segmentClient;
    }
    @Override
    public AdbcConnection connect() throws AdbcException {
        return new TrinoConnection(uri, httpClient, segmentClient);
    }

    @Override
    public void close()
            throws IOException
    {

    }
}
