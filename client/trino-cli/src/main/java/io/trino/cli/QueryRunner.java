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

import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.uri.TrinoUri;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.trino.client.ClientSession.stripTransactionId;
import static io.trino.client.OkHttpUtil.setupTimeouts;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final Consumer<OkHttpClient.Builder> sslSetup;

    public QueryRunner(
            TrinoUri uri,
            ClientSession session,
            boolean debug,
            HttpLoggingInterceptor.Level networkLogging)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.debug = debug;

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        try {
            sslSetup = uri.getSetupSsl();
            uri.setupClient(builder);
        }
        catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }

        setupTimeouts(builder, 30, SECONDS);
        builder.addNetworkInterceptor(
                new HttpLoggingInterceptor(System.err::println)
                        .setLevel(networkLogging));

        this.httpClient = builder.build();
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(requireNonNull(session, "session is null"));
    }

    public boolean isDebug()
    {
        return debug;
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(session.get(), query), debug);
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        OkHttpClient.Builder builder = httpClient.newBuilder();
        sslSetup.accept(builder);
        OkHttpClient client = builder.build();

        return newStatementClient((Call.Factory) client, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
