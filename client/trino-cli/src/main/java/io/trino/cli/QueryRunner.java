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
import io.trino.client.uri.HttpClientFactory;
import io.trino.client.uri.TrinoUri;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.client.ClientSession.stripTransactionId;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.Objects.requireNonNull;

public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final OkHttpClient segmentHttpClient;

    public QueryRunner(TrinoUri uri, ClientSession session, boolean debug)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.httpClient = HttpClientFactory.toHttpClientBuilder(uri, session.getSource()).build();
        this.segmentHttpClient = HttpClientFactory
                .unauthenticatedClientBuilder(uri, session.getSource())
                .build();
        this.debug = debug;
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
        return newStatementClient(httpClient, segmentHttpClient, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
