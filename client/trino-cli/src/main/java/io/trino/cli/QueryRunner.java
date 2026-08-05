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

import com.google.common.annotations.VisibleForTesting;
import io.trino.client.ClientCapabilities;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.uri.HttpClientFactory;
import io.trino.client.uri.TrinoUri;
import okhttp3.Cache;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.client.ClientSession.stripTransactionId;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.client.UserAgentBuilder.createUserAgent;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class QueryRunner
        implements Closeable
{
    private static final String USER_AGENT = createUserAgent("trino-cli");
    private static final Set<String> CLIENT_CAPABILITIES = stream(ClientCapabilities.values())
            .filter(capability -> capability != ClientCapabilities.VARIANT_BINARY)
            .map(Enum::name)
            .collect(toUnmodifiableSet());

    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final OkHttpClient segmentHttpClient;
    private final int maxQueuedRows;
    private final int maxBufferedRows;
    private final Theme theme;

    public QueryRunner(Theme theme, TrinoUri uri, ClientSession session, boolean debug, int maxQueuedRows, int maxBufferedRows)
    {
        this(theme,
                session,
                debug,
                HttpClientFactory.toHttpClientBuilder(uri, USER_AGENT).build(),
                HttpClientFactory.unauthenticatedClientBuilder(uri, USER_AGENT).build(),
                maxQueuedRows,
                maxBufferedRows);
    }

    @VisibleForTesting
    QueryRunner(
            Theme theme,
            ClientSession session,
            boolean debug,
            OkHttpClient httpClient,
            OkHttpClient segmentHttpClient,
            int maxQueuedRows,
            int maxBufferedRows)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.segmentHttpClient = requireNonNull(segmentHttpClient, "segmentHttpClient is null");
        this.debug = debug;
        this.maxQueuedRows = maxQueuedRows;
        this.maxBufferedRows = maxBufferedRows;
        this.theme = requireNonNull(theme, "theme is null");
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

    public Theme theme()
    {
        return theme;
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(session.get(), query), debug, maxQueuedRows, maxBufferedRows, theme);
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        return newStatementClient(httpClient, segmentHttpClient, session, query, Optional.of(CLIENT_CAPABILITIES));
    }

    @Override
    public void close()
    {
        closeAll(httpClient, segmentHttpClient);
    }

    private static void closeAll(OkHttpClient... clients)
    {
        RuntimeException failure = null;
        for (OkHttpClient client : clients) {
            try {
                closeClient(client);
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeClient(OkHttpClient client)
    {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
        Cache cache = client.cache();
        if (cache != null) {
            try {
                cache.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
