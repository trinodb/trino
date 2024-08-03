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
package io.trino.loki;

import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpUriBuilder;
import io.trino.spi.TrinoException;
import jakarta.inject.Inject;
import okhttp3.*;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.loki.LokiErrorCode.LOKI_UNKNOWN_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class LokiClient {

    private final OkHttpClient httpClient;
    private final URI lokiEndpoint;

    @Inject
    public LokiClient(LokiConnectorConfig config) {
        this.lokiEndpoint = config.getLokiURI();

        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder().readTimeout(Duration.ofMillis(config.getReadTimeout().toMillis()));
        setupBasicAuth(clientBuilder, config.getUser(), config.getPassword());
        this.httpClient = clientBuilder.build();
    }

    private static void setupBasicAuth(OkHttpClient.Builder clientBuilder, Optional<String> user, Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }

    private static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        if (user.contains(":")) {
            throw new TrinoException(GENERIC_USER_ERROR, "Illegal character ':' found in username");
        }

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    // TODO: execute query
    public QueryResult doQuery(String lokiQuery, Long start, Long end) {
        final URI uri =
                HttpUriBuilder.uriBuilderFrom(lokiEndpoint)
                        .appendPath("/loki/api/v1/query_range")
                        .addParameter("query", lokiQuery)
                        .addParameter("start", start.toString())
                        .addParameter("end", end.toString())
                        .addParameter("direction", "forward")
                        .build();

        try (Response response = requestUri(uri)) {
            if (response.isSuccessful() && response.body() != null) {
                return QueryResult.fromJSON(response.body().byteStream());
            }
            throw new TrinoException(LOKI_UNKNOWN_ERROR, "Bad response " + response.code() + " " + response.message());
        }
        catch (IOException e) {
            throw new TrinoException(LOKI_UNKNOWN_ERROR, "Error reading metrics", e);
        }
    }

    // TODO: do we need this?
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        String[] tables = {"this_client_does_not", "have_tables_as", "you_need_to_specify_a_table_function"};
        return ImmutableSet.copyOf(tables);
    }

    public Response requestUri(URI uri) throws IOException
    {
        Request.Builder requestBuilder = new Request.Builder().url(uri.toString());
        return httpClient.newCall(requestBuilder.build()).execute();
    }
}
