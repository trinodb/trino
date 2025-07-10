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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TrinoJsonCodec;
import io.trino.client.TypedQueryData;
import io.trino.client.uri.PropertyName;
import io.trino.client.uri.TrinoUri;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.PrintStream;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Properties;

import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.SET_COOKIE;
import static io.trino.cli.ClientOptions.OutputFormat.CSV;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.client.auth.external.ExternalRedirectStrategy.PRINT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestQueryRunner
{
    private static final TrinoJsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @StartStop
    private final MockWebServer server = new MockWebServer();

    @Test
    public void testCookie()
            throws Exception
    {
        server.enqueue(new MockResponse.Builder()
                .code(307)
                .addHeader(LOCATION, server.url("/v1/statement"))
                .addHeader(SET_COOKIE, "a=apple")
                .build());
        server.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(createResults(server))
                .build());
        server.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(createResults(server))
                .build());

        QueryRunner queryRunner = createQueryRunner(createTrinoUri(server, false), createClientSession(server));

        try (Query query = queryRunner.startQuery("first query will introduce a cookie")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false, false);
        }
        try (Query query = queryRunner.startQuery("second query should carry the cookie")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false, false);
        }

        assertThat(server.takeRequest().getHeaders().get("Cookie")).isNull();
        assertThat(server.takeRequest().getHeaders().get("Cookie")).isEqualTo("a=apple");
        assertThat(server.takeRequest().getHeaders().get("Cookie")).isEqualTo("a=apple");
    }

    static TrinoUri createTrinoUri(MockWebServer server, boolean insecureSsl)
    {
        Properties properties = new Properties();
        properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.toString(), PRINT.name());
        properties.setProperty(PropertyName.SSL.toString(), Boolean.toString(!insecureSsl));
        return TrinoUri.create(server.url("/").uri(), properties);
    }

    static ClientSession createClientSession(MockWebServer server)
    {
        return ClientSession.builder()
                .server(server.url("/").uri())
                .user(Optional.of("user"))
                .source("source")
                .clientInfo("clientInfo")
                .catalog("catalog")
                .schema("schema")
                .timeZone(ZoneId.of("America/Los_Angeles"))
                .locale(Locale.ENGLISH)
                .transactionId(null)
                .clientRequestTimeout(new Duration(2, MINUTES))
                .compressionDisabled(true)
                .build();
    }

    static String createResults(MockWebServer server)
    {
        QueryResults queryResults = new QueryResults(
                "20160128_214710_00012_rk68b",
                server.url("/query.html?20160128_214710_00012_rk68b").uri(),
                null,
                null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                TypedQueryData.of(ImmutableList.of(ImmutableList.of(123))),
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                //new StatementStats("FINISHED", false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.empty());
        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    static QueryRunner createQueryRunner(TrinoUri uri, ClientSession clientSession)
    {
        return new QueryRunner(
                uri,
                clientSession,
                false,
                1000,
                500);
    }

    static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
