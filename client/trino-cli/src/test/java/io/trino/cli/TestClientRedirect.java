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
import io.airlift.json.JsonCodec;
import io.trino.client.ClientException;
import io.trino.client.ClientSession;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.uri.PropertyName;
import io.trino.client.uri.TrinoUri;
import okhttp3.Credentials;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Properties;

import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.LOCATION;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.cli.ClientOptions.OutputFormat.CSV;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TestQueryRunner.createClientSession;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestClientRedirect
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final String DUMMY_ACCESS_TOKEN = Base64.getEncoder().encodeToString("jwt-signing-key".getBytes(US_ASCII));
    private static final String DUMMY_USER = "DUMMY_USER";
    private static final String DUMMY_PASSWORD = "DUMMY_PASSWORD";
    private static final String DUMMY_BASIC_AUTH_TOKEN = Credentials.basic(DUMMY_USER, DUMMY_PASSWORD);
    private static final String REDIRECT_EXCEPTION = "Client redirection was detected, but 'disableFollowRedirects' flag is set to true";
    private MockWebServer proxyServer;
    private MockWebServer server;

    @BeforeEach
    public void setup()
            throws IOException
    {
        proxyServer = new MockWebServer();
        proxyServer.start();
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    public void teardown()
            throws IOException
    {
        proxyServer.close();
        proxyServer = null;
        server.close();
        server = null;
    }

    @Test
    public void testDisableClientRedirectsDefaultWithAccessToken()
            throws Exception
    {
        assertRedirectIsFollowedWithAuthorizationHeader(Optional.empty(), false);
    }

    @Test
    public void testClientRedirectEnabledWithAccessToken()
            throws Exception
    {
        assertRedirectIsFollowedWithAuthorizationHeader(Optional.of(false), false);
    }

    @Test
    public void testClientRedirectDisabledWithAccessToken()
    {
        assertRedirectIsNotFollowed(false);
    }

    @Test
    public void testDisableClientRedirectsDefaultWithBasicAuth()
            throws Exception
    {
        assertRedirectIsFollowedWithAuthorizationHeader(Optional.empty(), true);
    }

    @Test
    public void testClientRedirectEnabledWithBasicAuth()
            throws Exception
    {
        assertRedirectIsFollowedWithAuthorizationHeader(Optional.of(false), true);
    }

    @Test
    public void testClientRedirectDisabledWithBasicAuth()
    {
        assertRedirectIsNotFollowed(true);
    }

    @Test
    public void testClientRedirectDisabledExternalAuth()
            throws Exception
    {
        proxyServer.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("redirect1/v1/statement")));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        try {
            QueryRunner queryRunner = createQueryRunner(createTrinoUri(proxyServer, Optional.of(true), Optional.empty(),
                    Optional.empty(), Optional.of(true)), createClientSession(proxyServer));

            try (Query query = queryRunner.startQuery("SELECT 1")) {
                query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
            }
        }
        catch (Exception e) {
            assertThat(e).isInstanceOf(ClientException.class);
            assertThat(e.getMessage()).isEqualTo(REDIRECT_EXCEPTION);
        }
    }

    @Test
    public void testClientRedirectsMultipleHop()
            throws Exception
    {
        MockWebServer server2 = new MockWebServer();
        server2.start();

        proxyServer.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server2.url("redirect1/v1/statement"))
                .addHeader(AUTHORIZATION, DUMMY_ACCESS_TOKEN));
        server2.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("redirect2/v1/statement")));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        QueryRunner queryRunner = createQueryRunner(createTrinoUri(proxyServer, Optional.empty(),
                Optional.empty(), Optional.of(true), Optional.empty()), createClientSession(proxyServer));

        try (Query query = queryRunner.startQuery("SELECT 1")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
        }

        //verify the redirect request has the same Authorization header
        RecordedRequest redirectedRequest = server.takeRequest();
        assertThat(redirectedRequest.getHeader(AUTHORIZATION)).isEqualTo("Bearer " + DUMMY_ACCESS_TOKEN);

        server2.close();
    }

    private void assertRedirectIsFollowedWithAuthorizationHeader(Optional<Boolean> disableFollowRedirects, boolean isBasicAuth)
            throws Exception
    {
        proxyServer.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("redirect/v1/statement"))
                .addHeader(AUTHORIZATION, isBasicAuth ? DUMMY_BASIC_AUTH_TOKEN : DUMMY_ACCESS_TOKEN));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        QueryRunner queryRunner;
        if (isBasicAuth) {
            queryRunner = createQueryRunner(createTrinoUri(proxyServer, disableFollowRedirects, Optional.of(true),
                    Optional.empty(), Optional.empty()), createClientSession(proxyServer));
        }
        else {
            queryRunner = createQueryRunner(createTrinoUri(proxyServer, disableFollowRedirects, Optional.empty(),
                    Optional.of(true), Optional.empty()), createClientSession(proxyServer));
        }

        try (Query query = queryRunner.startQuery("SELECT 1")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
        }

        //verify the redirect request has the same Authorization header
        RecordedRequest redirectedRequest = server.takeRequest();
        if (isBasicAuth) {
            assertThat(redirectedRequest.getHeader(AUTHORIZATION)).isEqualTo(DUMMY_BASIC_AUTH_TOKEN);
        }
        else {
            assertThat(redirectedRequest.getHeader(AUTHORIZATION)).isEqualTo("Bearer " + DUMMY_ACCESS_TOKEN);
        }
    }

    private void assertRedirectIsNotFollowed(boolean isBasicAuth)
    {
        proxyServer.enqueue(new MockResponse()
                .setResponseCode(307)
                .addHeader(LOCATION, server.url("redirect/v1/statement"))
                .addHeader(AUTHORIZATION, isBasicAuth ? DUMMY_BASIC_AUTH_TOKEN : DUMMY_ACCESS_TOKEN));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));
        try {
            QueryRunner queryRunner;
            if (isBasicAuth) {
                queryRunner = createQueryRunner(createTrinoUri(proxyServer, Optional.of(true), Optional.of(true),
                        Optional.empty(), Optional.empty()), createClientSession(proxyServer));
            }
            else {
                queryRunner = createQueryRunner(createTrinoUri(proxyServer, Optional.of(true),
                        Optional.empty(), Optional.of(true), Optional.empty()), createClientSession(proxyServer));
            }

            try (Query query = queryRunner.startQuery("SELECT 1")) {
                query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
            }
        }
        catch (Exception e) {
            assertThat(e).isInstanceOf(ClientException.class);
            assertThat(e.getMessage()).isEqualTo(REDIRECT_EXCEPTION);
        }
    }

    private static TrinoUri createTrinoUriExternalAuth(MockWebServer server, Optional<Boolean> disableFollowRedirects)
            throws SQLException
    {
        Properties properties = new Properties();
        if (disableFollowRedirects.isPresent()) {
            properties.setProperty(PropertyName.DISABLE_FOLLOW_REDIRECTS.toString(), Boolean.toString(disableFollowRedirects.get()));
        }
        properties.setProperty(PropertyName.SSL.toString(), Boolean.toString(true));
        properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION.toString(), Boolean.toString(true));
        return TrinoUri.create(server.url("/").uri(), properties);
    }

    private static TrinoUri createTrinoUri(MockWebServer server, Optional<Boolean> disableFollowRedirects,
                                    Optional<Boolean> isBasicAuth, Optional<Boolean> isAccessToken,
                                    Optional<Boolean> isExternalAuth)
            throws SQLException
    {
        Properties properties = new Properties();
        if (disableFollowRedirects.isPresent()) {
            properties.setProperty(PropertyName.DISABLE_FOLLOW_REDIRECTS.toString(), Boolean.toString(disableFollowRedirects.get()));
        }
        if (isBasicAuth.isPresent()) {
            properties.setProperty(PropertyName.USER.toString(), DUMMY_USER);
            properties.setProperty(PropertyName.PASSWORD.toString(), DUMMY_PASSWORD);
        }
        else if (isAccessToken.isPresent()) {
            properties.setProperty(PropertyName.ACCESS_TOKEN.toString(), DUMMY_ACCESS_TOKEN);
        }
        else if (isExternalAuth.isPresent()) {
            properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION.toString(), Boolean.toString(true));
        }
        properties.setProperty(PropertyName.SSL.toString(), Boolean.toString(true));
        return TrinoUri.create(server.url("/").uri(), properties);
    }

    private static String createResults(MockWebServer server)
    {
        QueryResults queryResults = new QueryResults(
                "20240327_104103_00010_pu6b8",
                server.url("/query.html?20240327_104103_00010_pu6b8").uri(),
                null,
                null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                ImmutableList.of(ImmutableList.of(1)),
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                null);
        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    private static QueryRunner createQueryRunner(TrinoUri uri, ClientSession clientSession)
    {
        return new QueryRunner(
                uri,
                clientSession,
                false,
                HttpLoggingInterceptor.Level.NONE);
    }

    private static PrintStream nullPrintStream()
    {
        return new PrintStream(nullOutputStream());
    }
}
