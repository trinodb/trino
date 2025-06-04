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
package io.trino.tests;

import com.google.common.base.Splitter;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.client.StatementClient;
import io.trino.client.StatementClientFactory;
import io.trino.client.uri.TrinoUri;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.testing.TestingTrinoClient;
import okhttp3.OkHttpClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.X_FORWARDED_HOST;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PORT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.MAX_HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ClientCapabilities.PATH;
import static io.trino.client.ClientCapabilities.SESSION_AUTHORIZATION;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.client.uri.HttpClientFactory.toHttpClientBuilder;
import static io.trino.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.OK;
import static jakarta.ws.rs.core.Response.Status.SEE_OTHER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestServer
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    private TestingTrinoServer server;
    private HttpClient client;

    @BeforeAll
    public void setup()
    {
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.of("http-server.process-forwarded", "true", "query.client.timeout", "1s"))
                .build();

        server.installPlugin(new MemoryPlugin());
        server.installPlugin(new TpchPlugin());
        server.createCatalog("memory", "memory");
        server.createCatalog("tpch", "tpch");

        client = new JettyHttpClient();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testInvalidSessionError()
    {
        String invalidTimeZone = "this_is_an_invalid_time_zone";
        QueryResults queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(TRINO_HEADERS.requestCatalog(), "catalog")
                .setHeader(TRINO_HEADERS.requestSchema(), "schema")
                .setHeader(TRINO_HEADERS.requestPath(), "path")
                .setHeader(TRINO_HEADERS.requestTimeZone(), invalidTimeZone))
                .map(JsonResponse::getValue)
                .peek(result -> checkState((result.getError() == null) != (result.getNextUri() == null)))
                .collect(last());

        QueryError queryError = queryResults.getError();
        assertThat(queryError).isNotNull();
        TimeZoneNotSupportedException expected = new TimeZoneNotSupportedException(invalidTimeZone);
        assertThat(queryError.getErrorCode()).isEqualTo(expected.getErrorCode().getCode());
        assertThat(queryError.getErrorName()).isEqualTo(expected.getErrorCode().getName());
        assertThat(queryError.getErrorType()).isEqualTo(expected.getErrorCode().getType().name());
        assertThat(queryError.getMessage()).isEqualTo(expected.getMessage());
    }

    @Test
    public void testFirstResponseColumns()
            throws Exception
    {
        List<QueryResults> queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(TRINO_HEADERS.requestCatalog(), "catalog")
                .setHeader(TRINO_HEADERS.requestSchema(), "schema")
                .setHeader(TRINO_HEADERS.requestPath(), "path"))
                .map(JsonResponse::getValue)
                .collect(toImmutableList());

        QueryResults first = queryResults.getFirst();
        QueryResults last = queryResults.getLast();

        Optional<QueryResults> data = queryResults.stream().filter(results -> results.getData() != null).findFirst();

        assertThat(first.getColumns()).isNull();
        assertThat(first.getStats().getState()).isEqualTo("QUEUED");
        assertThat(first.getData()).isNull();

        assertThat(last.getColumns()).hasSize(1);
        assertThat(getOnlyElement(last.getColumns()).getName()).isEqualTo("Catalog");
        assertThat(getOnlyElement(last.getColumns()).getType()).isEqualTo("varchar(6)");
        assertThat(last.getStats().getState()).isEqualTo("FINISHED");

        assertThat(data).isPresent();

        QueryResults results = data.orElseThrow();

        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(decoder.toRows(results)).containsOnly(ImmutableList.of("memory"), ImmutableList.of("system"), ImmutableList.of("tpch"));
        }
    }

    @Test
    public void testServerStarts()
    {
        StatusResponse response = client.execute(
                prepareGet().setUri(server.resolve("/v1/info")).build(),
                createStatusResponseHandler());

        assertThat(response.getStatusCode()).isEqualTo(OK.getStatusCode());
    }

    @Test
    public void testQuery()
    {
        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        QueryResults queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(TRINO_HEADERS.requestCatalog(), "catalog")
                .setHeader(TRINO_HEADERS.requestSchema(), "schema")
                .setHeader(TRINO_HEADERS.requestPath(), "path")
                .setHeader(TRINO_HEADERS.requestClientInfo(), "{\"clientVersion\":\"testVersion\"}")
                .addHeader(TRINO_HEADERS.requestSession(), QUERY_MAX_MEMORY + "=1GB")
                .addHeader(TRINO_HEADERS.requestSession(), JOIN_DISTRIBUTION_TYPE + "=partitioned," + MAX_HASH_PARTITION_COUNT + " = 43")
                .addHeader(TRINO_HEADERS.requestPreparedStatement(), "foo=select * from bar"))
                .map(JsonResponse::getValue)
                .peek(result -> assertThat(result.getError()).isNull())
                .peek(results -> {
                    if (results.getData() != null) {
                        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
                            data.addAll(decoder.toRows(results));
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .collect(last());

        // get the query info
        BasicQueryInfo queryInfo = server.getQueryManager().getQueryInfo(new QueryId(queryResults.getId()));

        // verify session properties
        assertThat(queryInfo.getSession().getSystemProperties()).isEqualTo(ImmutableMap.builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(MAX_HASH_PARTITION_COUNT, "43")
                .buildOrThrow());

        // verify client info in session
        assertThat(queryInfo.getSession().getClientInfo().get()).isEqualTo("{\"clientVersion\":\"testVersion\"}");

        // verify prepared statements
        assertThat(queryInfo.getSession().getPreparedStatements()).isEqualTo(ImmutableMap.of("foo", "select * from bar"));

        List<List<Object>> rows = data.build();
        assertThat(rows).isEqualTo(ImmutableList.of(ImmutableList.of("memory"), ImmutableList.of("system"), ImmutableList.of("tpch")));
    }

    @Test
    public void testTransactionSupport()
    {
        JsonResponse<QueryResults> queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(TRINO_HEADERS.requestTransactionId(), "none"))
                .peek(result -> assertThat(result.getValue().getError()).isNull())
                .collect(last());
        assertThat(queryResults.getHeader(TRINO_HEADERS.responseStartedTransactionId())).isNotNull();
    }

    @Test
    public void testNoTransactionSupport()
    {
        QueryResults queryResults = postQuery(request -> request.setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8)))
                .map(JsonResponse::getValue)
                .filter(result -> result.getError() != null)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Error expected"));

        assertThat(queryResults.getNextUri()).isNull();
        assertThat(queryResults.getError().getErrorCode()).isEqualTo(INCOMPATIBLE_CLIENT.toErrorCode().getCode());
    }

    @Test
    public void testVersionOnError()
    {
        // fails during parsing
        checkVersionOnError("SELECT query that fails parsing", "ParsingException: line 1:19: mismatched input 'fails'. Expecting");
        // fails during analysis
        checkVersionOnError("SELECT foo FROM some_catalog.some_schema.no_such_table", "TrinoException: line 1:17: Catalog 'some_catalog' not found");
        // fails during Values evaluation in LocalExecutionPlanner
        checkVersionOnError("SELECT 1 / 0", "TrinoException: Division by zero(?s:.*)at io.trino.sql.planner.LocalExecutionPlanner.plan");
        checkVersionOnError("select 1 / a from (values 0) t(a)", "TrinoException: Division by zero(?s:.*)at io.trino.sql.planner.LocalExecutionPlanner.plan");
        // fails during execution
        checkVersionOnError("select 1 / a + x + x from (values (rand(), 0)) t(x, a)", "TrinoException: Division by zero(?s:.*)at io.trino.operator.Driver.processInternal");
    }

    @Test
    public void testVersionOnCompilerFailedError()
    {
        String tableName = "memory.default.test_version_on_compiler_failed";
        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().build())) {
            testingClient.execute("DROP TABLE IF EXISTS " + tableName);
            testingClient.execute("CREATE TABLE " + tableName + " AS SELECT '' as foo");

            // This query is designed to cause a compile failure, and hopefully not run too long.
            // The exact query is not important, just that it causes a failure.
            String array = IntStream.range(0, 254)
                    .mapToObj(columnNumber -> "foo")
                    .collect(joining(", ", "ARRAY[", "]"));
            String query = "SELECT " +
                    String.join(" || ", Collections.nCopies(10, array)) +
                    "FROM " + tableName;

            checkVersionOnError(query, "TrinoException: Failed to execute query; (?s:.*)at io.trino.sql.gen.PageFunctionCompiler.compileProjectionInternal");
        }
    }

    @Test
    public void testSetPathSupportByClient()
    {
        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of()).build())) {
            assertThatThrownBy(() -> testingClient.execute("SET PATH foo"))
                    .hasMessage("SET PATH not supported by client");
        }

        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of(PATH.name())).build())) {
            testingClient.execute("SET PATH foo");
        }
    }

    @Test
    public void testSetSessionSupportByClient()
    {
        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of()).build())) {
            assertThatThrownBy(() -> testingClient.execute("SET SESSION AUTHORIZATION userA"))
                    .hasMessage("SET SESSION AUTHORIZATION not supported by client");
        }

        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of(
                SESSION_AUTHORIZATION.name())).build())) {
            testingClient.execute("SET SESSION AUTHORIZATION userA");
        }
    }

    @Test
    public void testAbandonedQueries()
            throws InterruptedException
    {
        TrinoUri trinoUri = TrinoUri.builder()
                .setUri(server.getBaseUrl())
                .build();

        OkHttpClient httpClient = toHttpClientBuilder(trinoUri, "Trino Test").build();
        ClientSession session = ClientSession.builder()
                .server(server.getBaseUrl())
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .user(Optional.of("user"))
                .heartbeatInterval(new Duration(1, TimeUnit.SECONDS))
                .build();

        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session, "SELECT * FROM tpch.sf1.nation")) {
            client.advance();
            Thread.sleep(2000); // client timeout exceeded
            client.advance();
            assertThat(client.currentStatusInfo().getError().getMessage())
                    .contains("was abandoned by the client, as it may have exited or stopped checking for query results");
        }

        try (StatementClient client = StatementClientFactory.newStatementClient(httpClient, session, "SELECT * FROM tpch.sf1.nation")) {
            while (client.advance()) {
                client.currentRows().forEach(_ -> {
                    try {
                        Thread.sleep(100); // 25 rows * 100 ms = 2500 ms > client timeout
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(client.currentStatusInfo().getError()).isNull();
            }
        }
    }

    @Test
    public void testResetSessionSupportByClient()
    {
        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of()).build())) {
            assertThatThrownBy(() -> testingClient.execute("RESET SESSION AUTHORIZATION"))
                    .hasMessage("RESET SESSION AUTHORIZATION not supported by client");
        }

        try (TestingTrinoClient testingClient = new TestingTrinoClient(server, testSessionBuilder().setClientCapabilities(Set.of(
                SESSION_AUTHORIZATION.name())).build())) {
            testingClient.execute("RESET SESSION AUTHORIZATION");
        }
    }

    private void checkVersionOnError(String query, @Language("RegExp") String proofOfOrigin)
    {
        QueryResults queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8)))
                .map(JsonResponse::getValue)
                .filter(result -> result.getError() != null)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Error expected"));

        assertThat(queryResults.getNextUri()).isNull();
        QueryError queryError = queryResults.getError();
        String stackTrace = getStackTraceAsString(queryError.getFailureInfo().toException());
        assertThat(stackTrace).containsPattern(proofOfOrigin);
        long versionLines = Splitter.on("\n").splitToStream(stackTrace)
                .filter(line -> line.contains("at io.trino.$gen.Trino_testversion____"))
                .count();
        if (versionLines != 1) {
            fail(format("Expected version embedded in the stacktrace exactly once, but was %s: %s", versionLines, stackTrace));
        }
    }

    @Test
    public void testStatusPing()
    {
        Request request = prepareHead()
                .setUri(uriFor("/v1/status"))
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .setFollowRedirects(false)
                .build();
        StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode())
                .describedAs("Status code")
                .isEqualTo(OK.getStatusCode());
        assertThat(response.getHeader(CONTENT_TYPE))
                .describedAs("Content Type")
                .isEqualTo(APPLICATION_JSON);
    }

    @Test
    public void testRedirectToUi()
    {
        Request request = prepareGet()
                .setUri(uriFor("/"))
                .setFollowRedirects(false)
                .build();
        StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode())
                .describedAs("Status code")
                .isEqualTo(SEE_OTHER.getStatusCode());
        assertThat(response.getHeader("Location"))
                .describedAs("Location")
                .isEqualTo(server.getBaseUrl() + "/ui/");

        // behind a proxy
        request = prepareGet()
                .setUri(uriFor("/"))
                .setHeader(X_FORWARDED_PROTO, "https")
                .setHeader(X_FORWARDED_HOST, "my-load-balancer.local")
                .setHeader(X_FORWARDED_PORT, "443")
                .setFollowRedirects(false)
                .build();
        response = client.execute(request, createStatusResponseHandler());
        assertThat(response.getStatusCode())
                .describedAs("Status code")
                .isEqualTo(SEE_OTHER.getStatusCode());
        assertThat(response.getHeader("Location"))
                .describedAs("Location")
                .isEqualTo("https://my-load-balancer.local/ui/");
    }

    private Stream<JsonResponse<QueryResults>> postQuery(Function<Request.Builder, Request.Builder> requestConfigurer)
    {
        Request.Builder request = preparePost()
                .setUri(uriFor("/v1/statement"))
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setHeader(TRINO_HEADERS.requestSource(), "source");
        request = requestConfigurer.apply(request);
        JsonResponse<QueryResults> queryResults = client.execute(request.build(), createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        return Streams.stream(new QueryResultsIterator(client, queryResults));
    }

    private URI uriFor(String path)
    {
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }

    /**
     * Retains last element of a stream. Does not accept null stream elements nor empty streams.
     */
    private static <T> Collector<T, ?, T> last()
    {
        return Collectors.collectingAndThen(Collectors.reducing((a, b) -> b), Optional::get);
    }

    private static class QueryResultsIterator
            extends AbstractSequentialIterator<JsonResponse<QueryResults>>
    {
        private final HttpClient client;

        QueryResultsIterator(HttpClient client, JsonResponse<QueryResults> firstResults)
        {
            super(requireNonNull(firstResults, "firstResults is null"));
            this.client = requireNonNull(client, "client is null");
        }

        @Override
        protected JsonResponse<QueryResults> computeNext(JsonResponse<QueryResults> previous)
        {
            if (previous.getValue().getNextUri() == null) {
                return null;
            }

            return client.execute(prepareGet().setUri(previous.getValue().getNextUri()).build(), createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        }
    }
}
