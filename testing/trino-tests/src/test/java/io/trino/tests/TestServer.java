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
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.BasicQueryInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.type.TimeZoneNotSupportedException;
import io.trino.testing.TestingTrinoClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SEE_OTHER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestServer
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private TestingTrinoServer server;
    private HttpClient client;

    @BeforeClass
    public void setup()
    {
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.process-forwarded", "true")
                        .build())
                .build();

        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");

        client = new JettyHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(server, client);
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
        assertNotNull(queryError);
        TimeZoneNotSupportedException expected = new TimeZoneNotSupportedException(invalidTimeZone);
        assertEquals(queryError.getErrorCode(), expected.getErrorCode().getCode());
        assertEquals(queryError.getErrorName(), expected.getErrorCode().getName());
        assertEquals(queryError.getErrorType(), expected.getErrorCode().getType().name());
        assertEquals(queryError.getMessage(), expected.getMessage());
    }

    @Test
    public void testFirstResponseColumns()
    {
        List<QueryResults> queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8))
                .setHeader(TRINO_HEADERS.requestCatalog(), "catalog")
                .setHeader(TRINO_HEADERS.requestSchema(), "schema")
                .setHeader(TRINO_HEADERS.requestPath(), "path"))
                .map(JsonResponse::getValue)
                .collect(toImmutableList());

        QueryResults first = queryResults.get(0);
        QueryResults last = queryResults.get(queryResults.size() - 1);

        Optional<QueryResults> data = queryResults.stream().filter(results -> results.getData() != null).findFirst();

        assertNull(first.getColumns());
        assertEquals(first.getStats().getState(), "QUEUED");
        assertNull(first.getData());

        assertThat(last.getColumns()).hasSize(1);
        assertThat(last.getColumns().get(0).getName()).isEqualTo("Catalog");
        assertThat(last.getColumns().get(0).getType()).isEqualTo("varchar(6)");
        assertEquals(last.getStats().getState(), "FINISHED");

        assertThat(data).isPresent();

        QueryResults results = data.orElseThrow();
        assertThat(results.getData()).containsOnly(ImmutableList.of("memory"), ImmutableList.of("system"));
    }

    @Test
    public void testServerStarts()
    {
        StatusResponse response = client.execute(
                prepareGet().setUri(server.resolve("/v1/info")).build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), OK.getStatusCode());
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
                .addHeader(TRINO_HEADERS.requestSession(), JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                .addHeader(TRINO_HEADERS.requestPreparedStatement(), "foo=select * from bar"))
                .map(JsonResponse::getValue)
                .peek(result -> assertNull(result.getError()))
                .peek(results -> {
                    if (results.getData() != null) {
                        data.addAll(results.getData());
                    }
                })
                .collect(last());

        // get the query info
        BasicQueryInfo queryInfo = server.getQueryManager().getQueryInfo(new QueryId(queryResults.getId()));

        // verify session properties
        assertEquals(queryInfo.getSession().getSystemProperties(), ImmutableMap.builder()
                .put(QUERY_MAX_MEMORY, "1GB")
                .put(JOIN_DISTRIBUTION_TYPE, "partitioned")
                .put(HASH_PARTITION_COUNT, "43")
                .build());

        // verify client info in session
        assertEquals(queryInfo.getSession().getClientInfo().get(), "{\"clientVersion\":\"testVersion\"}");

        // verify prepared statements
        assertEquals(queryInfo.getSession().getPreparedStatements(), ImmutableMap.builder()
                .put("foo", "select * from bar")
                .build());

        List<List<Object>> rows = data.build();
        assertEquals(rows, ImmutableList.of(ImmutableList.of("memory"), ImmutableList.of("system")));
    }

    @Test
    public void testTransactionSupport()
    {
        JsonResponse<QueryResults> queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(TRINO_HEADERS.requestTransactionId(), "none"))
                .peek(result -> assertNull(result.getValue().getError()))
                .collect(last());
        assertNotNull(queryResults.getHeader(TRINO_HEADERS.responseStartedTransactionId()));
    }

    @Test
    public void testNoTransactionSupport()
    {
        QueryResults queryResults = postQuery(request -> request.setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8)))
                .map(JsonResponse::getValue)
                .filter(result -> result.getError() != null)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Error expected"));

        assertNull(queryResults.getNextUri());
        assertEquals(queryResults.getError().getErrorCode(), INCOMPATIBLE_CLIENT.toErrorCode().getCode());
    }

    @Test
    public void testVersionOnError()
    {
        // fails during parsing
        checkVersionOnError("SELECT query that fails parsing", "ParsingException: line 1:19: mismatched input 'fails'. Expecting");
        // fails during analysis
        checkVersionOnError("SELECT foo FROM some_catalog.some_schema.no_such_table", "TrinoException: line 1:17: Catalog 'some_catalog' does not exist");
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

            checkVersionOnError(query, "TrinoException: Compiler failed(?s:.*)at io.trino.sql.gen.ExpressionCompiler.compile");
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

        assertNull(queryResults.getNextUri());
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
        assertEquals(response.getStatusCode(), OK.getStatusCode(), "Status code");
        assertEquals(response.getHeader(CONTENT_TYPE), APPLICATION_JSON, "Content Type");
    }

    @Test
    public void testRedirectToUi()
    {
        Request request = prepareGet()
                .setUri(uriFor("/"))
                .setFollowRedirects(false)
                .build();
        StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), SEE_OTHER.getStatusCode(), "Status code");
        assertEquals(response.getHeader("Location"), server.getBaseUrl() + "/ui/", "Location");

        // behind a proxy
        request = prepareGet()
                .setUri(uriFor("/"))
                .setHeader(X_FORWARDED_PROTO, "https")
                .setHeader(X_FORWARDED_HOST, "my-load-balancer.local")
                .setHeader(X_FORWARDED_PORT, "443")
                .setFollowRedirects(false)
                .build();
        response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), SEE_OTHER.getStatusCode(), "Status code");
        assertEquals(response.getHeader("Location"), "https://my-load-balancer.local/ui/", "Location");
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
