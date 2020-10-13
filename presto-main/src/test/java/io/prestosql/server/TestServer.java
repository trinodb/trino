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
package io.prestosql.server;

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
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryResults;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.type.TimeZoneNotSupportedException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
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
import static io.prestosql.SystemSessionProperties.HASH_PARTITION_COUNT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.client.PrestoHeaders.PRESTO_CATALOG;
import static io.prestosql.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static io.prestosql.client.PrestoHeaders.PRESTO_PATH;
import static io.prestosql.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static io.prestosql.client.PrestoHeaders.PRESTO_SCHEMA;
import static io.prestosql.client.PrestoHeaders.PRESTO_SESSION;
import static io.prestosql.client.PrestoHeaders.PRESTO_SOURCE;
import static io.prestosql.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static io.prestosql.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.spi.StandardErrorCode.INCOMPATIBLE_CLIENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SEE_OTHER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestServer
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private TestingPrestoServer server;
    private HttpClient client;

    @BeforeClass
    public void setup()
    {
        server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.process-forwarded", "true")
                        .build())
                .build();
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
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_PATH, "path")
                .setHeader(PRESTO_TIME_ZONE, invalidTimeZone))
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
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_PATH, "path"))
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
        assertThat(results.getData()).containsOnly(ImmutableList.of("system"));
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
                .setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema")
                .setHeader(PRESTO_PATH, "path")
                .setHeader(PRESTO_CLIENT_INFO, "{\"clientVersion\":\"testVersion\"}")
                .addHeader(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                .addHeader(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                .addHeader(PRESTO_PREPARED_STATEMENT, "foo=select * from bar"))
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

        // only the system catalog exists by default
        List<List<Object>> rows = data.build();
        assertEquals(rows, ImmutableList.of(ImmutableList.of("system")));
    }

    @Test
    public void testTransactionSupport()
    {
        JsonResponse<QueryResults> queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator("start transaction", UTF_8))
                .setHeader(PRESTO_TRANSACTION_ID, "none"))
                .peek(result -> assertNull(result.getValue().getError()))
                .collect(last());
        assertNotNull(queryResults.getHeader(PRESTO_STARTED_TRANSACTION_ID));
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

    @Test(dataProvider = "testVersionOnErrorDataProvider")
    public void testVersionOnError(String query)
    {
        QueryResults queryResults = postQuery(request -> request
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8)))
                .map(JsonResponse::getValue)
                .filter(result -> result.getError() != null)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Error expected"));

        assertNull(queryResults.getNextUri());
        QueryError queryError = queryResults.getError();
        List<String> stackTrace = Splitter.on("\n").splitToList(getStackTraceAsString(queryError.getFailureInfo().toException()));
        long versionLines = stackTrace.stream()
                .filter(line -> line.contains("at io.prestosql.$gen.Presto_testversion____"))
                .count();
        assertEquals(versionLines, 1, "Number of times version is embedded in stacktrace");
    }

    @DataProvider
    public Object[][] testVersionOnErrorDataProvider()
    {
        return new Object[][] {
                {"SELECT query that fails parsing"}, // fails during parsing
                {"SELECT foo FROM no.such.table"}, // fails during analysis
                {"SELECT 1 / 0"}, // fails during optimization
                {"select 1 / a from (values 0) t(a)"}, // fails during execution
        };
    }

    @Test
    public void testStatusPing()
    {
        Request request = prepareHead()
                .setUri(uriFor("/v1/status"))
                .setHeader(PRESTO_USER, "unknown")
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
                .setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source");
        request = requestConfigurer.apply(request);
        JsonResponse<QueryResults> queryResults = client.execute(request.build(), createFullJsonResponseHandler(QUERY_RESULTS_CODEC));
        return Streams.stream(new QueryResultsIterator(client, queryResults));
    }

    private URI uriFor(String path)
    {
        return HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }

    /**
     * Retains last element of a stream. Does not not accept null stream elements nor empty streams.
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
