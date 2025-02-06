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
package io.trino.server;

import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryDataClientJacksonModule;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.testing.Closeables.closeAll;
import static io.airlift.tracing.SpanSerialization.SpanDeserializer;
import static io.airlift.tracing.SpanSerialization.SpanSerializer;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.KILL_QUERY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.VIEW_QUERY;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestQueryResource
{
    static final JsonCodec<List<BasicQueryInfo>> BASIC_QUERY_INFO_CODEC = new JsonCodecFactory(
            new ObjectMapperProvider()
                    .withModules(Set.of(new QueryDataClientJacksonModule()))
                    .withJsonSerializers(Map.of(Span.class, new SpanSerializer(OpenTelemetry.noop())))
                    .withJsonDeserializers(Map.of(Span.class, new SpanDeserializer(OpenTelemetry.noop()))))
            .listJsonCodec(BasicQueryInfo.class);

    static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = new JsonCodecFactory(
            new ObjectMapperProvider()
                    .withModules(Set.of(new QueryDataClientJacksonModule())))
            .jsonCodec(QueryResults.class);

    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeEach
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterEach
    public void teardown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testIdempotentResults()
    {
        String sql = "SELECT * FROM tpch.tiny.lineitem";

        Request request = preparePost()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build())
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        URI uri = queryResults.getNextUri();
        while (uri != null) {
            QueryResults attempt1 = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

            QueryResults attempt2 = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

            assertDataEquals(attempt1.getColumns(), attempt2.getData(), attempt1.getData());
            uri = attempt1.getNextUri();
        }
    }

    @Test
    public void testGetQueryInfos()
    {
        runToCompletion("SELECT 1");
        runToCompletion("SELECT 2");
        runToCompletion("SELECT x FROM y");

        List<BasicQueryInfo> infos = getQueryInfos("/v1/query");
        assertThat(infos).hasSize(3);
        assertStateCounts(infos, 2, 1, 0);

        infos = getQueryInfos("/v1/query?state=finished");
        assertThat(infos).hasSize(2);
        assertStateCounts(infos, 2, 0, 0);

        infos = getQueryInfos("/v1/query?state=failed");
        assertThat(infos).hasSize(1);
        assertStateCounts(infos, 0, 1, 0);

        infos = getQueryInfos("/v1/query?state=running");
        assertThat(infos).isEmpty();
        assertStateCounts(infos, 0, 0, 0);

        infos = getQueryInfos("/v1/query?state=finished&state=failed&state=running");
        assertThat(infos).hasSize(3);
        assertStateCounts(infos, 2, 1, 0);

        server.getAccessControl().deny(privilege("query", VIEW_QUERY));
        try {
            assertThat(getQueryInfos("/v1/query")).isEmpty();
            assertThat(getQueryInfos("/v1/query?state=finished")).isEmpty();
            assertThat(getQueryInfos("/v1/query?state=failed")).isEmpty();
            assertThat(getQueryInfos("/v1/query?state=running")).isEmpty();
            assertThat(getQueryInfos("/v1/query?state=finished&state=failed&state=running")).isEmpty();
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testGetQueryInfoPruned()
    {
        String queryId = runToCompletion("SELECT now()");

        QueryInfo queryInfoPruned = getQueryInfo(queryId, true);
        QueryInfo queryInfoNotPruned = getQueryInfo(queryId);

        assertThat(queryInfoPruned.getRoutines()).hasSize(1);
        assertThat(queryInfoNotPruned.getRoutines()).hasSize(1);

        assertThat(queryInfoPruned.getRoutines().get(0).getRoutine()).isEqualTo("now");
        assertThat(queryInfoNotPruned.getRoutines().get(0).getRoutine()).isEqualTo("now");

        assertThat(queryInfoPruned.getOutputStage()).isPresent();
        assertThat(queryInfoNotPruned.getOutputStage()).isPresent();

        assertThat(queryInfoPruned.getOutputStage().get().getTasks()).isEmpty();
        assertThat(queryInfoNotPruned.getOutputStage().get().getTasks()).isNotEmpty();
    }

    @Test
    public void testGetQueryInfoDispatchFailure()
    {
        String queryId = runToCompletion("SELECT");
        QueryInfo info = getQueryInfo(queryId);
        assertThat(info.isScheduled()).isFalse();
        assertThat(info.getFailureInfo()).isNotNull();
        assertThat(info.getFailureInfo().getErrorCode()).isEqualTo(SYNTAX_ERROR.toErrorCode());

        server.getAccessControl().deny(privilege("query", VIEW_QUERY));
        try {
            assertThatThrownBy(() -> getQueryInfo(queryId))
                    .isInstanceOf(UnexpectedResponseException.class)
                    .matches(throwable -> ((UnexpectedResponseException) throwable).getStatusCode() == 403);
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testGetQueryInfoExecutionFailure()
    {
        String queryId = runToCompletion("SELECT cast(rand() AS integer) / 0");
        QueryInfo info = getQueryInfo(queryId);
        assertThat(info.isScheduled()).isTrue();
        assertThat(info.getFailureInfo()).isNotNull();
        assertThat(info.getFailureInfo().getErrorCode()).isEqualTo(DIVISION_BY_ZERO.toErrorCode());
    }

    @Test
    public void testCancel()
    {
        String queryId = startQuery("SELECT * FROM tpch.sf100.lineitem");

        server.getAccessControl().deny(privilege("query", KILL_QUERY));
        try {
            assertThat(cancelQueryInfo(queryId)).isEqualTo(403);
        }
        finally {
            server.getAccessControl().reset();
        }

        assertThat(cancelQueryInfo(queryId)).isEqualTo(204);
        assertThat(cancelQueryInfo(queryId)).isEqualTo(204);
        BasicQueryInfo queryInfo = server.getDispatchManager().getQueryInfo(new QueryId(queryId));
        assertThat(queryInfo.getState()).isEqualTo(FAILED);
        assertThat(queryInfo.getErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());
    }

    @Test
    public void testKilled()
    {
        testKilled("killed");
    }

    @Test
    public void testPreempted()
    {
        testKilled("preempted");
    }

    private void assertDataEquals(List<Column> columns, QueryData left, QueryData right)
    {
        if (left == null) {
            assertThat(right).isNull();
            return;
        }

        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(decoder.toRows(columns, left))
                    .containsAll(decoder.toRows(columns, right));
        }
        catch (Exception e) {
            fail(e);
        }
    }

    private void testKilled(String killType)
    {
        String queryId = startQuery("SELECT * FROM tpch.sf100.lineitem");

        server.getAccessControl().deny(privilege("query", KILL_QUERY));
        try {
            assertThat(killQueryInfo(queryId, killType)).isEqualTo(403);
        }
        finally {
            server.getAccessControl().reset();
        }

        assertThat(killQueryInfo(queryId, killType)).isEqualTo(202);
        assertThat(killQueryInfo(queryId, killType)).isEqualTo(409);
        BasicQueryInfo queryInfo = server.getDispatchManager().getQueryInfo(new QueryId(queryId));
        assertThat(queryInfo.getState()).isEqualTo(FAILED);
        if (killType.equals("killed")) {
            assertThat(queryInfo.getErrorCode()).isEqualTo(ADMINISTRATIVELY_KILLED.toErrorCode());
        }
        else {
            assertThat(queryInfo.getErrorCode()).isEqualTo(ADMINISTRATIVELY_PREEMPTED.toErrorCode());
        }
    }

    private String runToCompletion(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        Request request = preparePost()
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        while (queryResults.getNextUri() != null) {
            request = prepareGet()
                    .setHeader(TRINO_HEADERS.requestUser(), "user")
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        }
        return queryResults.getId();
    }

    private String startQuery(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build();
        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .setHeader(TRINO_HEADERS.requestUser(), "user")
                .build();
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        while (queryResults.getNextUri() != null && !queryResults.getStats().getState().equals(RUNNING.toString())) {
            request = prepareGet()
                    .setHeader(TRINO_HEADERS.requestUser(), "user")
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        }
        return queryResults.getId();
    }

    private List<BasicQueryInfo> getQueryInfos(String path)
    {
        Request request = prepareGet()
                .setUri(server.resolve(path))
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        return client.execute(request, createJsonResponseHandler(BASIC_QUERY_INFO_CODEC));
    }

    private static void assertStateCounts(Iterable<BasicQueryInfo> infos, int expectedFinished, int expectedFailed, int expectedRunning)
    {
        int failed = 0;
        int finished = 0;
        int running = 0;
        for (BasicQueryInfo info : infos) {
            switch (info.getState()) {
                case FINISHED:
                    finished++;
                    break;
                case FAILED:
                    failed++;
                    break;
                case RUNNING:
                    running++;
                    break;
                default:
                    fail("Unexpected query state " + info.getState());
            }
        }
        assertThat(failed).isEqualTo(expectedFailed);
        assertThat(finished).isEqualTo(expectedFinished);
        assertThat(running).isEqualTo(expectedRunning);
    }

    private QueryInfo getQueryInfo(String queryId)
    {
        return getQueryInfo(queryId, false);
    }

    private QueryInfo getQueryInfo(String queryId, boolean pruned)
    {
        HttpUriBuilder builder = uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/query")
                .appendPath(queryId)
                .addParameter("pretty", "true");

        if (pruned) {
            builder.addParameter("pruned", "true");
        }

        URI uri = builder.build();
        Request request = prepareGet()
                .setUri(uri)
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        JsonCodec<QueryInfo> codec = server.getInstance(Key.get(JsonCodecFactory.class)).jsonCodec(QueryInfo.class);
        return client.execute(request, createJsonResponseHandler(codec));
    }

    private int cancelQueryInfo(String queryId)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/query")
                .appendPath(queryId)
                .build();
        Request request = prepareDelete()
                .setUri(uri)
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        return client.execute(request, createStatusResponseHandler()).getStatusCode();
    }

    private int killQueryInfo(String queryId, String kind)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/query")
                .appendPath(queryId)
                .appendPath(kind)
                .build();
        Request request = preparePut()
                .setUri(uri)
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        return client.execute(request, createStatusResponseHandler()).getStatusCode();
    }
}
