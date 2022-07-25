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
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.client.QueryResults;
import io.trino.execution.QueryInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeAll;
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
import static io.trino.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryResource
{
    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeMethod
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        closeAll(server, client);
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

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        URI uri = queryResults.getNextUri();
        while (uri != null) {
            QueryResults attempt1 = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryResults.class)));

            QueryResults attempt2 = client.execute(
                    prepareGet()
                            .setHeader(TRINO_HEADERS.requestUser(), "user")
                            .setUri(uri)
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryResults.class)));

            assertEquals(attempt2.getData(), attempt1.getData());

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
        assertEquals(infos.size(), 3);
        assertStateCounts(infos, 2, 1, 0);

        infos = getQueryInfos("/v1/query?state=finished");
        assertEquals(infos.size(), 2);
        assertStateCounts(infos, 2, 0, 0);

        infos = getQueryInfos("/v1/query?state=failed");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 1, 0);

        infos = getQueryInfos("/v1/query?state=running");
        assertEquals(infos.size(), 0);
        assertStateCounts(infos, 0, 0, 0);

        server.getAccessControl().deny(privilege("query", VIEW_QUERY));
        try {
            assertTrue(getQueryInfos("/v1/query").isEmpty());
            assertTrue(getQueryInfos("/v1/query?state=finished").isEmpty());
            assertTrue(getQueryInfos("/v1/query?state=failed").isEmpty());
            assertTrue(getQueryInfos("/v1/query?state=running").isEmpty());
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testGetQueryInfoDispatchFailure()
    {
        String queryId = runToCompletion("SELECT");
        QueryInfo info = getQueryInfo(queryId);
        assertFalse(info.isScheduled());
        assertNotNull(info.getFailureInfo());
        assertEquals(info.getFailureInfo().getErrorCode(), SYNTAX_ERROR.toErrorCode());

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
        assertTrue(info.isScheduled());
        assertNotNull(info.getFailureInfo());
        assertEquals(info.getFailureInfo().getErrorCode(), DIVISION_BY_ZERO.toErrorCode());
    }

    @Test
    public void testCancel()
    {
        String queryId = startQuery("SELECT * FROM tpch.sf100.lineitem");

        server.getAccessControl().deny(privilege("query", KILL_QUERY));
        try {
            assertEquals(cancelQueryInfo(queryId), 403);
        }
        finally {
            server.getAccessControl().reset();
        }

        assertEquals(cancelQueryInfo(queryId), 204);
        assertEquals(cancelQueryInfo(queryId), 204);
        BasicQueryInfo queryInfo = server.getDispatchManager().getQueryInfo(new QueryId(queryId));
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), USER_CANCELED.toErrorCode());
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

    private void testKilled(String killType)
    {
        String queryId = startQuery("SELECT * FROM tpch.sf100.lineitem");

        server.getAccessControl().deny(privilege("query", KILL_QUERY));
        try {
            assertEquals(killQueryInfo(queryId, killType), 403);
        }
        finally {
            server.getAccessControl().reset();
        }

        assertEquals(killQueryInfo(queryId, killType), 202);
        assertEquals(killQueryInfo(queryId, killType), 409);
        BasicQueryInfo queryInfo = server.getDispatchManager().getQueryInfo(new QueryId(queryId));
        assertEquals(queryInfo.getState(), FAILED);
        if (killType.equals("killed")) {
            assertEquals(queryInfo.getErrorCode(), ADMINISTRATIVELY_KILLED.toErrorCode());
        }
        else {
            assertEquals(queryInfo.getErrorCode(), ADMINISTRATIVELY_PREEMPTED.toErrorCode());
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
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        while (queryResults.getNextUri() != null) {
            request = prepareGet()
                    .setHeader(TRINO_HEADERS.requestUser(), "user")
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
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
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        while (queryResults.getNextUri() != null && !queryResults.getStats().getState().equals(RUNNING.toString())) {
            request = prepareGet()
                    .setHeader(TRINO_HEADERS.requestUser(), "user")
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        }
        return queryResults.getId();
    }

    private List<BasicQueryInfo> getQueryInfos(String path)
    {
        Request request = prepareGet()
                .setUri(server.resolve(path))
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
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
        assertEquals(failed, expectedFailed);
        assertEquals(finished, expectedFinished);
        assertEquals(running, expectedRunning);
    }

    private QueryInfo getQueryInfo(String queryId)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/query")
                .appendPath(queryId)
                .addParameter("pretty", "true")
                .build();
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
