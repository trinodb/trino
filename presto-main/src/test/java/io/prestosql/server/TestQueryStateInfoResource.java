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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.prestosql.client.QueryResults;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeQuietly;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.VIEW_QUERY;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryStateInfoResource
{
    private static final String LONG_LASTING_QUERY = "SELECT * FROM tpch.sf1.lineitem";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = jsonCodec(QueryResults.class);

    private TestingPrestoServer server;
    private HttpClient client;
    private QueryResults queryResults;

    @BeforeClass
    public void setUp()
    {
        server = TestingPrestoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        client = new JettyHttpClient();

        Request request1 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(PRESTO_USER, "user1")
                .build();
        queryResults = client.execute(request1, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        Request request2 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(PRESTO_USER, "user2")
                .build();
        QueryResults queryResults2 = client.execute(request2, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        client.execute(prepareGet().setUri(queryResults2.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        // queries are started in the background, so they may not all be immediately visible
        while (true) {
            List<BasicQueryInfo> queryInfos = client.execute(
                    prepareGet()
                            .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/query").build())
                            .setHeader(PRESTO_USER, "unknown")
                            .build(),
                    createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
            if ((queryInfos.size() == 2) && queryInfos.stream().allMatch(info -> info.getState() == RUNNING)) {
                break;
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(server, client);
    }

    @Test
    public void testGetAllQueryStateInfos()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState"))
                        .setHeader(PRESTO_USER, "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertEquals(infos.size(), 2);
    }

    @Test
    public void testGetQueryStateInfosForUser()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState?user=user2"))
                        .setHeader(PRESTO_USER, "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertEquals(infos.size(), 1);
    }

    @Test
    public void testGetQueryStateInfosForUserNoResult()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState?user=user3"))
                        .setHeader(PRESTO_USER, "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertTrue(infos.isEmpty());
    }

    @Test
    public void testGetQueryStateInfo()
    {
        QueryStateInfo info = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState/" + queryResults.getId()))
                        .setHeader(PRESTO_USER, "unknown")
                        .build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));

        assertNotNull(info);
    }

    @Test
    public void testGetAllQueryStateInfosDenied()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState"))
                        .setHeader(PRESTO_USER, "any-other-user")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));
        assertEquals(infos.size(), 2);

        testGetAllQueryStateInfosDenied("user1", 1);
        testGetAllQueryStateInfosDenied("any-other-user", 0);
    }

    private void testGetAllQueryStateInfosDenied(String executionUser, int expectedCount)
    {
        server.getAccessControl().deny(privilege(executionUser, "query", VIEW_QUERY));
        try {
            List<QueryStateInfo> infos = client.execute(
                    prepareGet()
                            .setUri(server.resolve("/v1/queryState"))
                            .setHeader(PRESTO_USER, executionUser)
                            .build(),
                    createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

            assertEquals(infos.size(), expectedCount);
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testGetQueryStateInfoDenied()
    {
        server.getAccessControl().deny(privilege("query", VIEW_QUERY));
        try {
            assertThatThrownBy(() -> client.execute(
                    prepareGet()
                            .setUri(server.resolve("/v1/queryState/" + queryResults.getId()))
                            .setHeader(PRESTO_USER, "unknown")
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryStateInfo.class))))
                    .isInstanceOf(UnexpectedResponseException.class)
                    .matches(throwable -> ((UnexpectedResponseException) throwable).getStatusCode() == 403);
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test(expectedExceptions = UnexpectedResponseException.class, expectedExceptionsMessageRegExp = "Expected response code .*, but was 404")
    public void testGetQueryStateInfoNo()
    {
        client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState/123"))
                        .setHeader(PRESTO_USER, "unknown")
                        .build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));
    }
}
