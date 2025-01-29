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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.trino.client.QueryResults;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.protocol.spooling.QueryDataJacksonModule;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.server.TestQueryResource.BASIC_QUERY_INFO_CODEC;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.VIEW_QUERY;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestQueryStateInfoResource
{
    private static final String LONG_LASTING_QUERY = "SELECT * FROM tpch.sf1.lineitem";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    private TestingTrinoServer server;
    private HttpClient client;
    private QueryResults queryResults;
    private QueryResults createCatalogResults;

    @BeforeAll
    public void setUp()
    {
        server = TestingTrinoServer.create();
        server.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSecuritySensitivePropertyNames(ImmutableSet.of("password"))
                .build()));
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        client = new JettyHttpClient();

        Request request1 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(TRINO_HEADERS.requestUser(), "user1")
                .build();
        queryResults = client.execute(request1, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        Request request2 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(TRINO_HEADERS.requestUser(), "user2")
                .build();
        QueryResults queryResults2 = client.execute(request2, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        client.execute(prepareGet().setUri(queryResults2.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        Request createCatalogRequest = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator("""
                CREATE CATALOG test_catalog USING mock
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )""", UTF_8))
                .setHeader(TRINO_HEADERS.requestUser(), "catalogCreator")
                .build();
        createCatalogResults = client.execute(createCatalogRequest, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        client.execute(prepareGet().setUri(createCatalogResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        // queries are started in the background, so they may not all be immediately visible
        long start = System.nanoTime();
        while (Duration.nanosSince(start).compareTo(new Duration(5, MINUTES)) < 0) {
            List<BasicQueryInfo> queryInfos = client.execute(
                    prepareGet()
                            .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/query").build())
                            .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                            .build(),
                    createJsonResponseHandler(BASIC_QUERY_INFO_CODEC));
            if (queryInfos.size() == 3) {
                if (queryInfos.stream().allMatch(info -> info.getState() == RUNNING || info.getState() == FINISHING)) {
                    break;
                }

                List<ErrorCode> errorCodes = queryInfos.stream()
                        .filter(info -> info.getState() == FAILED)
                        .map(BasicQueryInfo::getErrorCode)
                        .collect(toImmutableList());
                if (!errorCodes.isEmpty()) {
                    fail("setup queries failed with: " + errorCodes);
                }
            }
        }
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        Closer closer = Closer.create();
        closer.register(server);
        closer.register(client);
        closer.close();
        server = null;
        client = null;
    }

    @Test
    public void testGetAllQueryStateInfos()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertThat(infos.size()).isEqualTo(3);
        QueryStateInfo createCatalogInfo = infos.stream()
                .filter(info -> info.getQueryId().getId().equals(createCatalogResults.getId()))
                .findFirst()
                .orElse(null);
        assertCreateCatalogQueryIsRedacted(createCatalogInfo);
    }

    @Test
    public void testGetQueryStateInfosForUser()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState?user=user2"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertThat(infos).hasSize(1);
    }

    @Test
    public void testGetQueryStateInfosForUserNoResult()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState?user=user3"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertThat(infos).isEmpty();
    }

    @Test
    public void testGetQueryStateInfo()
    {
        QueryStateInfo info = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState/" + queryResults.getId()))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));

        assertThat(info).isNotNull();
    }

    @Test
    public void testGetQueryStateInfoWithRedactedSecrets()
    {
        QueryStateInfo info = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState/" + createCatalogResults.getId()))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));

        assertCreateCatalogQueryIsRedacted(info);
    }

    @Test
    public void testGetAllQueryStateInfosDenied()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState"))
                        .setHeader(TRINO_HEADERS.requestUser(), "any-other-user")
                        .build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));
        assertThat(infos).hasSize(3);

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
                            .setHeader(TRINO_HEADERS.requestUser(), executionUser)
                            .build(),
                    createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

            assertThat(infos).hasSize(expectedCount);
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
                            .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                            .build(),
                    createJsonResponseHandler(jsonCodec(QueryStateInfo.class))))
                    .isInstanceOf(UnexpectedResponseException.class)
                    .matches(throwable -> ((UnexpectedResponseException) throwable).getStatusCode() == 403);
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    public void testGetQueryStateInfoNo()
    {
        assertThatThrownBy(() -> client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/queryState/123"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class))))
                .isInstanceOf(UnexpectedResponseException.class)
                .hasMessageMatching("Expected response code .*, but was 404");
    }

    private static void assertCreateCatalogQueryIsRedacted(QueryStateInfo info)
    {
        assertThat(info).isNotNull();
        assertThat(info.getQuery()).isEqualTo("""
                CREATE CATALOG test_catalog USING mock
                WITH (
                   "user" = 'bob',
                   "password" = '***'
                )""");
    }
}
