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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.client.QueryResults;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestResourceGroupStateInfoResource
{
    private TestingTrinoServer server;
    private HttpClient client;

    @BeforeAll
    public void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("web-ui.authentication.type", "fixed")
                        .put("web-ui.user", "test-user")
                        .buildOrThrow())
                .build();
        server.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSecuritySensitivePropertyNames(ImmutableSet.of("password"))
                .build()));
    }

    @AfterAll
    public void teardown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    void testGetResourceGroupInfoWithRedactedSecrets()
    {
        String catalog = "catalog_" + randomNameSuffix();
        startQuery("""
                CREATE CATALOG %s USING mock
                WITH (
                   "user" = 'bob',
                   "password" = '1234'
                )""".formatted(catalog));

        ResourceGroupInfo resourceGroupInfo = getResourceGroupInfo("global");
        Optional<List<QueryStateInfo>> queryStateInfos = resourceGroupInfo.runningQueries();
        assertThat(queryStateInfos.isPresent()).isTrue();
        List<QueryStateInfo> queryStates = queryStateInfos.get();
        assertThat(queryStates.size()).isEqualTo(1);
        assertThat(queryStates.getFirst().getQuery()).isEqualTo("""
                CREATE CATALOG %s USING mock
                WITH (
                   "user" = 'bob',
                   "password" = '***'
                )""".formatted(catalog));
    }

    private void startQuery(String sql)
    {
        Request request = preparePost()
                .setUri(server.resolve("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        checkState(queryResults.getNextUri() != null && queryResults.getNextUri().toString().contains("/v1/statement/queued/"), "nextUri should point to /v1/statement/queued/");
        request = prepareGet()
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .setUri(queryResults.getNextUri())
                .build();
        client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    private ResourceGroupInfo getResourceGroupInfo(String resourceGroupId)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl())
                .replacePath("/v1/resourceGroupState")
                .appendPath(resourceGroupId)
                .build();
        Request request = prepareGet()
                .setUri(uri)
                .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(ResourceGroupInfo.class)));
    }
}
