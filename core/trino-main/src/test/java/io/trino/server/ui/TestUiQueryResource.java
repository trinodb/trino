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
package io.trino.server.ui;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.server.TestQueryResource.runToCompletion;
import static io.trino.server.TestQueryResource.startQuery;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.VIEW_QUERY;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // access control is changed by a test
class TestUiQueryResource
{
    private static final JsonCodec<Map<String, List<FlattenedPlanNode>>> QUERY_PLAN_CODEC = mapJsonCodec(String.class, listJsonCodec(FlattenedPlanNode.class));

    private HttpClient client;
    private TestingTrinoServer server;

    @BeforeAll
    void setup()
    {
        client = new JettyHttpClient();
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.of(
                        "web-ui.authentication.type", "fixed",
                        "web-ui.user", "test-user"))
                .build();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
    }

    @AfterAll
    void teardown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    void testQueryPlan()
    {
        String queryId = runToCompletion(client, server, "SELECT * FROM tpch.tiny.nation");

        // a table scan needs no remote exchange, so the query has a single stage
        Map<String, List<FlattenedPlanNode>> plan = queryPlan(queryId);
        assertThat(plan).containsOnlyKeys(queryId + ".0");

        List<FlattenedPlanNode> stage = plan.get(queryId + ".0");
        assertThat(stage).extracting(FlattenedPlanNode::name).containsExactly("Output", "TableScan");
        FlattenedPlanNode output = stage.get(0);
        FlattenedPlanNode tableScan = stage.get(1);
        assertThat(output.descriptor()).containsEntry("columnNames", "[nationkey, name, regionkey, comment]");
        assertThat(output.sources()).containsExactly(tableScan.id());
        assertThat(tableScan.descriptor()).containsEntry("table", "tpch:tiny:nation");
        assertThat(tableScan.details()).contains("nationkey := tpch:nationkey");
        assertThat(tableScan.sources()).isEmpty();
    }

    @Test
    void testQueryPlanOfMultiStageQuery()
    {
        String queryId = runToCompletion(client, server, "SELECT regionkey, count(*) FROM tpch.tiny.nation GROUP BY regionkey");

        Map<String, List<FlattenedPlanNode>> plan = queryPlan(queryId);
        Set<String> stageIds = server.getDispatchManager().getFullQueryInfo(new QueryId(queryId)).orElseThrow()
                .getStages().orElseThrow()
                .getStages().stream()
                .map(stage -> stage.stageId().toString())
                .collect(toImmutableSet());
        assertThat(stageIds).hasSizeGreaterThan(1);
        assertThat(plan).containsOnlyKeys(stageIds);

        // the output stage reads the stage of the fragment its remote source names
        assertThat(plan.get(queryId + ".0"))
                .filteredOn(node -> node.name().equals("RemoteSource"))
                .singleElement()
                .satisfies(remoteSource -> {
                    assertThat(remoteSource.descriptor()).containsEntry("sourceFragmentIds", "[1]");
                    assertThat(remoteSource.sources()).isEmpty();
                });
        assertThat(plan).containsKey(queryId + ".1");
    }

    @Test
    void testQueryPlanOfRunningQuery()
    {
        String queryId = startQuery(client, server, "SELECT * FROM tpch.sf100.lineitem");
        try {
            assertThat(queryPlan(queryId).get(queryId + ".0"))
                    .extracting(FlattenedPlanNode::name)
                    .containsExactly("Output", "TableScan");
        }
        finally {
            server.getDispatchManager().cancelQuery(new QueryId(queryId));
        }
    }

    @Test
    void testQueryPlanOfQueryFailedInAnalysis()
    {
        String queryId = runToCompletion(client, server, "SELECT * FROM tpch.tiny.unknown_table");

        assertThat(queryPlan(queryId)).isEmpty();
    }

    @Test
    void testQueryPlanAccessDenied()
    {
        String queryId = runToCompletion(client, server, "SELECT * FROM tpch.tiny.nation");

        server.getAccessControl().deny(privilege("query", VIEW_QUERY));
        try {
            assertThatThrownBy(() -> queryPlan(queryId))
                    .isInstanceOfSatisfying(UnexpectedResponseException.class, exception -> assertThat(exception.getStatusCode()).isEqualTo(403));
        }
        finally {
            server.getAccessControl().reset();
        }
    }

    @Test
    void testQueryPlanOfUnknownQuery()
    {
        assertThatThrownBy(() -> queryPlan("20260101_000000_00000_abcde"))
                .isInstanceOfSatisfying(UnexpectedResponseException.class, exception -> assertThat(exception.getStatusCode()).isEqualTo(410));
    }

    private Map<String, List<FlattenedPlanNode>> queryPlan(String queryId)
    {
        Request request = prepareGet()
                .setUri(server.getBaseUrl().resolve("/ui/api/query/" + queryId + "/plan"))
                .build();
        return client.execute(request, createJsonResponseHandler(QUERY_PLAN_CODEC));
    }
}
