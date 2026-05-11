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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.json.JsonMapperProvider;
import io.trino.execution.QueryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.trino.execution.QueryState.RUNNING;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_8_IMAGE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * When Trino cancels a query, ES should stop executing it.
 *
 * <pre>
 * ./mvnw test -pl plugin/trino-elasticsearch -Dtest=TestElasticsearchQueryTimeout -Dair.check.skip-all=true
 * </pre>
 */
final class TestElasticsearchQueryTimeout
        extends AbstractTestQueryFramework
{
    private static final JsonMapper JSON = new JsonMapperProvider().get();

    private RestClient esClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ElasticsearchServer elasticsearch = closeAfterClass(new ElasticsearchServer(ELASTICSEARCH_8_IMAGE));
        esClient = closeAfterClass(elasticsearch.getClient().getLowLevelClient());
        return ElasticsearchQueryRunner.builder(elasticsearch).build();
    }

    @Test
    void testCancelledQueryStopsRunningOnEs()
            throws Exception
    {
        Request put = new Request("PUT", "/test/_doc/1?refresh");
        put.setJsonEntity("{\"v\":1}");
        esClient.performRequest(put);

        try {
            // O(n²) string concat in painless: runs for minutes on a single document.
            String esQuery = """
                    {
                        "query": {
                            "script_score": {
                                "query": {"match_all": {}},
                                "script": {
                                    "source": "String s = new String(); for (int i = 0; i < 999999; i++) { s = s + Integer.toString(i % 10); } return s.length();"
                                }
                            }
                        }
                    }""".replace("\n", " ");

            // Start the query in the background
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> future = executor.submit(() ->
                    getQueryRunner().execute(
                            testSessionBuilder()
                                    .setCatalog("elasticsearch").setSchema("tpch")
                                    .build(),
                            "SELECT result FROM TABLE(elasticsearch.system.raw_query(" +
                                    "schema => 'tpch', " +
                                    "index => 'test', " +
                                    "query => '" + esQuery + "'" +
                                    "))"));

            // Wait for ES to actually be executing the search
            QueryManager queryManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager();
            while (getSearchTasks().isEmpty()) {
                assertThat(future.isDone()).as("query should still be running").isFalse();
                Thread.sleep(100);
            }

            // Cancel the query
            for (BasicQueryInfo info : queryManager.getQueries()) {
                if (!info.getState().isDone()) {
                    System.out.println("Cancelling query " + info.getQueryId());
                    queryManager.cancelQuery(info.getQueryId());
                }
            }
            try { future.get(10, TimeUnit.SECONDS); }
            catch (Exception _) { /* expected */ }
            executor.shutdown();

            Thread.sleep(5000);

            List<String> tasks = getSearchTasks();
            System.out.println("ES search tasks 5s after cancellation: " + tasks);

            assertThat(tasks)
                    .as("ES should have no active search tasks 5s after Trino cancelled the query")
                    .isEmpty();
        }
        finally {
            esClient.performRequest(new Request("DELETE", "/test"));
        }
    }

    private List<String> getSearchTasks()
            throws IOException
    {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("actions", "*search*");
        request.addParameter("detailed", "true");
        JsonNode nodes = JSON.readTree(
                esClient.performRequest(request)
                        .getEntity().getContent()).get("nodes");
        List<String> descriptions = new ArrayList<>();
        if (nodes != null) {
            for (Iterator<JsonNode> it = nodes.elements(); it.hasNext(); ) {
                JsonNode tasks = it.next().get("tasks");
                if (tasks != null) {
                    for (Iterator<JsonNode> taskIt = tasks.elements(); taskIt.hasNext(); ) {
                        JsonNode task = taskIt.next();
                        if (!task.path("cancelled").asBoolean(false)) {
                            descriptions.add(task.get("description").asText());
                        }
                    }
                }
            }
        }
        return descriptions;
    }
}
