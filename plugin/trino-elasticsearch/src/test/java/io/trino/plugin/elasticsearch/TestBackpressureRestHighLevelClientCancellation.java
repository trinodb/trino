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
import com.google.common.collect.ImmutableMap;
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

import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_8_IMAGE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that cancelling a Trino SQL query cancels the in-flight
 * Elasticsearch search request.
 *
 * <p>Uses a runtime field that shadows a stored keyword property with
 * an expensive painless script. When Trino pushes down a
 * {@code WHERE slow_field = 'x'} predicate, ES evaluates the slow
 * script on every document, producing a long-running search through
 * {@code BackpressureRestHighLevelClient.search()}.
 *
 * <pre>
 * ./mvnw clean test -pl plugin/trino-elasticsearch -Dtest=TestBackpressureRestHighLevelClientCancellation -Dair.check.skip-all=true
 * </pre>
 */
final class TestBackpressureRestHighLevelClientCancellation
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

        // Define slow_field in both properties (so Trino discovers it as a column)
        // and runtime (so ES evaluates the expensive script when filtering on it).
        // The runtime definition shadows the stored property during query execution.
        Request createIndex = new Request("PUT", "/cancel_test");
        createIndex.setJsonEntity("""
                {
                    "mappings": {
                        "runtime": {
                            "slow_field": {
                                "type": "keyword",
                                "script": {
                                    "source": "String s = new String(); for (int i = 0; i < 999999; i++) { s = s + Integer.toString(i % 10); } emit(s.substring(0, 10));"
                                }
                            }
                        },
                        "properties": {
                            "name": { "type": "keyword" },
                            "slow_field": { "type": "keyword" }
                        }
                    }
                }""");
        esClient.performRequest(createIndex);

        Request put = new Request("PUT", "/cancel_test/_doc/1?refresh");
        put.setJsonEntity("{\"name\": \"test\", \"slow_field\": \"dummy\"}");
        esClient.performRequest(put);

        return ElasticsearchQueryRunner.builder(elasticsearch)
                // cancelling on trino should cancel on ES even when the request timeout is high
                .addConnectorProperties(ImmutableMap.of("elasticsearch.request-timeout", "5m"))
                .build();
    }

    @Test
    void testCancelledSqlQueryCancelsEsSearch()
            throws Exception
    {
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            executor.submit(() ->
                    getQueryRunner().execute(
                            testSessionBuilder()
                                    .setCatalog("elasticsearch")
                                    .setSchema("tpch")
                                    .build(),
                            "SELECT name FROM cancel_test WHERE slow_field = 'x'"));

            QueryManager queryManager = ((DistributedQueryRunner) getQueryRunner())
                    .getCoordinator().getQueryManager();

            // Wait for the query to appear in Elasticsearch (max 2s)
            for (int i = 0; i < 20 && getSearchTasks().isEmpty(); i++) {
                Thread.sleep(100);
            }
            assertThat(getSearchTasks()).isNotEmpty();

            for (BasicQueryInfo info : queryManager.getQueries()) {
                if (!info.getState().isDone()) {
                    queryManager.cancelQuery(info.getQueryId());
                }
            }

            // Give Elasticsearch 2s max to cancel the query
            for (int i = 0; i < 20 && !getSearchTasks().isEmpty(); i++) {
                Thread.sleep(100);
            }
            assertThat(getSearchTasks())
                    .as("Trino cancellation did not propagate to ES")
                    .isEmpty();
        }
    }

    private List<String> getSearchTasks()
            throws IOException
    {
        Request request = new Request("GET", "/_tasks");
        request.addParameter("actions", "*search*");
        request.addParameter("detailed", "true");
        JsonNode nodes = JSON.readTree(
                esClient.performRequest(request).getEntity().getContent()).get("nodes");
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
