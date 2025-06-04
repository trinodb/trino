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
package io.trino.plugin.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.json.ObjectMapperProvider;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.http.HttpHost;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.trino.plugin.opensearch.OpenSearchServer.OPENSEARCH_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOpenSearchComplexTypePredicatePushDown
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private OpenSearchServer opensearch;
    private RestHighLevelClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        opensearch = closeAfterClass(new OpenSearchServer(OPENSEARCH_IMAGE, false, ImmutableMap.of()));
        HostAndPort address = opensearch.getAddress();
        client = closeAfterClass(new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort()))));
        return OpenSearchQueryRunner.builder(opensearch.getAddress()).build();
    }

    @Test
    void testRowTypeOnlyNullsRowGroupPruning()
            throws IOException
    {
        String tableName = "test_primitive_column_nulls_pruning_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "col": {
                            "type": "long"
                        }
                    }
                }
                """;
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < 4096; i++) {
            Map<String, Object> document = new HashMap<>();
            document.put("col", null);
            Map<String, Object> indexPayload = ImmutableMap.of("index", ImmutableMap.of("_index", tableName, "_id", String.valueOf(System.nanoTime())));
            String jsonDocument = OBJECT_MAPPER.writeValueAsString(document);;
            String jsonIndex = OBJECT_MAPPER.writeValueAsString(indexPayload);
            payload.append(jsonIndex).append("\n").append(jsonDocument).append("\n");
        }

        createIndex(tableName, properties);
        bulkIndex(tableName, payload.toString());

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col IS NOT NULL");

        tableName = "test_nested_column_nulls_pruning_" + randomNameSuffix();
        properties =
                """
                {
                    "_meta": {
                        "trino": {
                            "col": {
                                "b": {
                                    "isArray": true
                                }
                            }
                        }
                    },
                    "properties": {
                        "col": {
                            "properties": {
                                "a": {
                                    "type": "long"
                                },
                                "b" : {
                                    "type" : "double"
                                }
                            }
                        }
                    }
                }
                """;
        // Nested column `a` has nulls count of 4096 and contains only nulls
        // Nested column `b` also has nulls count of 4096, but it contains non nulls as well
        Random random = new Random();
        payload = new StringBuilder();
        for (int i = 0; i < 4096; i++) {
            Map<String, Object> document = new HashMap<>();
            Map<String, Object> inner = new HashMap<>();
            inner.put("a", null);
            List<Double> bArray = new ArrayList<>();
            bArray.add(null);
            bArray.add(random.nextDouble());
            inner.put("b", bArray);
            document.put("col", inner);
            Map<String, Object> indexPayload = ImmutableMap.of("index", ImmutableMap.of("_index", tableName, "_id", String.valueOf(System.nanoTime())));

            String jsonDocument = OBJECT_MAPPER.writeValueAsString(document);
            String jsonIndex = OBJECT_MAPPER.writeValueAsString(indexPayload);
            payload.append(jsonIndex).append("\n").append(jsonDocument).append("\n");
        }

        createIndex(tableName, properties);
        bulkIndex(tableName, payload.toString());

        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col.a IS NOT NULL");

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.a IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        // no predicate push down for the entire array type
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.b IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col.b IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no predicate push down for entire ROW
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col IS NOT NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(4096));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col IS NULL",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    @Test
    void testRowTypeRowGroupPruning()
            throws IOException
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                  """
                  {
                      "properties": {
                          "col1Row": {
                              "properties": {
                                  "a": {
                                      "type": "long"
                                  },
                                  "b": {
                                      "type": "long"
                                  },
                                  "c": {
                                      "properties": {
                                          "c1": {
                                              "type": "long"
                                          },
                                          "c2": {
                                              "properties": {
                                                  "c21": {
                                                      "type": "long"
                                                  },
                                                  "c22": {
                                                      "type": "long"
                                                  }
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
                  """;

        int a = 2;
        int b = 100;
        int c1 = 1;
        int c21 = 5;
        int c22 = 6;

        StringBuilder payload = new StringBuilder();
        for (int i=0; i<10000; i++) {
            Map<String, Object> document = ImmutableMap.<String, Object>builder()
                    .put("col1Row", ImmutableMap.<String, Object>builder()
                            .put("a", a)
                            .put("b", b)
                            .put("c", ImmutableMap.<String, Object>builder()
                                    .put("c1", c1)
                                    .put("c2", ImmutableMap.<String, Object>builder()
                                            .put("c21", c21)
                                            .put("c22", c22)
                                            .buildOrThrow())
                                    .buildOrThrow())
                            .buildOrThrow())
                    .buildOrThrow();
            Map<String, Object> indexPayload = ImmutableMap.of("index", ImmutableMap.of("_index", tableName, "_id", String.valueOf(System.nanoTime())));
            String jsonDocument = OBJECT_MAPPER.writeValueAsString(document);
            String jsonIndex = OBJECT_MAPPER.writeValueAsString(indexPayload);
            payload.append(jsonIndex).append("\n").append(jsonDocument).append("\n");

            a = a + 2;
            c1 = c1 + 1;
            c21 = c21 + 5;
            c22 = c22 + 6;
        }

        createIndex(tableName, properties);
        bulkIndex(tableName, payload.toString());

        // no data read since the row dereference predicate is pushed down
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = -1");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a IS NULL");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.c.c2.c22 = -1");
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.a = -1 AND col1ROW.b = -1 AND col1ROW.c.c1 = -1 AND col1Row.c.c2.c22 = -1");

        // read all since predicate case matches with the data
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.b = 100",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(10000));

        // no predicate push down for matching with ROW type, as file format only stores stats for primitives
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1))",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1)) OR col1Row.a = -1 ",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no data read since the row group get filtered by primitives in the predicate
        assertNoDataRead("SELECT * FROM " + tableName + " WHERE col1Row.c = ROW(-1, ROW(-1, -1)) AND col1Row.a = -1 ");

        // no predicate push down for entire ROW, as file format only stores stats for primitives
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE col1Row = ROW(-1, -1, ROW(-1, ROW(-1, -1)))",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        deleteIndex(tableName);
    }

    @Test
    void testArrayTypeRowGroupPruning()
            throws IOException
    {
        String tableName = "test_nested_column_pruning_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                  """
                  {
                      "_meta": {
                          "trino": {
                              "colArray": {
                                  "isArray": true
                              }
                          }
                      },
                      "properties": {
                          "colArray": {
                              "type": "long"
                          }
                      }
                  }
                  """;

        StringBuilder payload = new StringBuilder();
        for (int i=0; i<10000; i++) {
            Map<String, Object> document = ImmutableMap.<String, Object>builder()
                    .put("colArray", ImmutableList.<Long>builder()
                            .add(100L)
                            .add(200L)
                            .build())
                    .buildOrThrow();
            Map<String, Object> indexPayload = ImmutableMap.of("index", ImmutableMap.of("_index", tableName, "_id", String.valueOf(System.nanoTime())));

            String jsonDocument = OBJECT_MAPPER.writeValueAsString(document);
            String jsonIndex = OBJECT_MAPPER.writeValueAsString(indexPayload);
            payload.append(jsonIndex).append("\n").append(jsonDocument).append("\n");
        }
        createIndex(tableName, properties);
        bulkIndex(tableName, payload.toString());

        // no predicate push down for ARRAY type dereference
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colArray[1] = -1",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // no predicate push down for entire ARRAY type
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE colArray = ARRAY[-1, -1]",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        deleteIndex(tableName);
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        String mappings = indexMapping(properties);
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mappings);
        client.getLowLevelClient().performRequest(request);
    }

    private static String indexMapping(@Language("JSON") String properties)
    {
        return "{\"mappings\": " + properties + "}";
    }

    private void bulkIndex(String index, String payload)
            throws IOException
    {
        String endpoint = format("%s?refresh", bulkEndpoint(index));
        Request request = new Request("PUT", endpoint);
        request.setJsonEntity(payload);
        client.getLowLevelClient().performRequest(request);
    }

    private static String bulkEndpoint(String index)
    {
        return format("/%s/_bulk", index);
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        Request request = new Request("DELETE",  "/" + indexName);
        client.getLowLevelClient().performRequest(request);
    }
}
