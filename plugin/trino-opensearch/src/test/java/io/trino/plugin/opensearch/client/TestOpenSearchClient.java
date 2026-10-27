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
package io.trino.plugin.opensearch.client;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.decoders.TimestampDecoder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.SearchHit;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOpenSearchClient
{
    @Test
    void testSearchAndScroll()
            throws Exception
    {
        assertThat(getClass().getClassLoader().getResource("org/joda/time/DateTime.class")).isNull();
        TestingServlet servlet = new TestingServlet();
        Injector injector = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule("opensearch-client"),
                binder -> binder.bind(Servlet.class).toInstance(servlet))
                .doNotInitializeLogging()
                .disableSystemProperties()
                .quiet()
                .initialize();
        URI uri = injector.getInstance(HttpServerInfo.class).getHttpUri();
        OpenSearchClient client = new OpenSearchClient(
                new OpenSearchConfig().setHosts(List.of(uri.getHost())).setPort(uri.getPort()),
                Optional.empty(),
                Optional.empty());
        try {
            client.initialize();
            assertThat(client.getNodes()).containsExactly(new OpenSearchNode("test", Optional.empty()));

            SearchResponse response = client.beginSearch(
                    "test",
                    0,
                    new RangeQueryBuilder("timestamp").gte("2025-01-01T00:00:00Z").lt("2026-01-01T00:00:00Z").timeZone("UTC"),
                    Optional.of(List.of("timestamp")),
                    List.of("timestamp"),
                    Optional.empty(),
                    OptionalLong.of(1));
            assertThat(servlet.searchRequest.get()).contains("2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "docvalue_fields");
            assertThat(response.getScrollId()).isEqualTo("scroll-id");
            SearchHit hit = response.getHits().getAt(0);
            assertThat(hit.getSourceAsMap()).containsExactly(Map.entry("timestamp", "2025-01-01T00:00:00Z"));
            BlockBuilder output = TIMESTAMP_MILLIS.createFixedSizeBlockBuilder(1);
            new TimestampDecoder("timestamp").decode(hit, () -> hit.getSourceAsMap().get("timestamp"), output);
            assertThat(TIMESTAMP_MILLIS.getLong(output.build(), 0)).isEqualTo(1_735_689_600_000_000L);

            assertThat(client.nextPage(response.getScrollId()).getHits().getHits()).isEmpty();
            client.clearScroll(response.getScrollId());
            assertThatThrownBy(() -> client.beginSearch("invalid", 0, new RangeQueryBuilder("timestamp").gte("invalid"), Optional.empty(), List.of(), Optional.empty(), OptionalLong.empty()))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("invalid query");
        }
        finally {
            client.close();
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    private static class TestingServlet
            extends HttpServlet
    {
        private final AtomicReference<String> searchRequest = new AtomicReference<>();

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            response.setContentType("application/json");
            String path = request.getRequestURI();
            if (path.equals("/_nodes/http")) {
                response.getWriter().write("{\"nodes\":{\"test\":{\"roles\":[\"data\"]}}}");
            }
            else if (path.equals("/test/_search")) {
                searchRequest.set(request.getReader().readAllAsString());
                response.getWriter().write(
                        """
                        {
                          "took":1,
                          "timed_out":false,
                          "_scroll_id":"scroll-id",
                          "_shards":{"total":1,"successful":1,"skipped":0,"failed":0},
                          "hits":{
                            "total":{"value":1,"relation":"eq"},
                            "max_score":1,
                            "hits":[{
                              "_index":"test","_id":"1","_score":1,
                              "_source":{"timestamp":"2025-01-01T00:00:00Z"},
                              "fields":{"timestamp":["2025-01-01T00:00:00.000Z"]}
                            }]
                          }
                        }
                        """);
            }
            else if (path.equals("/invalid/_search")) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write(
                        """
                        {
                          "error":{
                            "root_cause":[{"type":"parsing_exception","reason":"invalid query","line":1,"col":1}],
                            "type":"parsing_exception","reason":"invalid query","line":1,"col":1
                          },
                          "status":400
                        }
                        """);
            }
            else if (path.equals("/_search/scroll") && request.getMethod().equals("DELETE")) {
                response.getWriter().write("{\"succeeded\":true,\"num_freed\":1}");
            }
            else if (path.equals("/_search/scroll")) {
                response.getWriter().write(
                        """
                        {
                          "took":1,
                          "timed_out":false,
                          "_scroll_id":"scroll-id",
                          "_shards":{"total":1,"successful":1,"skipped":0,"failed":0},
                          "hits":{"total":{"value":1,"relation":"eq"},"max_score":null,"hits":[]}
                        }
                        """);
            }
            else {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }
}
