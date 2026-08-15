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
package io.trino.plugin.elasticsearch.client;

import com.sun.net.httpserver.HttpServer;
import io.airlift.units.Duration;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchClient
{
    private static final String PATH_PREFIX = "/elasticsearch";
    private static final byte[] NODES_RESPONSE =
            """
            {
              "nodes": {
                "node-1": {
                  "roles": ["data"],
                  "http": {
                    "publish_address": "127.0.0.1:9200"
                  }
                }
              }
            }
            """.getBytes(UTF_8);

    @Test
    public void testPathPrefix()
            throws Exception
    {
        List<String> requestedPaths = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestedPaths.add(exchange.getRequestURI().getPath());
            if ((PATH_PREFIX + "/_nodes/http").equals(exchange.getRequestURI().getPath())) {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, NODES_RESPONSE.length);
                exchange.getResponseBody().write(NODES_RESPONSE);
            }
            else {
                exchange.sendResponseHeaders(404, -1);
            }
            exchange.close();
        });
        server.start();

        ElasticsearchConfig config = new ElasticsearchConfig()
                .setHosts(List.of("127.0.0.1"))
                .setPort(server.getAddress().getPort())
                .setPathPrefix(PATH_PREFIX)
                .setIgnorePublishAddress(true)
                .setMaxRetryTime(new Duration(2, SECONDS))
                .setRequestTimeout(new Duration(2, SECONDS));

        ElasticsearchClient client = new ElasticsearchClient(config, Optional.empty(), Optional.empty());
        try {
            client.initialize();
            assertThat(requestedPaths).contains(PATH_PREFIX + "/_nodes/http");
            assertThat(client.getNodes()).isNotEmpty();
        }
        finally {
            client.close();
            server.stop(0);
        }
    }
}
