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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.PASSWORD;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.USER;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_8_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Base64.getEncoder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

final class TestElasticsearchPathPrefix
        extends AbstractTestQueryFramework
{
    private static final String PATH_PREFIX = "/elasticsearch";

    private RestClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ElasticsearchServer elasticsearch = closeAfterClass(new ElasticsearchServer(ELASTICSEARCH_8_IMAGE));
        client = closeAfterClass(elasticsearch.getClient());
        PathPrefixProxy proxy = closeAfterClass(new PathPrefixProxy(PATH_PREFIX, elasticsearch.getAddress()));
        return ElasticsearchQueryRunner.builder(elasticsearch)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("elasticsearch.host", "127.0.0.1")
                        .put("elasticsearch.port", Integer.toString(proxy.getPort()))
                        .put("elasticsearch.path-prefix", PATH_PREFIX)
                        .put("elasticsearch.tls.enabled", "false")
                        .buildOrThrow())
                .build();
    }

    @Test
    void testSelectWithPathPrefix()
            throws IOException
    {
        String tableName = "path_prefix_" + randomNameSuffix();
        Request request = new Request("PUT", format("/%s/_doc/1?refresh", tableName));
        request.setJsonEntity("{\"name\": \"test\"}");
        client.performRequest(request);
        try {
            assertThat(query("SELECT name FROM " + tableName))
                    .matches("VALUES VARCHAR 'test'");
        }
        finally {
            client.performRequest(new Request("DELETE", "/" + tableName));
        }
    }

    private static final class PathPrefixProxy
            implements Closeable
    {
        private final String pathPrefix;
        private final HostAndPort elasticsearch;
        private final CloseableHttpClient httpClient;
        private final ExecutorService executor;
        private final HttpServer server;

        PathPrefixProxy(String pathPrefix, HostAndPort elasticsearch)
                throws IOException
        {
            this.pathPrefix = pathPrefix;
            this.elasticsearch = elasticsearch;
            this.httpClient = HttpClients.custom()
                    .setSSLContext(sslContext())
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .build();
            this.executor = newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setName("path-prefix-proxy");
                return thread;
            });
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        }

        int getPort()
        {
            return server.getAddress().getPort();
        }

        private void handle(HttpExchange exchange)
                throws IOException
        {
            String path = exchange.getRequestURI().getPath();
            if (!path.equals(pathPrefix) && !path.startsWith(pathPrefix + "/")) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }

            String backendPath = path.substring(pathPrefix.length());
            if (backendPath.isEmpty()) {
                backendPath = "/";
            }
            String query = exchange.getRequestURI().getRawQuery();
            URI uri = URI.create(format(
                    "https://%s:%s%s%s",
                    elasticsearch.getHost(),
                    elasticsearch.getPort(),
                    backendPath,
                    query == null ? "" : "?" + query));

            RequestBuilder requestBuilder = RequestBuilder.create(exchange.getRequestMethod()).setUri(uri);
            requestBuilder.addHeader(
                    "Authorization",
                    "Basic " + getEncoder().encodeToString(format("%s:%s", USER, PASSWORD).getBytes(UTF_8)));
            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            if (requestBody.length > 0) {
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                requestBuilder.setEntity(new ByteArrayEntity(
                        requestBody,
                        contentType == null ? ContentType.APPLICATION_OCTET_STREAM : ContentType.parse(contentType)));
            }

            try (CloseableHttpResponse response = httpClient.execute(requestBuilder.build())) {
                byte[] body = response.getEntity() == null ? new byte[0] : EntityUtils.toByteArray(response.getEntity());
                copyHeader(response, exchange, "Content-Type");
                copyHeader(response, exchange, "X-Elastic-Product");
                int status = response.getStatusLine().getStatusCode();
                exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
                if (body.length > 0) {
                    exchange.getResponseBody().write(body);
                }
            }
            catch (IOException _) {
                exchange.sendResponseHeaders(502, -1);
            }
            finally {
                exchange.close();
            }
        }

        private static void copyHeader(CloseableHttpResponse response, HttpExchange exchange, String name)
        {
            Header header = response.getFirstHeader(name);
            if (header != null) {
                exchange.getResponseHeaders().set(name, header.getValue());
            }
        }

        private static SSLContext sslContext()
        {
            try {
                return createSSLContext(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new File(getResource("truststore.jks").toURI())),
                        Optional.of("123456"));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            server.stop(0);
            executor.shutdownNow();
            httpClient.close();
        }
    }
}
