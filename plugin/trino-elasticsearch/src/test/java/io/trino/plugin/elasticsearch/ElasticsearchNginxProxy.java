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

import com.google.common.net.HostAndPort;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.NginxContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class ElasticsearchNginxProxy
{
    private static final int PROXY_PORT = 9201;
    private static final String NGINX_CONFIG_TEMPLATE =
            "limit_req_zone $binary_remote_addr zone=mylimit:1m rate=REQUEST_PER_SECONDr/s;\n" +
            "upstream elasticsearch {\n" +
            "  server elasticsearch-server:9200;\n" +
            "  keepalive 15;\n" +
            "}\n" +
            "server {\n" +
            "  access_log  /var/log/nginx/access.log  main;" +
            "  listen " + PROXY_PORT + ";\n" +
            "  location / {\n" +
            "    proxy_pass http://elasticsearch;\n" +
            "    proxy_redirect http://elasticsearch /;\n" +
            "    proxy_buffering off;\n" +
            "    proxy_http_version 1.1;\n" +
            "    proxy_set_header Connection \"Keep-Alive\";\n" +
            "    proxy_set_header Proxy-Connection \"Keep-Alive\";\n" +
            "    client_max_body_size 0;\n" +
            "  }\n" +
            "  location /_search/scroll {\n" +
            "    limit_req zone=mylimit;\n" +
            "    limit_req_status 429;\n" +
            "    proxy_pass http://elasticsearch;\n" +
            "    proxy_redirect http://elasticsearch /;\n" +
            "    proxy_buffering off;\n" +
            "    proxy_http_version 1.1;\n" +
            "    proxy_set_header Connection \"Keep-Alive\";\n" +
            "    proxy_set_header Proxy-Connection \"Keep-Alive\";\n" +
            "    client_max_body_size 0;\n" +
            "  }\n" +
            "  location ~ /.*/_search$ {\n" +
            "    limit_req zone=mylimit;\n" +
            "    limit_req_status 429;\n" +
            "    proxy_pass http://elasticsearch;\n" +
            "    proxy_redirect http://elasticsearch /;\n" +
            "    proxy_buffering off;\n" +
            "    proxy_http_version 1.1;\n" +
            "    proxy_set_header Connection \"Keep-Alive\";\n" +
            "    proxy_set_header Proxy-Connection \"Keep-Alive\";\n" +
            "    client_max_body_size 0;\n" +
            "  }\n" +
            "}\n";

    private final Path configurationPath;
    private final NginxContainer container;

    public ElasticsearchNginxProxy(Network network, int requestsPerSecond)
            throws IOException
    {
        container = new NginxContainer("nginx:1.19.8");
        container.withNetwork(network);
        container.withNetworkAliases("elasticsearch-proxy");
        // Create the Nginx configuration file on host and copy it into a predefined path the container
        configurationPath = Files.createTempDirectory("elasticsearchProxy");
        Path path = configurationPath.resolve("elasticsearch.conf");
        Files.writeString(path, NGINX_CONFIG_TEMPLATE.replace("REQUEST_PER_SECOND", String.valueOf(requestsPerSecond)), UTF_8);
        container.withCopyFileToContainer(forHostPath(path), "/etc/nginx/conf.d/elasticsearch.conf");
        container.addExposedPort(PROXY_PORT);
        container.start();
    }

    public void stop()
            throws IOException
    {
        container.close();
        deleteRecursively(configurationPath, ALLOW_INSECURE);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromString(container.getHost() + ":" + container.getMappedPort(PROXY_PORT));
    }
}
