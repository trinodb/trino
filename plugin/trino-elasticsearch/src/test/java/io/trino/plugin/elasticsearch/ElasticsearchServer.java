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

import com.amazonaws.util.Base64;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import io.trino.testing.ResourcePresence;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.PASSWORD;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.USER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class ElasticsearchServer
{
    public static final String ELASTICSEARCH_7_IMAGE = "elasticsearch:7.16.2";
    public static final String ELASTICSEARCH_8_IMAGE = "elasticsearch:8.11.3";

    private final Path configurationPath;
    private final ElasticsearchContainer container;

    public ElasticsearchServer(String image)
            throws IOException
    {
        this(Network.SHARED, image);
    }

    public ElasticsearchServer(Network network, String image)
            throws IOException
    {
        DockerImageName dockerImageName = DockerImageName.parse(image).asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        container = new ElasticsearchContainer(dockerImageName);
        container.withNetwork(network);
        container.withNetworkAliases("elasticsearch-server");
        container.withStartupTimeout(Duration.ofMinutes(5));

        configurationPath = createTempDirectory(null);
        Map<String, String> configurationFiles = ImmutableMap.<String, String>builder()
                .put("elasticsearch.yml", loadResource("elasticsearch.yml"))
                .put("users", loadResource("users"))
                .put("users_roles", loadResource("users_roles"))
                .put("roles.yml", loadResource("roles.yml"))
                .put("ca.crt", loadResource("ca.crt"))
                .put("server.crt", loadResource("server.crt"))
                .put("server.key", loadResource("server.key"))
                .buildOrThrow();

        for (Map.Entry<String, String> entry : configurationFiles.entrySet()) {
            String name = entry.getKey();
            String contents = entry.getValue();

            Path path = configurationPath.resolve(name);
            Files.writeString(path, contents, UTF_8);
            container.withCopyFileToContainer(forHostPath(path), "/usr/share/elasticsearch/config/" + name);
        }
        container.start();
    }

    public void stop()
            throws IOException
    {
        container.close();
        deleteRecursively(configurationPath, ALLOW_INSECURE);
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromString(container.getHttpHostAddress());
    }

    public RestHighLevelClient getClient()
    {
        HostAndPort address = getAddress();
        return new RestHighLevelClientBuilder(RestClient.builder(new HttpHost(address.getHost(), address.getPort(), "https"))
                .setStrictDeprecationMode(false)
                .setHttpClientConfigCallback(ElasticsearchServer::enableSecureCommunication).build())
                .setApiCompatibilityMode(true) // Needed for 7.x client to work with 8.x server
                .build();
    }

    private static HttpAsyncClientBuilder enableSecureCommunication(HttpAsyncClientBuilder clientBuilder)
    {
        return clientBuilder.setSSLContext(getSSLContext())
                .setDefaultHeaders(ImmutableList.of(new BasicHeader("Authorization", format("Basic %s", Base64.encodeAsString(format("%s:%s", USER, PASSWORD).getBytes(StandardCharsets.UTF_8))))));
    }

    private static SSLContext getSSLContext()
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

    private static String loadResource(String file)
            throws IOException
    {
        return Resources.toString(getResource(file), UTF_8);
    }
}
