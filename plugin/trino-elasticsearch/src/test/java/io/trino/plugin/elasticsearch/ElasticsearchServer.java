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
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class ElasticsearchServer
{
    private final Path configurationPath;
    private final ElasticsearchContainer container;

    public ElasticsearchServer(String image, Map<String, String> configurationFiles)
            throws IOException
    {
        this(Network.SHARED, image, configurationFiles);
    }

    public ElasticsearchServer(Network network, String image, Map<String, String> configurationFiles)
            throws IOException
    {
        DockerImageName dockerImageName = DockerImageName.parse(image).asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        container = new ElasticsearchContainer(dockerImageName);
        container.withNetwork(network);
        container.withNetworkAliases("elasticsearch-server");

        configurationPath = createTempDirectory(null);
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

    public HostAndPort getAddress()
    {
        return HostAndPort.fromString(container.getHttpHostAddress());
    }
}
