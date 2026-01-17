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
package io.trino.plugin.weaviate;

import com.google.common.net.HostAndPort;
import io.weaviate.client6.v1.api.WeaviateClient;
import org.testcontainers.containers.Network;
import org.testcontainers.weaviate.WeaviateContainer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

public class WeaviateServer
        implements Closeable
{
    public static final String WEAVIATE_IMAGE = "semitechnologies/weaviate:1.35.2";

    private final WeaviateContainer container;

    public WeaviateServer()
            throws IOException
    {
        this(WEAVIATE_IMAGE);
    }

    public WeaviateServer(String image)
            throws IOException
    {
        this(Network.SHARED, image);
    }

    public WeaviateServer(Network network, String image)
            throws IOException
    {
        container = new WeaviateContainer(image);
        container.withNetwork(network);
        container.withNetworkAliases("weaviate-server");
        container.withStartupTimeout(Duration.ofMinutes(5));
    }

    public WeaviateClient getClient()
    {
        if (!container.isRunning()) {
            container.start();
        }
        HostAndPort http = getHttpAddress();
        HostAndPort grpc = getGrpcAddress();
        return WeaviateClient.connectToLocal(
                local -> local
                        .host(http.getHost())
                        .port(http.getPort())
                        .grpcPort(grpc.getPort()));
    }

    HostAndPort getHttpAddress()
    {
        return HostAndPort.fromString(container.getHttpHostAddress());
    }

    HostAndPort getGrpcAddress()
    {
        return HostAndPort.fromString(container.getGrpcHostAddress());
    }

    @Override
    public void close()
            throws IOException
    {
        container.close();
    }
}
