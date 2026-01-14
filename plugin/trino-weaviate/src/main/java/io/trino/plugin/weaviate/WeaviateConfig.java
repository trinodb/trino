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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel;
import jakarta.validation.constraints.NotNull;

public class WeaviateConfig
{
    private String scheme = "http";
    private String httpHost = "localhost";
    private int httpPort = 8080;
    private String grpcHost = "localhost";
    private int grpcPort = 50051;
    private Duration timeout;
    private ConsistencyLevel consistencyLevel;

    @NotNull
    public String getScheme()
    {
        return scheme;
    }

    @Config("weaviate.scheme")
    @ConfigDescription("Connection URL scheme (http/https)")
    public WeaviateConfig setScheme(String scheme)
    {
        this.scheme = scheme;
        return this;
    }

    @NotNull
    public String getHttpHost()
    {
        return httpHost;
    }

    @Config("weaviate.http-host")
    public WeaviateConfig setHttpHost(String httpHost)
    {
        this.httpHost = httpHost;
        return this;
    }

    public int getHttpPort()
    {
        return httpPort;
    }

    @Config("weaviate.http-port")
    public WeaviateConfig setHttpPort(int httpPort)
    {
        this.httpPort = httpPort;
        return this;
    }

    @NotNull
    public String getGrpcHost()
    {
        return grpcHost;
    }

    @Config("weaviate.grpc-host")
    public WeaviateConfig setGrpcHost(String grpcHost)
    {
        this.grpcHost = grpcHost;
        return this;
    }

    public int getGrpcPort()
    {
        return grpcPort;
    }

    @Config("weaviate.grpc-port")
    public WeaviateConfig setGrpcPort(int grpcPort)
    {
        this.grpcPort = grpcPort;
        return this;
    }

    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("weaviate.timeout")
    @ConfigDescription("Query timeout")
    public WeaviateConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Config("weaviate.consistency-level")
    @ConfigDescription("Consistency level for reads and writes")
    public WeaviateConfig setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
        return this;
    }
}
