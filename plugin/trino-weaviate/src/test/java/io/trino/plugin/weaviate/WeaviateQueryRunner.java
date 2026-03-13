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
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class WeaviateQueryRunner
{
    static final String CATALOG_NAME = "weaviate";

    private WeaviateQueryRunner()
    {
    }

    static Builder builder(WeaviateServer weaviateServer)
    {
        HostAndPort httpAddress = weaviateServer.getHttpAddress();
        HostAndPort grpcAddress = weaviateServer.getGrpcAddress();
        return new Builder(weaviateServer)
                .addConnectorProperty("weaviate.scheme", "http")
                .addConnectorProperty("weaviate.http-host", "localhost")
                .addConnectorProperty("weaviate.grpc-host", "localhost")
                .addConnectorProperty("weaviate.http-port", String.valueOf(httpAddress.getPort()))
                .addConnectorProperty("weaviate.grpc-port", String.valueOf(grpcAddress.getPort()))
                .addConnectorProperty("weaviate.consistency-level", "ONE");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private final WeaviateServer weaviateServer;

        private Builder(WeaviateServer weaviateServer)
        {
            super(testSessionBuilder().setCatalog(CATALOG_NAME).setSchema("default").build());
            this.weaviateServer = weaviateServer;
        }

        private Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new WeaviatePlugin());
                queryRunner.createCatalog(CATALOG_NAME, "weaviate", connectorProperties);
                queryRunner.registerResource(weaviateServer);
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }
}
