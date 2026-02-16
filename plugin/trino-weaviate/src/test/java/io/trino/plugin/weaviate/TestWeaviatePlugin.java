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

import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestWeaviatePlugin
{
    @Test
    public void testDefaultConfiguration()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test",
                        Map.of(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testLocalConnection()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test",
                        Map.of(
                                "weaviate.http-host", "localhost",
                                "weaviate.grpc-host", "localhost",
                                "weaviate.http-port", "8080",
                                "weaviate.grpc-port", "50051"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testApiKeyAuthentication()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test",
                        Map.of(
                                "weaviate.http-host", "localhost",
                                "weaviate.grpc-host", "localhost",
                                "weaviate.http-port", "8080",
                                "weaviate.grpc-port", "50051",
                                "weaviate.auth.api-key", "xxx-xxx"),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new WeaviatePlugin().getConnectorFactories());
    }
}
