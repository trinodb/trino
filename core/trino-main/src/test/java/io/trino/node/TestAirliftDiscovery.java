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
package io.trino.node;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.server.DynamicAnnouncement;
import io.airlift.discovery.server.DynamicServiceAnnouncement;
import io.airlift.discovery.server.DynamicStore;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.discovery.server.Id;
import io.airlift.discovery.server.Node;
import io.airlift.discovery.server.Service;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracing.TracingModule;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class TestAirliftDiscovery
{
    @Test
    void testEmbeddedDiscovery()
            throws Exception
    {
        try (DiscoveryServer discovery = new DiscoveryServer();
                TestingTrinoServer server = TestingTrinoServer.builder()
                        .setEnvironment("test")
                        .addProperty("discovery.type", "AIRLIFT_DISCOVERY")
                        .addProperty("discovery.uri", discovery.getBaseUrl().toString())
                        .addProperty("discovery.store-cache-ttl", "0ms")
                        .build()) {
            NodeInventory inventory = server.getInstance(Key.get(NodeInventory.class));
            assertThat(inventory.getNodes()).contains(server.getBaseUrl());

            DynamicStore store = server.getInstance(Key.get(DynamicStore.class));
            Id<Node> nodeId = Id.random();
            store.put(nodeId, new DynamicAnnouncement("test", "test", "/test", ImmutableSet.of(
                    new DynamicServiceAnnouncement(Id.random(), "test-service", ImmutableMap.of()))));
            assertThat(store.get("test-service")).extracting(Service::getNodeId).containsExactly(nodeId);
            store.delete(nodeId);
            assertThat(store.get("test-service")).isEmpty();
        }
    }

    private static class DiscoveryServer
            implements AutoCloseable
    {
        private final LifeCycleManager lifeCycleManager;
        private final URI baseUrl;

        public DiscoveryServer()
        {
            Injector injector = new Bootstrap(
                    new MBeanModule(),
                    new TestingNodeModule("test"),
                    new TestingHttpServerModule("testing-discovery"),
                    new JsonModule(),
                    new JaxrsModule(),
                    new TestingJmxModule(),
                    new TracingModule("discovery", "testversion"),
                    new DiscoveryModule(),
                    new EmbeddedDiscoveryModule())
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperty("discovery.store-cache-ttl", "0ms")
                    .quiet()
                    .initialize();
            lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            baseUrl = injector.getInstance(TestingHttpServer.class).getBaseUrl();
        }

        public URI getBaseUrl()
        {
            return baseUrl;
        }

        @Override
        public void close()
                throws Exception
        {
            lifeCycleManager.stop();
        }
    }
}
