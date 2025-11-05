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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.client.NodeVersion;
import io.trino.server.security.SecurityConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestAnnounceNodeInventory
{
    private final HttpClient httpClient = new JettyHttpClient();

    @AfterAll
    void teardown()
    {
        httpClient.close();
    }

    @Test
    void simpleAnnounce()
    {
        try (AnnouncementServer server = createAnnounceServer()) {
            URI clientUri = URI.create("https://example.com:7777");
            AnnounceNodeAnnouncer announcer = new AnnounceNodeAnnouncer(
                    clientUri,
                    List.of(server.serverUri()),
                    false,
                    httpClient);
            getFutureValue(announcer.forceAnnounce());
            Set<URI> nodes = server.nodeInventory().getNodes();
            assertThat(nodes).containsExactly(clientUri);
        }
    }

    @Test
    void multipleAnnouncements()
    {
        try (AnnouncementServer server = createAnnounceServer()) {
            URI clientUriA = URI.create("https://example.com:7777");
            AnnounceNodeAnnouncer announcerA = new AnnounceNodeAnnouncer(
                    clientUriA,
                    List.of(server.serverUri()),
                    false,
                    httpClient);
            getFutureValue(announcerA.forceAnnounce());

            URI clientUriB = URI.create("https://example.com:8888");
            AnnounceNodeAnnouncer announcerB = new AnnounceNodeAnnouncer(
                    clientUriB,
                    List.of(server.serverUri()),
                    false,
                    httpClient);
            getFutureValue(announcerB.forceAnnounce());

            assertThat(server.nodeInventory().getNodes()).containsExactly(clientUriA, clientUriB);
        }
    }

    @Test
    void announceToMultipleServers()
    {
        try (AnnouncementServer serverA = createAnnounceServer();
                AnnouncementServer serverB = createAnnounceServer()) {
            URI clientUri = URI.create("https://example.com:7777");
            AnnounceNodeAnnouncer announcer = new AnnounceNodeAnnouncer(
                    clientUri,
                    List.of(serverA.serverUri(), serverB.serverUri()),
                    false,
                    httpClient);
            getFutureValue(announcer.forceAnnounce());
            assertThat(serverA.nodeInventory().getNodes()).containsExactly(clientUri);
            assertThat(serverA.nodeInventory().getNodes()).containsExactly(clientUri);
        }
    }

    @Test
    @Timeout(60)
    void fullIntegration()
            throws InterruptedException
    {
        try (AnnouncementServer server = createAnnounceServer()) {
            URI clientUriA = URI.create("https://example.com:7777");
            AnnounceNodeAnnouncer announcerA = new AnnounceNodeAnnouncer(
                    clientUriA,
                    List.of(server.serverUri()),
                    false,
                    httpClient);
            URI clientUriB = URI.create("https://example.com:8888");
            AnnounceNodeAnnouncer announcerB = new AnnounceNodeAnnouncer(
                    clientUriB,
                    List.of(server.serverUri()),
                    false,
                    httpClient);

            announcerA.start();
            announcerB.start();

            // wait for the announcements to be processed
            while (!server.nodeInventory().getNodes().equals(Set.of(clientUriA, clientUriB))) {
                Thread.sleep(10);
            }

            announcerB.stop();

            // stop B and ensure it is removed from the inventory
            // this takes 30 seconds
            while (!server.nodeInventory().getNodes().equals(Set.of(clientUriA))) {
                Thread.sleep(100);
            }
        }
    }

    private record AnnouncementServer(AnnounceNodeInventory nodeInventory, URI serverUri, Runnable onClose)
            implements Closeable
    {
        @Override
        public void close()
        {
            onClose.run();
        }
    }

    private static AnnouncementServer createAnnounceServer()
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new AnnounceNodeInventoryModule())
                .add(binder -> {
                    configBinder(binder).bindConfig(SecurityConfig.class);
                    binder.bind(InternalNode.class).toInstance(new InternalNode(
                            "test-node-id",
                            URI.create("https://example.com:1234"),
                            new NodeVersion("test-version"),
                            true));
                });
        Injector injector = new Bootstrap(modules.build())
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        AnnounceNodeInventory nodeInventory = injector.getInstance(AnnounceNodeInventory.class);
        URI serverUri = injector.getInstance(HttpServerInfo.class).getHttpUri();
        return new AnnouncementServer(
                nodeInventory,
                serverUri,
                () -> injector.getInstance(LifeCycleManager.class).stop());
    }
}
