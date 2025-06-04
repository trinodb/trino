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
package io.trino.failuredetector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.execution.QueryManagerConfig;
import io.trino.failuredetector.HeartbeatFailureDetector.Stats;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.security.SecurityConfig;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.net.URI;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.discovery.client.ServiceTypes.serviceType;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHeartbeatFailureDetector
{
    @Test
    public void testExcludesCurrentNode()
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingJmxModule(),
                new TestingDiscoveryModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new FailureDetectorModule(),
                binder -> {
                    configBinder(binder).bindConfig(SecurityConfig.class);
                    configBinder(binder).bindConfig(InternalCommunicationConfig.class);
                    configBinder(binder).bindConfig(QueryManagerConfig.class);
                    discoveryBinder(binder).bindSelector("trino");
                    discoveryBinder(binder).bindHttpAnnouncement("trino");

                    // Jersey with jetty 9 requires at least one resource
                    // todo add a dummy resource to airlift jaxrs in this case
                    jaxrsBinder(binder).bind(FooResource.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        ServiceSelector selector = injector.getInstance(Key.get(ServiceSelector.class, serviceType("trino")));
        assertThat(selector.selectAllServices()).hasSize(1);

        HeartbeatFailureDetector detector = injector.getInstance(HeartbeatFailureDetector.class);
        detector.updateMonitoredServices();

        assertThat(detector.getTotalCount()).isEqualTo(0);
        assertThat(detector.getActiveCount()).isEqualTo(0);
        assertThat(detector.getFailedCount()).isEqualTo(0);
        assertThat(detector.getFailed()).isEmpty();
    }

    @Test
    public void testHeartbeatStatsSerialization()
            throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        Stats stats = new Stats(new URI("http://example.com"));
        String serialized = objectMapper.writeValueAsString(stats);
        JsonNode deserialized = objectMapper.readTree(serialized);
        assertThat(deserialized.has("lastFailureInfo")).isFalse();

        stats.recordFailure(new SocketTimeoutException("timeout"));
        serialized = objectMapper.writeValueAsString(stats);
        deserialized = objectMapper.readTree(serialized);
        assertThat(deserialized.get("lastFailureInfo").isNull()).isFalse();
        assertThat(deserialized.get("lastFailureInfo").get("type").asText()).isEqualTo(SocketTimeoutException.class.getName());
    }

    @Path("/foo")
    public static final class FooResource
    {
        @GET
        public static String hello()
        {
            return "hello";
        }
    }
}
