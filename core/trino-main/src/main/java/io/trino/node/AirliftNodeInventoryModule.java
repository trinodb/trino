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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.server.DynamicAnnouncementResource;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.discovery.server.ServiceResource;
import io.airlift.discovery.store.StoreResource;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.trino.failuredetector.FailureDetectorModule;
import io.trino.server.InternalAuthenticationManager;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.NodeResource;
import io.trino.server.ServerConfig;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.node.AddressToHostname.encodeAddressAsHostname;
import static io.trino.server.InternalCommunicationHttpClientModule.configureClient;
import static io.trino.server.security.ResourceSecurityBinder.resourceSecurityBinder;

public class AirliftNodeInventoryModule
        extends AbstractConfigurationAwareModule
{
    private final String nodeVersion;

    public AirliftNodeInventoryModule(String nodeVersion)
    {
        this.nodeVersion = nodeVersion;
    }

    @Override
    protected void setup(Binder binder)
    {
        boolean coordinator = buildConfigObject(ServerConfig.class).isCoordinator();
        if (coordinator) {
            if (buildConfigObject(EmbeddedDiscoveryConfig.class).isEnabled()) {
                install(new EmbeddedDiscoveryModule());
            }

            binder.bind(NodeInventory.class).to(AirliftNodeInventory.class).in(Scopes.SINGLETON);

            // selector
            discoveryBinder(binder).bindSelector("trino");

            // coordinator announcement
            discoveryBinder(binder).bindHttpAnnouncement("trino-coordinator");

            // failure detector
            install(new FailureDetectorModule());
            jaxrsBinder(binder).bind(NodeResource.class);

            // server security configuration
            resourceSecurityBinder(binder)
                    .managementReadResource(ServiceResource.class)
                    .internalOnlyResource(DynamicAnnouncementResource.class)
                    .internalOnlyResource(StoreResource.class);
        }

        // both coordinator and worker must announce
        install(new DiscoveryModule());
        binder.bind(Announcer.class).to(AirliftAnnouncer.class).in(Scopes.SINGLETON);
        discoveryBinder(binder).bindHttpAnnouncement("trino")
                .addProperty("node_version", nodeVersion)
                .addProperty("coordinator", String.valueOf(coordinator));

        // internal communication setup for discovery http client
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        Multibinder<HttpRequestFilter> discoveryFilterBinder = newSetBinder(binder, HttpRequestFilter.class, ForDiscoveryClient.class);
        if (internalCommunicationConfig.isHttpsRequired() && internalCommunicationConfig.getKeyStorePath() == null && internalCommunicationConfig.getTrustStorePath() == null) {
            discoveryFilterBinder.addBinding().to(DiscoveryEncodeAddressAsHostname.class);
        }
        configBinder(binder).bindConfigDefaults(HttpClientConfig.class, ForDiscoveryClient.class, config -> configureClient(config, internalCommunicationConfig));
        discoveryFilterBinder.addBinding().to(InternalAuthenticationManager.class);
    }

    private static class DiscoveryEncodeAddressAsHostname
            implements HttpRequestFilter
    {
        @Override
        public Request filterRequest(Request request)
        {
            return Request.Builder.fromRequest(request)
                    .setUri(toIpEncodedAsHostnameUri(request.getUri()))
                    .build();
        }

        private static URI toIpEncodedAsHostnameUri(URI uri)
        {
            if (!uri.getScheme().equals("https")) {
                return uri;
            }
            try {
                String host = uri.getHost();
                InetAddress inetAddress = InetAddress.getByName(host);
                String addressAsHostname = encodeAddressAsHostname(inetAddress);
                return new URI(uri.getScheme(), uri.getUserInfo(), addressAsHostname, uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
            }
            catch (UnknownHostException e) {
                throw new UncheckedIOException(e);
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
