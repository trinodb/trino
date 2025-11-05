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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ForDiscoveryClient;
import io.airlift.discovery.server.DynamicAnnouncementResource;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.discovery.server.ServiceResource;
import io.airlift.discovery.store.StoreResource;
import io.trino.failuredetector.FailureDetectorModule;
import io.trino.server.NodeResource;
import io.trino.server.ServerConfig;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
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
        install(new InternalCommunicationForDiscoveryModule(ForDiscoveryClient.class));
    }
}
