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
import io.airlift.units.Duration;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AnnounceNodeInventoryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        boolean coordinator = buildConfigObject(ServerConfig.class).isCoordinator();
        if (coordinator) {
            jaxrsBinder(binder).bind(AnnounceNodeResource.class);

            binder.bind(AnnounceNodeInventory.class).in(Scopes.SINGLETON);
            binder.bind(NodeInventory.class).to(AnnounceNodeInventory.class).in(Scopes.SINGLETON);
        }

        // both coordinator and worker can announce, although coordinator normally does not
        binder.bind(Announcer.class).to(AnnounceNodeAnnouncer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(AnnounceNodeAnnouncerConfig.class);
        install(internalHttpClientModule("announcer", ForAnnouncer.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(3, SECONDS));
                    config.setRequestTimeout(new Duration(3, SECONDS));
                }).build());
    }
}
