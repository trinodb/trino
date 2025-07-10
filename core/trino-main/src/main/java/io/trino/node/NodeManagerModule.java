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

import static io.airlift.configuration.SwitchModule.switchModule;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class NodeManagerModule
        extends AbstractConfigurationAwareModule
{
    private final String nodeVersion;

    public NodeManagerModule(String nodeVersion)
    {
        this.nodeVersion = nodeVersion;
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        if (serverConfig.isCoordinator()) {
            binder.bind(CoordinatorNodeManager.class).in(Scopes.SINGLETON);
            binder.bind(InternalNodeManager.class).to(CoordinatorNodeManager.class).in(Scopes.SINGLETON);
            newExporter(binder).export(CoordinatorNodeManager.class).withGeneratedName();
            install(internalHttpClientModule("node-manager", ForNodeManager.class)
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(30, SECONDS));
                        config.setRequestTimeout(new Duration(10, SECONDS));
                    }).build());
        }
        else {
            binder.bind(InternalNodeManager.class).to(WorkerInternalNodeManager.class).in(Scopes.SINGLETON);
        }

        install(switchModule(
                NodeInventoryConfig.class,
                NodeInventoryConfig::getType,
                type -> switch (type) {
                    case AIRLIFT_DISCOVERY -> new AirliftNodeInventoryModule(nodeVersion);
                    case ANNOUNCE -> new AnnounceNodeInventoryModule();
                    case DNS -> new DnsNodeInventoryModule();
                }));
    }
}
