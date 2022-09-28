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
package io.trino.execution.scheduler;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.execution.scheduler.TopologyAwareNodeSelectorConfig.TopologyType.FILE;
import static io.trino.execution.scheduler.TopologyAwareNodeSelectorConfig.TopologyType.FLAT;
import static io.trino.execution.scheduler.TopologyAwareNodeSelectorConfig.TopologyType.SUBNET;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class TopologyAwareNodeSelectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TopologyAwareNodeSelectorConfig.class);
        bindNetworkTopology();
        binder.bind(TopologyAwareNodeSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(NodeSelectorFactory.class).to(TopologyAwareNodeSelectorFactory.class).in(Scopes.SINGLETON);
        binder.bind(NodeSchedulerExporter.class).in(Scopes.SINGLETON);
    }

    private void bindNetworkTopology()
    {
        install(conditionalModule(
                TopologyAwareNodeSelectorConfig.class,
                config -> config.getType() == FLAT,
                binder -> binder.bind(NetworkTopology.class).to(FlatNetworkTopology.class).in(Scopes.SINGLETON)));

        install(conditionalModule(
                TopologyAwareNodeSelectorConfig.class,
                config -> config.getType() == FILE,
                binder -> {
                    configBinder(binder).bindConfig(TopologyFileConfig.class);
                    binder.bind(NetworkTopology.class).to(FileBasedNetworkTopology.class).in(Scopes.SINGLETON);
                    newExporter(binder).export(FileBasedNetworkTopology.class).withGeneratedName();
                }));

        install(conditionalModule(
                TopologyAwareNodeSelectorConfig.class,
                config -> config.getType() == SUBNET,
                binder -> {
                    configBinder(binder).bindConfig(SubnetTopologyConfig.class);
                    binder.bind(NetworkTopology.class).to(SubnetBasedTopology.class).in(Scopes.SINGLETON);
                }));
    }
}
