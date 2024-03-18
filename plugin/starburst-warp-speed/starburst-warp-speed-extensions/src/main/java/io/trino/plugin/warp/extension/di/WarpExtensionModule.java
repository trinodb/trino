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
package io.trino.plugin.warp.extension.di;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import io.trino.plugin.varada.di.InitializationModule;
import io.trino.plugin.varada.di.VaradaBaseModule;
import io.trino.plugin.varada.di.VaradaClientModule;
import io.trino.plugin.varada.di.WarmupCloudFetcherModule;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.EmptyWarmupRuleFetcher;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfiguration;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.warp.extension.configuration.CallHomeConfiguration;
import io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration;
import io.trino.plugin.warp.extension.execution.ClusterReadyTaskExecutionIsAllowedSupplier;
import io.trino.plugin.warp.extension.execution.VaradaTasksModule;
import io.trino.plugin.warp.extension.execution.WorkerReadyTaskExecutionIsAllowedSupplier;
import io.trino.plugin.warp.extension.execution.callhome.CallHomeService;
import io.trino.plugin.warp.extension.warmup.WorkerWarmupRuleFetcher;
import io.trino.spi.connector.ConnectorContext;
import io.varada.tools.util.StringUtils;

import java.util.Map;
import java.util.function.BooleanSupplier;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class WarpExtensionModule
        extends AbstractModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext connectorContext;
    private String catalogName;

    @SuppressWarnings("unused")
    public WarpExtensionModule() {}

    public WarpExtensionModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        this.config = requireNonNull(config);
        this.connectorContext = requireNonNull(connectorContext);
        this.catalogName = requireNonNull(catalogName);
    }

    @Override
    public void configure()
    {
        binder().install(new VaradaClientModule(config));

        ImmutableSet.Builder<Class<? extends BooleanSupplier>> booleanSuppliers = ImmutableSet.builder();
        if (connectorContext.getNodeManager().getCurrentNode().isCoordinator()) {
            booleanSuppliers.add(ClusterReadyTaskExecutionIsAllowedSupplier.class);
        }
        else {
            booleanSuppliers.add(WorkerReadyTaskExecutionIsAllowedSupplier.class);
        }
        binder().install(
                new VaradaTasksModule(
                        VaradaBaseModule.isCoordinator(connectorContext),
                        VaradaBaseModule.isWorker(connectorContext, config),
                        booleanSuppliers.build()));
        if (!Boolean.parseBoolean(config.getOrDefault(WarpExtensionConfiguration.USE_HTTP_SERVER_PORT, Boolean.TRUE.toString()))) {
            configureHttpServer();
        }

        binder().bind(CallHomeService.class);
        configBinder(binder()).bindConfig(WarpExtensionConfiguration.class);
        configBinder(binder()).bindConfig(CallHomeConfiguration.class);

        if (VaradaBaseModule.isWorker(connectorContext, config)) {
            binder().bind(WarmupRuleFetcher.class).to(WorkerWarmupRuleFetcher.class);
        }
        else {
            ConfigurationFactory configurationFactory = new ConfigurationFactory(config);
            WarmupRuleCloudFetcherConfiguration warmupRuleCloudFetcherConfiguration = configurationFactory.build(WarmupRuleCloudFetcherConfiguration.class);
            if (StringUtils.isEmpty(warmupRuleCloudFetcherConfiguration.getStorePath())) {
                binder().bind(WarmupRuleFetcher.class).to(EmptyWarmupRuleFetcher.class);
            }
            else {
                binder().install(new WarmupCloudFetcherModule(config, connectorContext, catalogName));
            }
        }
    }

    @Override
    public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        return new WarpExtensionModule(config, connectorContext, catalogName);
    }

    private void configureHttpServer()
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(config);
        JaxrsModule jaxrsModule = new JaxrsModule();
        configurationFactory.registerConfigurationClasses(jaxrsModule);
        HttpServerModule httpServerModule = new HttpServerModule();
        configurationFactory.registerConfigurationClasses(httpServerModule);
        binder().install(new NodeModule());
        binder().install(httpServerModule);
        binder().install(new JsonModule());
        VaradaJaxrsModule module = new VaradaJaxrsModule();
        module.setConfigurationFactory(configurationFactory);
        binder().install(module);
        binder().install(binder1 -> binder1.bind(HttpServerLifeCycleHandler.class));
    }
}
