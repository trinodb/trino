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

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.varada.di.InitializationModule;
import io.trino.plugin.varada.di.WarmupCloudFetcherModule;
import io.trino.plugin.varada.dispatcher.connectors.ConnectorTaskExecutor;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.EmptyWarmupRuleFetcher;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfiguration;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.spi.connector.ConnectorContext;
import io.varada.tools.util.StringUtils;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class WarpEmptyExtensionModule
        extends AbstractModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext connectorContext;
    private String catalogName;

    @SuppressWarnings("unused")
    public WarpEmptyExtensionModule() {}

    @SuppressWarnings("unused")
    public WarpEmptyExtensionModule(
            Map<String, String> config,
            ConnectorContext connectorContext,
            String catalogName)
    {
        this.config = requireNonNull(config);
        this.connectorContext = requireNonNull(connectorContext);
        this.catalogName = requireNonNull(catalogName);
    }

    @Override
    public void configure()
    {
        binder().bind(ConnectorTaskExecutor.class).to(EmptyTaskExecutor.class);

        ConfigurationFactory configurationFactory = new ConfigurationFactory(config);
        WarmupRuleCloudFetcherConfiguration warmupRuleCloudFetcherConfiguration = configurationFactory.build(WarmupRuleCloudFetcherConfiguration.class);
        if (StringUtils.isEmpty(warmupRuleCloudFetcherConfiguration.getStorePath())) {
            binder().bind(WarmupRuleFetcher.class).to(EmptyWarmupRuleFetcher.class);
        }
        else {
            binder().install(new WarmupCloudFetcherModule(config, connectorContext, catalogName));
        }
    }

    @Override
    public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
    {
        return new WarpEmptyExtensionModule(config, connectorContext, catalogName);
    }

    static class EmptyTaskExecutor
            implements ConnectorTaskExecutor
    {
        @Override
        public Object executeTask(String taskName, String dataStr, String httpMethod)
        {
            return null;
        }
    }
}
