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
package io.trino.plugin.varada.di;

import com.google.inject.Binder;
import io.trino.plugin.varada.annotations.ForWarmupRuleCloudFetcher;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.EmptyWarmupRuleFetcher;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleCloudFetcher;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleCloudFetcherConfiguration;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudvendors.CloudVendorModule;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class WarmupCloudFetcherModule
        implements InitializationModule
{
    private Map<String, String> config;
    private ConnectorContext context;
    private String catalogName;

    @SuppressWarnings("unused")
    public WarmupCloudFetcherModule() {}

    @SuppressWarnings("unused")
    public WarmupCloudFetcherModule(
            Map<String, String> config,
            ConnectorContext context,
            String catalogName)
    {
        this.config = requireNonNull(config);
        this.context = context;
        this.catalogName = catalogName;
    }

    @SuppressWarnings("unused")
    @Override
    public InitializationModule createModule(
            Map<String, String> config,
            ConnectorContext context,
            String catalogName)
    {
        return new WarmupCloudFetcherModule(config, context, catalogName);
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(WarmupRuleCloudFetcherConfiguration.class, ForWarmupRuleCloudFetcher.class);
        binder.install(
                CloudVendorModule.getModule(
                        context,
                        WarmupRuleCloudFetcherConfiguration.PREFIX,
                        ForWarmupRuleCloudFetcher.class,
                        catalogName,
                        config,
                        WarmupRuleCloudFetcherConfiguration.STORE_PATH,
                        WarmupRuleCloudFetcherConfiguration.STORE_TYPE,
                        WarmupRuleCloudFetcherConfiguration.class));

        if (VaradaBaseModule.isWorker(context, config)) {
            binder.bind(WarmupRuleFetcher.class).to(WarmupRuleCloudFetcher.class);
        }
        else {
            binder.bind(WarmupRuleFetcher.class).to(EmptyWarmupRuleFetcher.class);
        }
    }
}
