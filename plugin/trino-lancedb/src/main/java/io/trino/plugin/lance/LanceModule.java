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
package io.trino.plugin.lance;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.lance.internal.LanceBind;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;


public class LanceModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final NodeManager nodeManager;

    public LanceModule(String catalogName, NodeManager nodeManager)
    {
        this.catalogName = catalogName;
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LanceConfig.class);
        binder.bind(LanceMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LanceSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LancePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(LanceSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(LanceNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(LanceNodePartitioningProvider.class).in(Scopes.SINGLETON);
        install(conditionalModule(
                LanceConfig.class,
                LanceConfig::isJni,
                new LanceJniModule(),
                new LanceHttpModule()));
    }

    public static class LanceJniModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            // TODO: install jni related binding
        }
    }

    public static class LanceHttpModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            httpClientBinder(binder).bindHttpClient("lance", LanceBind.class)
                    .withConfigDefaults(cfg -> {
                        cfg.setIdleTimeout(new Duration(300, SECONDS));
                        cfg.setConnectTimeout(new Duration(300, SECONDS));
                        cfg.setRequestTimeout(new Duration(300, SECONDS));
                        cfg.setMaxConnectionsPerServer(250);
                        cfg.setMaxContentLength(DataSize.of(32, MEGABYTE));
                        cfg.setSelectorCount(10);
                        cfg.setTimeoutThreads(8);
                        cfg.setTimeoutConcurrency(4);
                    });
        }
    }
}
