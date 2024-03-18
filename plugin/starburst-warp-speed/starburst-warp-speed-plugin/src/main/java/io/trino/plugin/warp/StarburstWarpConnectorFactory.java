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
package io.trino.plugin.warp;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.di.DefaultFakeConnectorSessionProvider;
import io.trino.plugin.varada.di.FakeConnectorSessionProvider;
import io.trino.plugin.varada.di.InitializationModule;
import io.trino.plugin.varada.dispatcher.DispatcherConnectorFactory;
import io.trino.plugin.varada.execution.VaradaClient;
import io.trino.plugin.varada.util.UriUtils;
import io.trino.plugin.warp.extension.configuration.WarpExtensionConfiguration;
import io.trino.plugin.warp.extension.di.WarpEmptyExtensionModule;
import io.trino.plugin.warp.extension.di.WarpExtensionModule;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorInitializer;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorInitializer;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.varada.tools.configuration.MultiPrefixConfigurationWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class StarburstWarpConnectorFactory
        implements ConnectorFactory
{
    private final DispatcherConnectorFactory dispatcherConnectorFactory;
    private final LicenseManager licenseManager;
    private final List<Class<? extends InitializationModule>> extraModules;

    public StarburstWarpConnectorFactory(
            DispatcherConnectorFactory dispatcherConnectorFactory,
            LicenseManager licenseManager,
            List<Class<? extends InitializationModule>> extraModules)
    {
        this.dispatcherConnectorFactory = requireNonNull(dispatcherConnectorFactory);
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
        this.extraModules = requireNonNull(extraModules, "extraModules is null");
    }

    @Override
    public String getName()
    {
        return DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(licenseManager, "licenseManager is null");
        Map<String, String> configMap = new MultiPrefixConfigurationWrapper(new HashMap<>(config));

        if (!Boolean.parseBoolean(configMap.getOrDefault(WarpExtensionConfiguration.USE_HTTP_SERVER_PORT, Boolean.TRUE.toString()))) {
            String httpRestPortStr = VaradaClient.getRestHttpPortStr(configMap,
                    UriUtils.getHttpUri(context.getNodeManager().getCurrentNode()).getPort());

            configMap.put("http-server.http.port", httpRestPortStr);
            if (!config.containsKey(WarpExtensionConfiguration.HTTP_REST_PORT)) {
                configMap.put(WarpExtensionConfiguration.HTTP_REST_PORT, httpRestPortStr);
            }
        }

        List<Class<? extends InitializationModule>> extraModules = !this.extraModules.isEmpty() ?
                new ArrayList<>(this.extraModules) :
                new ArrayList<>(List.of(WarpCorkModule.class));
        return new StarburstWarpConnector(dispatcherConnectorFactory.create(
                catalogName,
                configMap,
                context,
                Optional.of(extraModules),
                Map.of(ProxiedConnectorConfiguration.DELTA_LAKE_CONNECTOR_NAME, DeltaLakeProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfiguration.HIVE_CONNECTOR_NAME, HiveProxiedConnectorInitializer.class.getName(),
                        ProxiedConnectorConfiguration.ICEBERG_CONNECTOR_NAME, IcebergProxiedConnectorInitializer.class.getName())));
    }

    public static class WarpCorkModule
            implements InitializationModule
    {
        private Map<String, String> config;
        private ConnectorContext connectorContext;
        private String catalogName;

        @SuppressWarnings("unused")
        public WarpCorkModule() {}

        public WarpCorkModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
        {
            this.config = requireNonNull(config);
            this.connectorContext = requireNonNull(connectorContext);
            this.catalogName = requireNonNull(catalogName);
        }

        @Override
        public Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName)
        {
            return new WarpCorkModule(config, connectorContext, catalogName);
        }

        @Override
        public void configure(Binder binder)
        {
            ConfigurationFactory configurationFactory = new ConfigurationFactory(config);
            WarpExtensionConfiguration warpExtensionConfiguration = configurationFactory.build(WarpExtensionConfiguration.class);
            if (warpExtensionConfiguration.isEnabled()) {
                binder.install(new WarpExtensionModule(config, connectorContext, catalogName));
            }
            else {
                binder.install(new WarpEmptyExtensionModule(config, connectorContext, catalogName));
            }

            binder.bind(FakeConnectorSessionProvider.class).to(DefaultFakeConnectorSessionProvider.class);
        }
    }
}
