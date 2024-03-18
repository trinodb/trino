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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigurationUtils;
import io.airlift.event.client.EventModule;
import io.airlift.log.Logger;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.di.VaradaModules;
import io.trino.plugin.varada.di.dispatcher.DispatcherCoordinatorModule;
import io.trino.plugin.varada.di.dispatcher.DispatcherMainModule;
import io.trino.plugin.varada.dispatcher.connectors.DispatcherConnectorBase;
import io.trino.spi.NodeManager;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;
import io.varada.tools.configuration.MultiPrefixConfigurationWrapper;
import io.varada.tools.util.Pair;
import org.weakref.jmx.guice.MBeanModule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class InternalDispatcherConnectorFactory
{
    public static final String WARP_PREFIX = "WARP__";
    private static final Logger logger = Logger.get(InternalDispatcherConnectorFactory.class);

    private InternalDispatcherConnectorFactory() {}

    @SuppressWarnings({"unused", "OptionalUsedAsFieldOrParameterType"})
    public static Pair<Connector, CacheManager> createConnector(
            String catalogName,
            Map<String, String> config,
            Optional<List<Module>> optionalModules,
            Map<String, ProxiedConnectorInitializer> proxiedConnectorInitializerMap,
            Module storageEngineModule,
            Module cloudVendorModule,
            ConnectorContext context)
    {
        config = ConfigurationUtils.replaceEnvironmentVariables(config);
        logger.debug("catalogName: %s, config:%s", catalogName, config);
        Map<String, String> warpConfig = config.entrySet().stream()
                .filter(e ->
                        e.getKey().startsWith("warp-speed") ||
                        e.getKey().startsWith(WARP_PREFIX) ||
                        // TrinoFileSystem hdfs configuration
                        e.getKey().startsWith("hive.s3") || e.getKey().startsWith("hive.azure") || e.getKey().startsWith("hive.gcs") ||
                        // TrinoFileSystem native configuration
                        e.getKey().startsWith("fs.") || e.getKey().startsWith("s3.") || e.getKey().startsWith("azure.") || e.getKey().startsWith("gcs.") ||
                        e.getKey().startsWith("http") ||
                        e.getKey().equals("node.environment"))
                .collect(Collectors.toMap(entry -> entry.getKey().startsWith(WARP_PREFIX) ? entry.getKey().substring(WARP_PREFIX.length()) : entry.getKey(), Entry::getValue));

        MultiPrefixConfigurationWrapper configWrapper = new MultiPrefixConfigurationWrapper(warpConfig);
        String proxiedConnectorName = configWrapper.get(ProxiedConnectorConfiguration.PROXIED_CONNECTOR);
        ProxiedConnectorInitializer proxiedConnectorInitializer = getProxiedConnectorInitializer(proxiedConnectorName, proxiedConnectorInitializerMap);
        Connector proxiedConnector = proxiedConnectorInitializer.create(catalogName, config, context);
        List<Module> modules = new ArrayList<>();
        modules.addAll(asList(
                new VaradaModules(catalogName, configWrapper, context)
                        .withStorageEngineModule(storageEngineModule)
                        .withCloudVendorModule(cloudVendorModule),
                new EventModule(),
                new MBeanServerModule(),
                new MBeanModule(),
                new DispatcherMainModule(catalogName, configWrapper, context),
                new DispatcherCoordinatorModule(configWrapper, context),
                binder -> {
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                }));
        modules.addAll(proxiedConnectorInitializer.getModules(context));
        modules.add(proxiedConnectorModule(proxiedConnector));
        optionalModules.ifPresent(modules::addAll);
        Bootstrap app = new Bootstrap(modules);

        @SuppressWarnings("removal")
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(Collections.emptyMap())
                .setOptionalConfigurationProperties(warpConfig)
                .initialize();

        initializeSystemServices(injector);
        return Pair.of(injector.getInstance(DispatcherConnectorBase.class),
                injector.getInstance(CacheManager.class));
    }

    private static void initializeSystemServices(Injector injector)
    {
        logger.debug("begin initialize system services");
        injector.getInstance(VaradaInitializedServiceRegistry.class).init();
        logger.debug("finish initialize system services");
    }

    private static ProxiedConnectorInitializer getProxiedConnectorInitializer(
            String proxiedConnector,
            Map<String, ProxiedConnectorInitializer> proxiedConnectorInitializerMap)
    {
        if (proxiedConnectorInitializerMap.get(proxiedConnector) == null) {
            throw new RuntimeException(String.format("proxied connector initializer %s is not available", proxiedConnector));
        }
        return proxiedConnectorInitializerMap.get(proxiedConnector);
    }

    private static Module proxiedConnectorModule(Connector connector)
    {
        return binder -> {
            binder.bind(Connector.class).annotatedWith(ForWarp.class).toInstance(connector);
            binder.bind(ConnectorSplitManager.class).annotatedWith(ForWarp.class).toInstance(connector.getSplitManager());
            binder.bind(ConnectorCacheMetadata.class).annotatedWith(ForWarp.class).toInstance(connector.getCacheMetadata());
            binder.bind(ConnectorPageSourceProvider.class).annotatedWith(ForWarp.class).toInstance(connector.getPageSourceProvider());
            binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForWarp.class).toInstance(connector.getPageSinkProvider());
            binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(ForWarp.class).toInstance(connector.getNodePartitioningProvider());
        };
    }
}
