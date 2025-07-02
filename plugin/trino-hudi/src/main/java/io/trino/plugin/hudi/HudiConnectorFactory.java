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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class HudiConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "hudi";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, Optional.empty());
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<Module> module)
    {
        ClassLoader classLoader = HudiConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new JsonModule(),
                    new HudiModule(),
                    new HiveMetastoreModule(Optional.empty()),
                    new HudiFileSystemModule(catalogName, context),
                    new MBeanServerModule(),
                    module.orElse(EMPTY_MODULE),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            HudiTransactionManager transactionManager = injector.getInstance(HudiTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(new Key<>() {});
            HudiTableProperties hudiTableProperties = injector.getInstance(HudiTableProperties.class);

            return new HudiConnector(
                    injector,
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    sessionPropertiesProviders,
                    hudiTableProperties.getTableProperties());
        }
    }

    private static class HudiFileSystemModule
            extends AbstractConfigurationAwareModule
    {
        private final String catalogName;
        private final NodeManager nodeManager;
        private final OpenTelemetry openTelemetry;

        public HudiFileSystemModule(String catalogName, ConnectorContext context)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.nodeManager = context.getNodeManager();
            this.openTelemetry = context.getOpenTelemetry();
        }

        @Override
        protected void setup(Binder binder)
        {
            boolean metadataCacheEnabled = buildConfigObject(HudiConfig.class).isMetadataCacheEnabled();
            install(new FileSystemModule(catalogName, nodeManager, openTelemetry, metadataCacheEnabled));
        }
    }
}
