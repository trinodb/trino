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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveHdfsModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
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

import static java.util.Objects.requireNonNull;

public class HudiConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Optional<HiveMetastore> metastore;

    public HudiConnectorFactory(String name, Optional<HiveMetastore> metastore)
    {
        this.name = requireNonNull(name, "name is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    // public HudiConnectorFactory(String name, Class<? extends Module> module)
    // {
    //     checkArgument(!isNullOrEmpty(name), "name is null or empty");
    //     this.name = name;
    //     this.module = requireNonNull(module, "module is null");
    // }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, metastore);
    }

    // public Connector createLegacy(String catalogName, Map<String, String> config, ConnectorContext context)
    // {
    //     ClassLoader classLoader = context.duplicatePluginClassLoader();
    //     try {
    //         Object moduleInstance = classLoader.loadClass(module.getName()).getConstructor().newInstance();
    //         Class<?> moduleClass = classLoader.loadClass(Module.class.getName());
    //         return (Connector) classLoader.loadClass(InternalHudiConnectorFactory.class.getName())
    //                 .getMethod("createConnector", String.class, Map.class, ConnectorContext.class, moduleClass)
    //                 .invoke(null, catalogName, config, context, moduleInstance);
    //     }
    //     catch (InvocationTargetException e) {
    //         Throwable targetException = e.getTargetException();
    //         throwIfUnchecked(targetException);
    //         throw new RuntimeException(targetException);
    //     }
    //     catch (ReflectiveOperationException e) {
    //         throw new RuntimeException(e);
    //     }
    // }
    //
    // public static class EmptyModule
    //         implements Module
    // {
    //     @Override
    //     public void configure(Binder binder) {}
    // }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<HiveMetastore> metastore)
    {
        requireNonNull(config, "config is null");
        ClassLoader classLoader = InternalHudiConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HudiModule(),
                    new HiveMetastoreModule(metastore),
                    new HiveHdfsModule(),
                    new HiveS3Module(),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HdfsAuthenticationModule(),
                    new MBeanServerModule(),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
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
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {}));
            HudiTableProperties hudiTableProperties = injector.getInstance(HudiTableProperties.class);
            Optional<ConnectorAccessControl> hudiAccessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}))
                    .map(accessControl -> new ClassLoaderSafeConnectorAccessControl(accessControl, classLoader));

            return new HudiConnector(
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    ImmutableSet.of(),
                    sessionPropertiesProviders,
                    hudiTableProperties.getTableProperties(),
                    hudiAccessControl);
        }
    }
}
