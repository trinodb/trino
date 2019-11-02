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
package io.prestosql.plugin.phoenix;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PhoenixConnectorFactory
        implements ConnectorFactory
{
    private final ClassLoader classLoader;

    public PhoenixConnectorFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "phoenix";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PhoenixHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MBeanServerModule(),
                    new MBeanModule(),
                    new PhoenixClientModule(context.getTypeManager(), catalogName));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            PhoenixMetadata metadata = injector.getInstance(PhoenixMetadata.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorRecordSetProvider recordSetProvider = injector.getInstance(ConnectorRecordSetProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            PhoenixTableProperties tableProperties = injector.getInstance(PhoenixTableProperties.class);
            PhoenixColumnProperties columnProperties = injector.getInstance(PhoenixColumnProperties.class);

            return new PhoenixConnector(
                    lifeCycleManager,
                    new ClassLoaderSafeConnectorMetadata(metadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    recordSetProvider,
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    tableProperties,
                    columnProperties);
        }
    }
}
