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
package io.trino.plugin.pulsar;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.jmx.RebindSafeMBeanServer;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;

import javax.management.MBeanServer;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Defines binding of classes in the Trino connector.
 */
public class PulsarConnectorModule
        implements Module
{
    private final String catalogName;
    private final TypeManager typeManager;

    public PulsarConnectorModule(String catalogName,
                                 TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));

        binder.bind(PulsarConnector.class).in(Scopes.SINGLETON);
        binder.bind(PulsarConnectorCache.class).to(PulsarConnectorManagedLedgerFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(PulsarMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ClassLoaderSafeConnectorMetadata.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(PulsarSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(PulsarRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(PulsarDispatchingRowDecoderFactory.class).in(Scopes.SINGLETON);

        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
        newExporter(binder).export(PulsarConnectorMetricsTracker.class).withGeneratedName();

        configBinder(binder).bindConfig(PulsarConnectorConfig.class);

        binder.install(new DecoderModule());
    }
}
