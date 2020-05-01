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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.raptor.legacy.backup.BackupModule;
import io.prestosql.plugin.raptor.legacy.security.RaptorSecurityModule;
import io.prestosql.plugin.raptor.legacy.storage.StorageModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class RaptorConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;

    public RaptorConnectorFactory(String name, Module metadataModule, Map<String, Module> backupProviders)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = requireNonNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(requireNonNull(backupProviders, "backupProviders is null"));
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new RaptorHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new MBeanModule(),
                new ConnectorObjectNameGeneratorModule(catalogName, "io.prestosql.plugin.raptor.legacy", "presto.plugin.raptor.legacy"),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                },
                metadataModule,
                new BackupModule(backupProviders),
                new StorageModule(),
                new RaptorModule(catalogName),
                new RaptorSecurityModule(catalogName));

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(RaptorConnector.class);
    }
}
