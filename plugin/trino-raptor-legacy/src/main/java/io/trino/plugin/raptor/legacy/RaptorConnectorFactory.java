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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.raptor.legacy.backup.BackupModule;
import io.trino.plugin.raptor.legacy.security.RaptorSecurityModule;
import io.trino.plugin.raptor.legacy.storage.StorageModule;
import io.trino.spi.NodeManager;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.Versions.checkSpiVersion;
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
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkSpiVersion(context, this);

        Bootstrap app = new Bootstrap(
                new CatalogNameModule(catalogName),
                new JsonModule(),
                new MBeanModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.raptor.legacy", "trino.plugin.raptor.legacy"),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                },
                metadataModule,
                new BackupModule(backupProviders),
                new StorageModule(),
                new RaptorModule(),
                new RaptorSecurityModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(RaptorConnector.class);
    }
}
