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
package io.trino.plugin.lakehouse;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.security.HiveSecurityModule;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.MetadataProvider;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class LakehouseConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "lakehouse";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        try (var _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new ConnectorObjectNameGeneratorModule("io.trino.plugin", "trino.plugin"),
                    new JsonModule(),
                    new TypeDeserializerModule(context.getTypeManager()),
                    new LakehouseModule(),
                    new LakehouseHiveModule(),
                    new LakehouseIcebergModule(),
                    new LakehouseDeltaModule(),
                    new LakehouseHudiModule(),
                    new HiveSecurityModule(),
                    new LakehouseFileSystemModule(catalogName, context),
                    binder -> {
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
                        binder.bind(MetadataProvider.class).toInstance(context.getMetadataProvider());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(CatalogHandle.class).toInstance(context.getCatalogHandle());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(LakehouseConnector.class);
        }
    }
}
