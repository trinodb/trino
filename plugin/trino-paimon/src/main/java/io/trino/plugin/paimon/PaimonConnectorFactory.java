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
package io.trino.plugin.paimon;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.plugin.paimon.catalog.PaimonCatalogModule;
import io.trino.spi.Node;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static java.util.Objects.requireNonNull;

public class PaimonConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "paimon";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, Optional.empty());
    }

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<Module> module)
    {
        ClassLoader classLoader = PaimonConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PaimonCatalogModule(),
                    new PaimonModule(),
                    // bind the trino file system module
                    new PaimonFileSystemModule(catalogName, context),
                    binder -> {
                        binder.bind(ClassLoader.class).toInstance(PaimonConnectorFactory.class.getClassLoader());
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getCurrentNode().getVersion()));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                        binder.bind(Node.class).toInstance(context.getCurrentNode());
                        binder.bind(Tracer.class).toInstance(context.getTracer());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    },
                    module.orElse(EMPTY_MODULE));

            Injector injector = app.doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(PaimonConnector.class);
        }
    }

    private static class PaimonFileSystemModule
            extends AbstractConfigurationAwareModule
    {
        private final String catalogName;
        private final ConnectorContext context;

        private PaimonFileSystemModule(String catalogName, ConnectorContext context)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.context = requireNonNull(context, "context is null");
        }

        @Override
        protected void setup(Binder binder)
        {
            boolean metadataCacheEnabled = buildConfigObject(PaimonConfig.class).isMetadataCacheEnabled();
            install(new FileSystemModule(catalogName, context, metadataCacheEnabled));
        }
    }
}
