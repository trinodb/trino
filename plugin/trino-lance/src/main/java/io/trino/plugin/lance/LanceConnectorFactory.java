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
package io.trino.plugin.lance;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LanceConnectorFactory
        implements ConnectorFactory
{
    public static final String NAME = "lance";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                new FileSystemModule(catalogName, context, true),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                },
                new LanceModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(LanceConnector.class);
    }
}
