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
package io.trino.plugin.jdbc;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.config.ConfigUtils;
import io.trino.spi.NodeManager;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Supplier<Module> module;

    public JdbcConnectorFactory(String name, Supplier<Module> module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = createBootstrap(catalogName, requiredConfig, context);

        Injector injector = app.initialize();

        return injector.getInstance(JdbcConnector.class);
    }

    @Override
    public Set<String> getSecuritySensitivePropertyNames(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = createBootstrap(catalogName, config, context);

        Set<ConfigPropertyMetadata> usedProperties = app
                .quiet()
                .skipErrorReporting()
                .configure();

        return ConfigUtils.getSecuritySensitivePropertyNames(config, usedProperties);
    }

    private Bootstrap createBootstrap(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                binder -> binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder()),
                binder -> binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry()),
                binder -> binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName)),
                new JdbcModule(),
                module.get());

        return app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config);
    }
}
