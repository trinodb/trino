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
package io.trino.plugin.atop;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.atop.AtopConnectorConfig.AtopSecurity;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class AtopConnectorFactory
        implements ConnectorFactory
{
    private final Class<? extends AtopFactory> atopFactoryClass;
    private final ClassLoader classLoader;

    public AtopConnectorFactory(Class<? extends AtopFactory> atopFactoryClass, ClassLoader classLoader)
    {
        this.atopFactoryClass = requireNonNull(atopFactoryClass, "atopFactoryClass is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "atop";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new AtopModule(
                            atopFactoryClass,
                            context.getTypeManager(),
                            context.getNodeManager(),
                            context.getNodeManager().getEnvironment()),
                    new CatalogNameModule(catalogName),
                    new ConnectorAccessControlModule(),
                    conditionalModule(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity() == AtopSecurity.FILE,
                            combine(
                                new FileBasedAccessControlModule(),
                                new JsonModule())));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(AtopConnector.class);
        }
    }
}
