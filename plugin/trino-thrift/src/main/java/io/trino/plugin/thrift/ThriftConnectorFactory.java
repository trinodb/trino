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
package io.trino.plugin.thrift;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class ThriftConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module locationModule;

    public ThriftConnectorFactory(String name, Module locationModule)
    {
        this.name = requireNonNull(name, "name is null");
        this.locationModule = requireNonNull(locationModule, "locationModule is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new MBeanModule(),
                new MBeanServerModule(),
                new ConnectorObjectNameGeneratorModule("io.trino.plugin.thrift", "trino.plugin.thrift"),
                new DriftNettyClientModule(),
                new ConnectorContextModule(catalogName, context),
                locationModule,
                new ThriftModule());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ThriftConnector.class);
    }
}
