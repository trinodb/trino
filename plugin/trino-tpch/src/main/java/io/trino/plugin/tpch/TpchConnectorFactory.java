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
package io.trino.plugin.tpch;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

public class TpchConnectorFactory
        implements ConnectorFactory
{
    private final int defaultSplitsPerNode;
    private final boolean predicatePushdownEnabled;

    public TpchConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public TpchConnectorFactory(int defaultSplitsPerNode)
    {
        this(defaultSplitsPerNode, true);
    }

    public TpchConnectorFactory(int defaultSplitsPerNode, boolean predicatePushdownEnabled)
    {
        this.defaultSplitsPerNode = defaultSplitsPerNode;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
    }

    @Override
    public String getName()
    {
        return "tpch";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> properties, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry()),
                new MBeanModule(),
                new JsonModule(),
                new TpchModule(context.getNodeManager(), defaultSplitsPerNode, predicatePushdownEnabled),
                new MBeanServerModule());

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperties(properties)
                .initialize();

        return injector.getInstance(Connector.class);
    }
}
