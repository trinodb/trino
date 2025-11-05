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
package io.trino.plugin.jmx;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class JmxConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "jmx";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new MBeanServerModule(),
                new ConnectorContextModule(catalogName, context),
                binder -> {
                    configBinder(binder).bindConfig(JmxConnectorConfig.class);
                    binder.bind(JmxConnector.class).in(Scopes.SINGLETON);
                    binder.bind(JmxHistoricalData.class).in(Scopes.SINGLETON);
                    binder.bind(JmxMetadata.class).in(Scopes.SINGLETON);
                    binder.bind(JmxSplitManager.class).in(Scopes.SINGLETON);
                    binder.bind(JmxPeriodicSampler.class).in(Scopes.SINGLETON);
                    binder.bind(JmxRecordSetProvider.class).in(Scopes.SINGLETON);
                    binder.bind(ScheduledExecutorService.class).toInstance(newSingleThreadScheduledExecutor(daemonThreadsNamed("jmx-history-%s")));
                    closingBinder(binder).registerExecutor(ScheduledExecutorService.class);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(JmxConnector.class);
    }
}
