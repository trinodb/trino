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
package io.trino.plugin.exchange.hdfs;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.jmx.PrefixObjectNameGeneratorModule;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManager;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeManagerContext;
import io.trino.spi.exchange.ExchangeManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HdfsExchangeManagerFactory
        implements ExchangeManagerFactory
{
    static final String NAME = "hdfs";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public ExchangeManager create(Map<String, String> config, ExchangeManagerContext context)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.exchange.hdfs",
                new MBeanModule(),
                new MBeanServerModule(),
                new PrefixObjectNameGeneratorModule("io.trino.plugin.exchange.hdfs", "trino.plugin.exchange.hdfs"),
                new HdfsExchangeModule(),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(FileSystemExchangeManager.class);
    }
}
