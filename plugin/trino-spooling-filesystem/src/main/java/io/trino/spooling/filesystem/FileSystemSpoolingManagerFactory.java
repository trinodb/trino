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
package io.trino.spooling.filesystem;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.protocol.SpoolingManagerContext;
import io.trino.spi.protocol.SpoolingManagerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FileSystemSpoolingManagerFactory
        implements SpoolingManagerFactory
{
    @Override
    public String getName()
    {
        return "filesystem";
    }

    @Override
    public SpoolingManager create(Map<String, String> config, SpoolingManagerContext context)
    {
        requireNonNull(config, "requiredConfig is null");
        Bootstrap app = new Bootstrap(
                new FileSystemSpoolingModule(context.isCoordinator()),
                new MBeanModule(),
                new MBeanServerModule(),
                binder -> {
                    binder.bind(SpoolingManagerContext.class).toInstance(context);
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return new TracingSpoolingManager(context.getTracer(), injector.getInstance(SpoolingManager.class));
    }
}
