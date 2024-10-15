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

package io.trino.plugin.eventlistener.kafka;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class KafkaEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new MBeanServerModule(),
                new KafkaProducerModule(),
                binder -> {
                    configBinder(binder).bindConfig(KafkaEventListenerConfig.class);
                    binder.bind(KafkaEventListener.class).in(Scopes.SINGLETON);
                    newExporter(binder).export(KafkaEventListener.class).withGeneratedName();
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(KafkaEventListener.class);
    }
}
