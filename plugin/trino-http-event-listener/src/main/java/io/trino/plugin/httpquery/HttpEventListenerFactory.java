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
package io.trino.plugin.httpquery;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class HttpEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "http";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    jsonCodecBinder(binder).bindJsonCodec(QueryCompletedEvent.class);
                    jsonCodecBinder(binder).bindJsonCodec(QueryCreatedEvent.class);
                    jsonCodecBinder(binder).bindJsonCodec(SplitCompletedEvent.class);
                    configBinder(binder).bindConfig(HttpEventListenerConfig.class);
                    httpClientBinder(binder).bindHttpClient("http-event-listener", ForHttpEventListener.class);
                    binder.bind(HttpEventListener.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(HttpEventListener.class);
    }
}
