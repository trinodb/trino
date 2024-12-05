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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.HttpServerModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class HttpServerEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "http-server";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        return createInternal(config, false);
    }

    @VisibleForTesting
    HttpServerEventListener createInternal(Map<String, String> config, boolean testing)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new JaxrsModule(),
                testing ? new TestingHttpServerModule() : new HttpServerModule(),
                binder -> {
                    jsonCodecBinder(binder).bindJsonCodec(QueryCompletedEvent.class);
                    configBinder(binder).bindConfig(HttpServerEventListenerConfig.class);
                    binder.bind(HttpServerEventListener.class).in(Scopes.SINGLETON);
                    jaxrsBinder(binder).bind(HttpServerEventListener.class);
                    binder.bind(NodeInfo.class).toInstance(new NodeInfo("dummy"));
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(HttpServerEventListener.class);
    }
}
