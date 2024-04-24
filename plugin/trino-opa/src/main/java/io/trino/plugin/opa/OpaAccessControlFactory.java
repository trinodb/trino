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
package io.trino.plugin.opa;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonModule;
import io.trino.plugin.opa.schema.OpaColumnMaskQueryResult;
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQuery;
import io.trino.plugin.opa.schema.OpaQueryResult;
import io.trino.plugin.opa.schema.OpaRowFiltersQueryResult;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class OpaAccessControlFactory
        implements SystemAccessControlFactory
{
    @Override
    public String getName()
    {
        return "opa";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        return create(config, Optional.empty(), Optional.empty());
    }

    @Override
    public SystemAccessControl create(Map<String, String> config, SystemAccessControlContext context)
    {
        return create(config, Optional.empty(), Optional.ofNullable(context));
    }

    @VisibleForTesting
    protected static SystemAccessControl create(Map<String, String> config, Optional<HttpClient> httpClient, Optional<SystemAccessControlContext> context)
    {
        requireNonNull(config, "config is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(context, "context is null");

        Bootstrap app = new Bootstrap(
                new JsonModule(),
                binder -> {
                    jsonCodecBinder(binder).bindJsonCodec(OpaQuery.class);
                    jsonCodecBinder(binder).bindJsonCodec(OpaQueryResult.class);
                    jsonCodecBinder(binder).bindJsonCodec(OpaRowFiltersQueryResult.class);
                    jsonCodecBinder(binder).bindJsonCodec(OpaColumnMaskQueryResult.class);
                    httpClient.ifPresentOrElse(
                            client -> binder.bind(Key.get(HttpClient.class, ForOpa.class)).toInstance(client),
                            () -> httpClientBinder(binder).bindHttpClient("opa", ForOpa.class));
                    context.ifPresentOrElse(
                            actualContext -> binder.bind(OpaPluginContext.class).toInstance(new OpaPluginContext(actualContext.getVersion())),
                            () -> binder.bind(OpaPluginContext.class).toInstance(new OpaPluginContext("UNKNOWN")));
                    binder.bind(OpaHighLevelClient.class);
                    binder.bind(Key.get(Executor.class, ForOpa.class))
                            .toProvider(ExecutorProvider.class)
                            .in(Scopes.SINGLETON);
                    binder.bind(OpaHttpClient.class).in(Scopes.SINGLETON);
                },
                new OpaAccessControlModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(SystemAccessControl.class);
    }

    private static class ExecutorProvider
            implements Provider<Executor>
    {
        private final Executor executor;

        private ExecutorProvider()
        {
            this.executor = new BoundedExecutor(
                    newCachedThreadPool(daemonThreadsNamed("opa-access-control-http-%s")),
                    Runtime.getRuntime().availableProcessors());
        }

        @Override
        public Executor get()
        {
            return executor;
        }
    }
}
