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
package io.trino.plugin.warp.extension.di;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServer;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;

import static java.util.Objects.requireNonNull;

@Singleton
public class HttpServerLifeCycleHandler
        implements VaradaInitializedServiceMarker
{
    private final HttpServer httpServer;
    private final LifeCycleManager lifeCycleManager;

    @Inject
    public HttpServerLifeCycleHandler(
            HttpServer httpServer,
            LifeCycleManager lifeCycleManager,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry)
    {
        this.httpServer = requireNonNull(httpServer);
        this.lifeCycleManager = requireNonNull(lifeCycleManager);
        varadaInitializedServiceRegistry.addService(this);
    }

    @Override
    public void init()
    {
        lifeCycleManager.addInstance(httpServer);
    }
}
