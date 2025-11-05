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
package io.trino.server.security.oauth2;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class OAuth2AuthenticationSupportModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OAuth2TokenExchange.class).in(Scopes.SINGLETON);
        binder.bind(OAuth2TokenHandler.class).to(OAuth2TokenExchange.class).in(Scopes.SINGLETON);

        binder.bind(Key.get(ExecutorService.class, ForOAuth2.class))
                .toInstance(newCachedThreadPool(threadsNamed("oauth2-%s")));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForOAuth2.class));

        binder.bind(Key.get(ScheduledExecutorService.class, ForOAuth2.class))
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("oauth2-scheduler-%s")));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForOAuth2.class));

        jaxrsBinder(binder).bind(OAuth2TokenExchangeResource.class);
        install(new OAuth2ServiceModule());
    }
}
