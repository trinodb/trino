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
package io.trino.plugin.ai.functions;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.secrets.RuntimeSecretResolver;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class OpenAiModule
        implements Module
{
    private final ConnectorContext context;

    public OpenAiModule(ConnectorContext context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(OpenAiConfig.class);

        httpClientBinder(binder).bindHttpClient("ai", ForAiClient.class);

        context.getRuntimeSecretResolver().ifPresent(resolver ->
                binder.bind(RuntimeSecretResolver.class).toInstance(resolver));

        binder.bind(OpenAiClient.class).in(Scopes.SINGLETON);
        binder.bind(AiClient.class).to(OpenAiClient.class).in(Scopes.SINGLETON);
    }
}
