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
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import io.trino.spi.security.OAuth2TokenExchanger;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class OAuth2TokenExchangeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(OAuth2TokenExchangeConfig.class);

        httpClientBinder(binder)
                .bindHttpClient("oauth2-token-exchange", ForOAuth2TokenExchange.class)
                .withConfigDefaults(clientConfig -> clientConfig
                        .setRequestBufferSize(DataSize.of(32, KILOBYTE))
                        .setResponseBufferSize(DataSize.of(32, KILOBYTE)));

        binder.bind(TokenExchangeManager.class).in(Scopes.SINGLETON);
        binder.bind(OAuth2TokenExchanger.class).to(TokenExchangeManager.class).in(Scopes.SINGLETON);
    }
}
