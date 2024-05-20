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
package io.trino.server.security.jwt;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.jsonwebtoken.Locator;

import java.net.URI;
import java.security.Key;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class JwtAuthenticatorSupportModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(JwtAuthenticatorConfig.class);
        install(conditionalModule(
                JwtAuthenticatorConfig.class,
                JwtAuthenticatorSupportModule::isHttp,
                new JwkModule(),
                jwkBinder -> jwkBinder.bind(new TypeLiteral<Locator<Key>>() {}).annotatedWith(ForJwt.class).to(FileSigningKeyLocator.class).in(Scopes.SINGLETON)));
    }

    private static boolean isHttp(JwtAuthenticatorConfig config)
    {
        return config.getKeyFile().startsWith("https://") || config.getKeyFile().startsWith("http://");
    }

    private static class JwkModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            httpClientBinder(binder).bindHttpClient("jwk", ForJwt.class);
        }

        @Provides
        @Singleton
        @ForJwt
        public static JwkService createJwkService(JwtAuthenticatorConfig config, @ForJwt HttpClient httpClient)
        {
            return new JwkService(URI.create(config.getKeyFile()), httpClient);
        }

        @Provides
        @Singleton
        @ForJwt
        public static Locator<Key> createJwkSigningKeyLocator(@ForJwt JwkService jwkService)
        {
            return new JwkSigningKeyLocator(jwkService);
        }

        // this module can be added multiple times, and this prevents multiple processing by Guice
        @Override
        public int hashCode()
        {
            return JwkModule.class.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof JwkModule;
        }
    }
}
