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
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.jwt.ForJwk;
import io.trino.server.security.jwt.JwkService;
import io.trino.server.security.jwt.JwkSigningKeyResolver;

import javax.inject.Singleton;

import java.net.URI;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class OAuth2ClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, OAuth2Client.class)
                .setDefault()
                .to(ScribeJavaOAuth2Client.class)
                .in(Scopes.SINGLETON);
        httpClientBinder(binder)
                .bindHttpClient("oauth2-jwk", ForOAuth2.class)
                // Reset to defaults to override InternalCommunicationModule changes to this client default configuration.
                // Setting a keystore and/or a truststore for internal communication changes the default SSL configuration
                // for all clients in this guice context. This does not make sense for this client which will very rarely
                // use the same SSL configuration, so using the system default truststore makes more sense.
                .withConfigDefaults(config -> config
                        .setKeyStorePath(null)
                        .setKeyStorePassword(null)
                        .setTrustStorePath(null)
                        .setTrustStorePassword(null)
                        .setAutomaticHttpsSharedSecret(null));
        // Used by JwkService
        binder.bind(HttpClient.class).annotatedWith(ForJwk.class).to(Key.get(HttpClient.class, ForOAuth2.class));
        binder.bind(JwkService.class).in(Scopes.SINGLETON);
        binder.bind(SigningKeyResolver.class).annotatedWith(ForOAuth2.class).to(JwkSigningKeyResolver.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForJwk
    public static URI createJwkAddress(OAuth2Endpoints endpoints)
    {
        return URI.create(endpoints.getJwksUrl());
    }

    // this module can be added multiple times, and this prevents multiple processing by Guice
    @Override
    public int hashCode()
    {
        return OAuth2ClientModule.class.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof OAuth2ClientModule;
    }
}
