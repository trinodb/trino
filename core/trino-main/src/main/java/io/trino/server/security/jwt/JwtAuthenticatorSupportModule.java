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
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class JwtAuthenticatorSupportModule
        extends AbstractConfigurationAwareModule
{
    private static final String JWK = "jwk";

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(JwtAuthenticatorConfig.class);
        httpClientBinder(binder)
                .bindHttpClient(JWK, ForJwt.class)
                // Reset HttpClient default configuration to override InternalCommunicationModule changes.
                // Setting a keystore and/or a truststore for internal communication changes the default SSL configuration
                // for all clients in the same guice context. This, however, does not make sense for this client which will
                // very rarely use the same SSL setup as internal communication, so using the system default truststore
                // makes more sense.
                .withConfigDefaults(config -> config
                        .setKeyStorePath(null)
                        .setKeyStorePassword(null)
                        .setTrustStorePath(null)
                        .setTrustStorePassword(null)
                        .setAutomaticHttpsSharedSecret(null));
    }
}
