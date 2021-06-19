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
package io.trino.server;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClientConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class InternalCommunicationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        configBinder(binder).bindConfigGlobalDefaults(HttpClientConfig.class, config -> {
            // Set defaults for all HttpClients in the same guice context
            // so in case of any additions or alternations here an update in:
            //   io.trino.server.security.jwt.JwtAuthenticatorSupportModule.JwkModule.configure
            // and
            //   io.trino.server.security.oauth2.OAuth2ServiceModule.setup
            // may also be required.
            config.setHttp2Enabled(internalCommunicationConfig.isHttp2Enabled());
            config.setKeyStorePath(internalCommunicationConfig.getKeyStorePath());
            config.setKeyStorePassword(internalCommunicationConfig.getKeyStorePassword());
            config.setTrustStorePath(internalCommunicationConfig.getTrustStorePath());
            config.setTrustStorePassword(internalCommunicationConfig.getTrustStorePassword());
        });

        binder.bind(InternalAuthenticationManager.class);
        httpClientBinder(binder).bindGlobalFilter(InternalAuthenticationManager.class);
    }
}
