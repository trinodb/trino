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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpsConfig;
import io.airlift.node.NodeConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.node.NodeConfig.AddressSource.IP_ENCODED_AS_HOSTNAME;

public class InternalCommunicationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalCommunicationConfig internalCommunicationConfig = buildConfigObject(InternalCommunicationConfig.class);
        if (internalCommunicationConfig.isHttpsRequired() && internalCommunicationConfig.getKeyStorePath() == null && internalCommunicationConfig.getTrustStorePath() == null) {
            String sharedSecret = internalCommunicationConfig.getSharedSecret()
                    .orElseThrow(() -> new IllegalArgumentException("Internal shared secret must be set when internal HTTPS is enabled"));
            configBinder(binder).bindConfigDefaults(HttpsConfig.class, config -> config.setAutomaticHttpsSharedSecret(sharedSecret));
            configBinder(binder).bindConfigGlobalDefaults(NodeConfig.class, config -> config.setInternalAddressSource(IP_ENCODED_AS_HOSTNAME));
        }
        binder.bind(InternalAuthenticationManager.class).in(Scopes.SINGLETON);
    }
}
