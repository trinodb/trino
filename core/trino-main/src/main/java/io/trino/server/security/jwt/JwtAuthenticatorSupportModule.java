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
        httpClientBinder(binder).bindHttpClient(JWK, ForJwt.class);
    }

    // this module can be added multiple times, and this prevents multiple processing by Guice
    @Override
    public int hashCode()
    {
        return JwtAuthenticatorSupportModule.class.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof JwtAuthenticatorSupportModule;
    }
}
