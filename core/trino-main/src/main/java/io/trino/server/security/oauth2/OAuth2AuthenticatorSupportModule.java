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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class OAuth2AuthenticatorSupportModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(OAuth2TokenExchange.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(OAuth2TokenExchangeResource.class);
        install(new OAuth2SupportModule());
    }

    // this module can be added multiple times, and this prevents multiple processing by Guice
    @Override
    public int hashCode()
    {
        return OAuth2AuthenticatorSupportModule.class.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof OAuth2AuthenticatorSupportModule;
    }
}
