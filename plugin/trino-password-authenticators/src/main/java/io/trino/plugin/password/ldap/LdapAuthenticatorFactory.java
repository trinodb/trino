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
package io.trino.plugin.password.ldap;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ldap.LdapClientModule;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class LdapAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "ldap";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                new LdapClientModule(),
                binder -> {
                    configBinder(binder).bindConfig(LdapAuthenticatorConfig.class);
                    binder.bind(LdapAuthenticatorClient.class).in(Scopes.SINGLETON);
                    binder.bind(LdapAuthenticator.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(LdapAuthenticator.class);
    }
}
