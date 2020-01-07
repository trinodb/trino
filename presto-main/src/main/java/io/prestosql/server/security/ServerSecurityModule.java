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
package io.prestosql.server.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Locale.ENGLISH;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(AuthenticationFilter.class).in(Scopes.SINGLETON);

        binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);
        binder.bind(CertificateAuthenticatorManager.class).in(Scopes.SINGLETON);

        authenticatorBinder(binder); // create empty map binder

        installAuthenticator("certificate", CertificateAuthenticator.class, CertificateConfig.class);
        installAuthenticator("kerberos", KerberosAuthenticator.class, KerberosConfig.class);
        installAuthenticator("password", PasswordAuthenticator.class, PasswordAuthenticatorConfig.class);
        installAuthenticator("jwt", JsonWebTokenAuthenticator.class, JsonWebTokenConfig.class);
    }

    @Provides
    public List<Authenticator> getAuthenticatorList(SecurityConfig config, Map<String, Authenticator> authenticators)
    {
        return authenticationTypes(config).stream()
                .map(type -> {
                    Authenticator authenticator = authenticators.get(type);
                    if (authenticator == null) {
                        throw new RuntimeException("Unknown authenticator type: " + type);
                    }
                    return authenticator;
                })
                .collect(toImmutableList());
    }

    public static Module authenticatorModule(String name, Class<? extends Authenticator> clazz, Module module)
    {
        checkArgument(name.toLowerCase(ENGLISH).equals(name), "name is not lower case: %s", name);
        Module authModule = binder -> authenticatorBinder(binder).addBinding(name).to(clazz).in(Scopes.SINGLETON);
        return installModuleIf(
                SecurityConfig.class,
                config -> authenticationTypes(config).contains(name),
                Modules.combine(module, authModule));
    }

    private void installAuthenticator(String name, Class<? extends Authenticator> authenticator, Class<?> config)
    {
        install(authenticatorModule(name, authenticator, binder -> configBinder(binder).bindConfig(config)));
    }

    private static MapBinder<String, Authenticator> authenticatorBinder(Binder binder)
    {
        return newMapBinder(binder, String.class, Authenticator.class);
    }

    private static List<String> authenticationTypes(SecurityConfig config)
    {
        return config.getAuthenticationTypes().stream()
                .map(type -> type.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }
}
