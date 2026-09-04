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
package io.trino.plugin.base.security.credential;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialResolver;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class CredentialProviderModule
        implements Module
{
    protected final Named named;
    private final String prefix;

    protected CredentialProviderModule(String name, String prefix)
    {
        this.named = Names.named(requireNonNull(name, "name is null"));
        this.prefix = prefix;
    }

    @Override
    public void configure(Binder binder)
    {
        String prefix = this.prefix == null ? "" : "%s.".formatted(this.prefix);
        configBinder(binder).bindConfig(CredentialProviderConfig.class, named, "%scredential-provider.%s".formatted(prefix, named.value()));
        binder.bind(CredentialProvider.class).annotatedWith(named).toProvider(new CredentialProviderProvider(named)).in(SINGLETON);
    }

    public static void credentialProvider(Binder binder, String name)
    {
        credentialProvider(binder, name, null);
    }

    public static void credentialProvider(Binder binder, String name, String prefix)
    {
        binder.install(new CredentialProviderModule(name, prefix));
    }

    private static class CredentialProviderProvider
            implements Provider<CredentialProvider>
    {
        private final Named named;
        private Injector injector;

        public CredentialProviderProvider(Named named)
        {
            this.named = requireNonNull(named, "named is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public CredentialProvider get()
        {
            requireNonNull(injector, "injector is null");
            CredentialResolver credentialResolver = injector.getInstance(CredentialResolver.class);
            String name = injector.getInstance(Key.get(CredentialProviderConfig.class, named)).getName();

            // When the credential provider plugin is not loaded yet,
            // use the lazy provider as it should be resolvable when all plugins are loaded
            return credentialResolver.get(name).orElseGet(() -> new LazyCredentialProvider(credentialResolver, name));
        }
    }
}
