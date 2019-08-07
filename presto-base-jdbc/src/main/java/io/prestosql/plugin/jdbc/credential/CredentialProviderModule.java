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
package io.prestosql.plugin.jdbc.credential;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.credential.file.ConfigFileBasedCredentialProviderConfig;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.plugin.jdbc.credential.CredentialProviderType.FILE;
import static io.prestosql.plugin.jdbc.credential.CredentialProviderType.INLINE;
import static java.util.Objects.requireNonNull;

public class CredentialProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCredentialProviderModule(
                INLINE,
                internalBinder -> {
                    configBinder(internalBinder).bindConfig(CredentialConfig.class);
                    internalBinder.bind(CredentialProvider.class).annotatedWith(ForExtraCredentialProvider.class).to(ConfigFileBasedCredentialProvider.class).in(SINGLETON);
                });
        bindCredentialProviderModule(
                FILE,
                internalBinder -> {
                    configBinder(binder).bindConfig(ConfigFileBasedCredentialProviderConfig.class);
                    internalBinder.bind(CredentialProvider.class).annotatedWith(ForExtraCredentialProvider.class).toProvider(ConfigFileBasedCredentialProviderFactory.class).in(SINGLETON);
                });
        binder.bind(CredentialProvider.class).to(ExtraCredentialProvider.class).in(SINGLETON);
    }

    private void bindCredentialProviderModule(CredentialProviderType name, Module module)
    {
        install(installModuleIf(
                BaseJdbcConfig.class,
                config -> name.equals(config.getCredentialProviderType()),
                module));
    }

    private static class ConfigFileBasedCredentialProviderFactory
            implements Provider<CredentialProvider>
    {
        private final CredentialConfig credentialsConfig;

        @Inject
        public ConfigFileBasedCredentialProviderFactory(ConfigFileBasedCredentialProviderConfig config)
                throws IOException
        {
            requireNonNull(config, "config is null");
            Map<String, String> properties = loadPropertiesFrom(config.getCredentialFile());
            credentialsConfig = new ConfigurationFactory(properties).build(CredentialConfig.class);
        }

        @Override
        public CredentialProvider get()
        {
            return new ConfigFileBasedCredentialProvider(credentialsConfig);
        }
    }
}
