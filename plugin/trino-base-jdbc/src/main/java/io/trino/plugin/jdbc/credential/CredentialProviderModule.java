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
package io.trino.plugin.jdbc.credential;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.plugin.jdbc.credential.file.ConfigFileBasedCredentialProviderConfig;
import io.trino.plugin.jdbc.credential.keystore.KeyStoreBasedCredentialProviderConfig;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.plugin.jdbc.credential.CredentialProviderType.FILE;
import static io.trino.plugin.jdbc.credential.CredentialProviderType.INLINE;
import static io.trino.plugin.jdbc.credential.CredentialProviderType.KEYSTORE;
import static io.trino.plugin.jdbc.credential.keystore.KeyStoreUtils.loadKeyStore;
import static io.trino.plugin.jdbc.credential.keystore.KeyStoreUtils.readEntity;

public class CredentialProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCredentialProviderModule(INLINE, new InlineCredentialProviderModule());
        bindCredentialProviderModule(FILE, new ConfigFileBasedCredentialProviderModule());
        bindCredentialProviderModule(KEYSTORE, new KeyStoreBasedCredentialProviderModule());

        configBinder(binder).bindConfig(ExtraCredentialConfig.class);
        binder.bind(CredentialProvider.class).to(ExtraCredentialProvider.class).in(SINGLETON);
    }

    private void bindCredentialProviderModule(CredentialProviderType name, Module module)
    {
        install(conditionalModule(
                CredentialProviderTypeConfig.class,
                config -> name == config.getCredentialProviderType(),
                module));
    }

    private static class InlineCredentialProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(CredentialConfig.class);
        }

        @Provides
        @Singleton
        @ForExtraCredentialProvider
        public CredentialProvider getCredentialProvider(CredentialConfig config)
        {
            return new StaticCredentialProvider(config.getConnectionUser(), config.getConnectionPassword());
        }
    }

    private static class ConfigFileBasedCredentialProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(ConfigFileBasedCredentialProviderConfig.class);
        }

        @Provides
        @Singleton
        @ForExtraCredentialProvider
        public CredentialProvider getCredentialProvider(ConfigFileBasedCredentialProviderConfig fileConfig)
                throws IOException
        {
            Map<String, String> properties = loadPropertiesFrom(fileConfig.getCredentialFile());
            CredentialConfig config = new ConfigurationFactory(properties).build(CredentialConfig.class);
            return new StaticCredentialProvider(config.getConnectionUser(), config.getConnectionPassword());
        }
    }

    private static class KeyStoreBasedCredentialProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(KeyStoreBasedCredentialProviderConfig.class);
        }

        @Provides
        @Singleton
        @ForExtraCredentialProvider
        public CredentialProvider getCredentialProvider(KeyStoreBasedCredentialProviderConfig config)
                throws IOException, GeneralSecurityException
        {
            KeyStore keyStore = loadKeyStore(config.getKeyStoreType(), config.getKeyStoreFilePath(), config.getKeyStorePassword());
            String user = readEntity(keyStore, config.getUserCredentialName(), config.getPasswordForUserCredentialName());
            String password = readEntity(keyStore, config.getPasswordCredentialName(), config.getPasswordForPasswordCredentialName());
            return new StaticCredentialProvider(Optional.of(user), Optional.of(password));
        }
    }
}
