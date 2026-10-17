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
package io.trino.security.credential;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialProviderFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

public class FileCredentialProviderStore
        implements CredentialProviderStore
{
    private static final Logger LOG = Logger.get(FileCredentialProviderStore.class);
    private final File credentialProviderConfigDir;
    private final SecretsResolver secretsResolver;

    @Inject
    public FileCredentialProviderStore(FileCredentialProviderStoreConfig config, SecretsResolver secretsResolver)
    {
        this.credentialProviderConfigDir = requireNonNull(config, "config is null").getCredentialProviderConfigDir();
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public void loadCredentialProviders(Map<String, CredentialProviderFactory> factories, Map<String, CredentialProvider> credentialProviders)
    {
        if (!credentialProviderConfigDir.isDirectory()) {
            LOG.warn("Credential providers directory does not exist or is not a directory: " + credentialProviderConfigDir.getAbsolutePath());
        }

        listCredentialProviderFiles(credentialProviderConfigDir)
                .stream()
                .map(file -> new CredentialProviderConfigFile(file.getName().replace(".properties", ""), file))
                .filter(configFile -> !credentialProviders.containsKey(configFile.name()))
                .map(CredentialProviderConfigFile::loadProperties)
                .forEach(properties -> {
                    CredentialProviderFactory credentialProviderFactory = factories.get(properties.factoryName());
                    if (credentialProviderFactory == null) {
                        throw new IllegalStateException("Credential provider factory %s not found in: [%s]".formatted(properties.factoryName(), String.join(", ", factories.keySet())));
                    }
                    Map<String, String> config = secretsResolver.getResolvedConfiguration(properties.properties());
                    credentialProviders.put(properties.providerName(), credentialProviderFactory.create(properties.providerName(), config));
                });
    }

    private static List<File> listCredentialProviderFiles(File credentialProvidersDirectory)
    {
        if (credentialProvidersDirectory == null || !credentialProvidersDirectory.isDirectory()) {
            return ImmutableList.of();
        }

        File[] files = credentialProvidersDirectory.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(files)
                .filter(File::isFile)
                .filter(file -> file.getName().endsWith(".properties"))
                .collect(toImmutableList());
    }

    public record CredentialProviderConfigFile(String name, File file)
    {
        public CredentialProviderProperties loadProperties()
        {
            Map<String, String> properties;
            try {
                properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog property file " + file, e);
            }

            String factoryName = properties.remove("credential-provider.name");
            checkState(factoryName != null, "Credential provider configuration %s does not contain 'credential-provider.name'", file.getAbsoluteFile());

            return new CredentialProviderProperties(name, factoryName.strip(), properties);
        }
    }

    public record CredentialProviderProperties(String providerName, String factoryName, Map<String, String> properties) {}
}
