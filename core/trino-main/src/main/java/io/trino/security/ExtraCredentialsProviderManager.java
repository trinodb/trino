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
package io.trino.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ExtraCredentialsProvider;
import io.trino.spi.security.ExtraCredentialsProviderFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExtraCredentialsProviderManager
        implements ExtraCredentialsProvider
{
    private static final Logger log = Logger.get(ExtraCredentialsProviderManager.class);
    private static final File CONFIG_FILE = new File("etc/extra-credentials-provider.properties");
    private static final String NAME_PROPERTY = "extra-credentials-provider.name";

    private final Map<String, ExtraCredentialsProviderFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<ExtraCredentialsProvider>> configuredProvider = new AtomicReference<>(Optional.empty());
    private final SecretsResolver secretsResolver;

    @Inject
    public ExtraCredentialsProviderManager(SecretsResolver secretsResolver)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    public void addExtraCredentialsProviderFactory(ExtraCredentialsProviderFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (factories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Extra credentials provider '%s' is already registered", factory.getName()));
        }
    }

    public void loadConfiguredExtraCredentialsProvider()
            throws IOException
    {
        loadConfiguredExtraCredentialsProvider(CONFIG_FILE);
    }

    @VisibleForTesting
    void loadConfiguredExtraCredentialsProvider(File configFile)
            throws IOException
    {
        if (configuredProvider.get().isPresent() || !configFile.exists()) {
            return;
        }
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        String name = properties.remove(NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name),
                "Extra credentials provider configuration %s does not contain %s",
                configFile.getAbsoluteFile(),
                NAME_PROPERTY);
        setConfiguredExtraCredentialsProvider(name, properties);
    }

    @VisibleForTesting
    protected void setConfiguredExtraCredentialsProvider(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading extra credentials provider %s --", name);

        ExtraCredentialsProviderFactory factory = factories.get(name);
        checkState(factory != null, "Extra credentials provider %s is not registered", name);

        ExtraCredentialsProvider provider;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            provider = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)));
        }

        setConfiguredExtraCredentialsProvider(provider);

        log.info("-- Loaded extra credentials provider %s --", name);
    }

    @VisibleForTesting
    protected void setConfiguredExtraCredentialsProvider(ExtraCredentialsProvider provider)
    {
        checkState(configuredProvider.compareAndSet(Optional.empty(), Optional.of(provider)), "extraCredentialsProvider is already set");
    }

    @Override
    public Map<String, String> getExtraCredentials(String user)
    {
        requireNonNull(user, "user is null");
        return configuredProvider.get()
                .map(provider -> provider.getExtraCredentials(user))
                .orElse(Map.of());
    }
}
