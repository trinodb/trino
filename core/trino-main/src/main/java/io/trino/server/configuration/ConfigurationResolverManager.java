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
package io.trino.server.configuration;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.server.PluginClassLoader;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.configuration.ConfigurationValueResolver;
import io.trino.spi.configuration.ConfigurationValueResolverFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.google.common.io.Files.getNameWithoutExtension;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.server.PluginManager.createClassLoader;
import static io.trino.server.ServerPluginsProvider.buildClassPath;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Locale.ENGLISH;

public final class ConfigurationResolverManager
{
    private static final Logger log = Logger.get(ConfigurationResolverManager.class);

    private static final String NAME_PROPERTY = "configuration-provider.name";

    private static final Pattern CONFIG_PRODIVER_NAME_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9_-]*");

    private final Map<String, ConfigurationValueResolverFactory> configurationProviderFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Map<String, ConfigurationValueResolver>> configurationProviders = new AtomicReference<>(ImmutableMap.of());
    private final File installedConfigurationResolver;
    private final List<File> configFiles;

    @Inject
    public ConfigurationResolverManager(ConfigurationResolverConfig config)
    {
        this.installedConfigurationResolver = config.getInstalledConfigurationResolver();
        configFiles = ImmutableList.copyOf(config.getConfigurationResolverFiles());
    }

    public void load()
    {
        listFiles(installedConfigurationResolver).stream()
                .filter(File::isDirectory)
                .forEach(file -> loadConfigurationResolvers(file.getAbsolutePath(), () -> createClassLoader(file.getName(), buildClassPath(file))));
        ImmutableMap.Builder<String, ConfigurationValueResolver> builder = ImmutableMap.builder();
        for (File configFile : configFiles) {
            builder.put(getNameWithoutExtension(configFile.getName()), loadConfigProvider(configFile.getAbsoluteFile()));
        }
        this.configurationProviders.set(builder.buildOrThrow());
    }

    private static List<File> listFiles(File path)
    {
        try {
            try (DirectoryStream<Path> directoryStream = newDirectoryStream(path.toPath())) {
                return stream(directoryStream)
                        .map(Path::toFile)
                        .sorted()
                        .collect(toImmutableList());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadConfigurationResolvers(String plugin, Supplier<PluginClassLoader> createClassLoader)
    {
        log.info("-- Loading plugin %s --", plugin);

        PluginClassLoader pluginClassLoader = createClassLoader.get();

        log.debug("Classpath for plugin:");
        for (URL url : pluginClassLoader.getURLs()) {
            log.debug("    %s", url.getPath());
        }

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }

        log.info("-- Finished loading plugin %s --", plugin);
    }

    public ConfigurationResolver getConfigurationResolver()
    {
        return new ConfigurationResolver(configurationProviders.get());
    }

    public void addConfigurationProviderFactory(ConfigurationValueResolverFactory configurationValueResolverFactory)
    {
        configurationProviderFactories.put(configurationValueResolverFactory.getName().toLowerCase(ENGLISH), configurationValueResolverFactory);
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader)
    {
        ServiceLoader<ConfigurationValueResolverFactory> serviceLoader = ServiceLoader.load(ConfigurationValueResolverFactory.class, pluginClassLoader);
        serviceLoader.stream().forEach(new Consumer<ServiceLoader.Provider<ConfigurationValueResolverFactory>>() {
            @Override
            public void accept(ServiceLoader.Provider<ConfigurationValueResolverFactory> configurationValueResolverFactoryProvider)
            {
                addConfigurationProviderFactory(configurationValueResolverFactoryProvider.get());
            }
        });
    }

    private ConfigurationValueResolver loadConfigProvider(File configFile)
    {
        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Configuration resolver configuration %s does not contain '%s'", configFile, NAME_PROPERTY);
        verify(CONFIG_PRODIVER_NAME_PATTERN.matcher(name).matches(), "Configuration resolver name doesn't match pattern [a-zA-Z][a-zA-Z0-9_-]*");

        return loadConfigProvider(name, properties);
    }

    private ConfigurationValueResolver loadConfigProvider(String configProviderName, Map<String, String> properties)
    {
        log.info("-- Loading configuration resolver --");

        ConfigurationValueResolverFactory factory = configurationProviderFactories.get(configProviderName);
        checkState(factory != null, "Configuration resolver '%s' is not registered", configProviderName);

        ConfigurationValueResolver configurationValueResolver;
        try (var _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            configurationValueResolver = factory.createConfigurationValueResolver(ImmutableMap.copyOf(properties));
        }

        log.info("-- Loaded configuration resolver %s --", configProviderName);
        return configurationValueResolver;
    }
}
