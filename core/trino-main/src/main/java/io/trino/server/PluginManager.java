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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.connector.CatalogFactory;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.trino.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("org.openjdk.jol.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private final PluginsProvider pluginsProvider;
    private final CatalogFactory connectorFactory;
    private final TypeRegistry typeRegistry;
    private final HandleResolver handleResolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final Set<PluginInstaller> pluginInstallers;

    @Inject
    public PluginManager(
            PluginsProvider pluginsProvider,
            CatalogFactory connectorFactory,
            TypeRegistry typeRegistry,
            HandleResolver handleResolver,
            Set<PluginInstaller> pluginInstallers)
    {
        this.pluginsProvider = requireNonNull(pluginsProvider, "pluginsProvider is null");
        this.connectorFactory = requireNonNull(connectorFactory, "connectorFactory is null");
        this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.pluginInstallers = requireNonNull(pluginInstallers, "pluginInstallers is null");
    }

    public void loadPlugins()
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        pluginsProvider.loadPlugins(this::loadPlugin, PluginManager::createClassLoader);

        typeRegistry.verifyTypes();
    }

    private void loadPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader)
    {
        log.info("-- Loading plugin %s --", plugin);

        PluginClassLoader pluginClassLoader = createClassLoader.get();

        log.debug("Classpath for plugin:");
        for (URL url : pluginClassLoader.getURLs()) {
            log.debug("    %s", url.getPath());
        }

        handleResolver.registerClassLoader(pluginClassLoader);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }

        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        checkState(!plugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));

        for (Plugin plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            installPlugin(plugin, pluginClassLoader::duplicate);
        }
    }

    public void installPlugin(Plugin plugin, Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        installPluginInternal(plugin, duplicatePluginClassLoaderFactory);
        typeRegistry.verifyTypes();
    }

    private void installPluginInternal(Plugin plugin, Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", connectorFactory.getName());
            this.connectorFactory.addConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory);
        }

        for (PluginInstaller pluginInstaller : pluginInstallers) {
            pluginInstaller.installPlugin(plugin);
        }
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls)
    {
        ClassLoader parent = PluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }

    public interface PluginsProvider
    {
        void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader);

        interface Loader
        {
            void load(String description, Supplier<PluginClassLoader> getClassLoader);
        }

        interface ClassLoaderFactory
        {
            PluginClassLoader create(String pluginName, List<URL> urls);
        }
    }
}
