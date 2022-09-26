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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.server.PluginClassLoader;
import io.trino.server.PluginManager;
import io.trino.server.ServerPluginsProvider;
import io.trino.server.ServerPluginsProviderConfig;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.io.File;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Arrays.asList;

public class FunctionsReader
{
    private static final Logger log = Logger.get(FunctionsReader.class);

    private FunctionsReader() {}

    public static List<Plugin> loadFunctions(File path)
    {
        ServerPluginsProviderConfig config = new ServerPluginsProviderConfig();
        config.setInstalledPluginsDir(path.isDirectory() ? path : path.getParentFile());
        ServerPluginsProvider pluginsProvider = new ServerPluginsProvider(config, directExecutor());
        ImmutableList.Builder<Plugin> plugins = ImmutableList.builder();
        pluginsProvider.loadFunctions((plugin, createClassLoader) -> loadPlugin(createClassLoader, plugins), PluginManager::createClassLoader, path);
        return plugins.build();
    }

    private static void loadPlugin(Supplier<PluginClassLoader> createClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        PluginClassLoader pluginClassLoader = createClassLoader.get();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadServicePlugin(pluginClassLoader, plugins);
        }
    }

    private static void loadServicePlugin(PluginClassLoader pluginClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> loadedPlugins = ImmutableList.copyOf(serviceLoader);
        checkState(!loadedPlugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));
        plugins.addAll(loadedPlugins);
    }
}
