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
import io.trino.spi.Plugin;
import io.trino.spi.connector.CatalogHandle;

import java.net.URL;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface PluginManager
{
    static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.trino.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("org.openjdk.jol.")
            .build();

    void loadPlugins();

    void installPlugin(Plugin plugin, Function<CatalogHandle, ClassLoader> duplicatePluginClassLoaderFactory);

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls)
    {
        ClassLoader parent = PluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }

    interface PluginsProvider
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
