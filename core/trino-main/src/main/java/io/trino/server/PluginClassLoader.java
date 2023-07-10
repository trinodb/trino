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
import io.trino.spi.connector.CatalogHandle;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class PluginClassLoader
        extends URLClassLoader
{
    private final String id;
    private final String pluginName;
    private final Optional<CatalogHandle> catalogHandle;
    private final ClassLoader spiClassLoader;
    private final List<String> spiPackages;
    private final List<String> spiResources;

    public PluginClassLoader(
            String pluginName,
            List<URL> urls,
            ClassLoader spiClassLoader,
            List<String> spiPackages)
    {
        this(pluginName,
                Optional.empty(),
                urls,
                spiClassLoader,
                spiPackages,
                spiPackages.stream()
                        .map(PluginClassLoader::classNameToResource)
                        .collect(toImmutableList()));
    }

    private PluginClassLoader(
            String pluginName,
            Optional<CatalogHandle> catalogHandle,
            List<URL> urls,
            ClassLoader spiClassLoader,
            Iterable<String> spiPackages,
            Iterable<String> spiResources)
    {
        // plugins should not have access to the system (application) class loader
        super(urls.toArray(new URL[0]), getPlatformClassLoader());
        this.pluginName = requireNonNull(pluginName, "pluginName is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.spiClassLoader = requireNonNull(spiClassLoader, "spiClassLoader is null");
        this.spiPackages = ImmutableList.copyOf(spiPackages);
        this.spiResources = ImmutableList.copyOf(spiResources);
        this.id = pluginName + catalogHandle.map(name -> ":%s:%s".formatted(name.getCatalogName(), name.getVersion())).orElse("");
    }

    public PluginClassLoader duplicate(CatalogHandle catalogHandle)
    {
        checkState(this.catalogHandle.isEmpty(), "class loader is already a duplicate");
        return new PluginClassLoader(
                pluginName,
                Optional.of(requireNonNull(catalogHandle, "catalogHandle is null")),
                ImmutableList.copyOf(getURLs()),
                spiClassLoader,
                spiPackages,
                spiResources);
    }

    public PluginClassLoader withUrl(URL url)
    {
        List<URL> urls = ImmutableList.<URL>builder().add(getURLs()).add(url).build();
        return new PluginClassLoader(pluginName, catalogHandle, urls, spiClassLoader, spiPackages, spiResources);
    }

    public String getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("plugin", pluginName)
                .add("catalog", catalogHandle.orElse(null))
                .add("identityHash", "@" + Integer.toHexString(identityHashCode(this)))
                .toString();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        // grab the magic lock
        synchronized (getClassLoadingLock(name)) {
            // Check if class is in the loaded classes cache
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }

            // If this is an SPI class, only check SPI class loader
            if (isSpiClass(name)) {
                return resolveClass(spiClassLoader.loadClass(name), resolve);
            }

            // Look for class locally
            return super.loadClass(name, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name)
    {
        // If this is an SPI resource, only check SPI class loader
        if (isSpiResource(name)) {
            return spiClassLoader.getResource(name);
        }

        // Look for resource locally
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        // If this is an SPI resource, use SPI resources
        if (isSpiClass(name)) {
            return spiClassLoader.getResources(name);
        }

        // Use local resources
        return super.getResources(name);
    }

    private boolean isSpiClass(String name)
    {
        // todo maybe make this more precise and only match base package
        return spiPackages.stream().anyMatch(name::startsWith);
    }

    private boolean isSpiResource(String name)
    {
        // todo maybe make this more precise and only match base package
        return spiResources.stream().anyMatch(name::startsWith);
    }

    private static String classNameToResource(String className)
    {
        return className.replace('.', '/');
    }
}
