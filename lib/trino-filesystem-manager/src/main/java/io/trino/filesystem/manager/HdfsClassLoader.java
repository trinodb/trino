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
package io.trino.filesystem.manager;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

// based on io.trino.server.PluginClassLoader
final class HdfsClassLoader
        extends URLClassLoader
{
    public HdfsClassLoader(List<URL> urls)
    {
        // This class loader should not have access to the system (application) class loader
        super(urls.toArray(URL[]::new), getPlatformClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name)) {
            // Check if class is in the loaded classes cache
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }

            // If this is an override class, only check override class loader
            if (isOverrideClass(name)) {
                return resolveClass(overrideClassLoader().loadClass(name), resolve);
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
        // If this is an override resource, only check override class loader
        if (isOverrideResource(name)) {
            return overrideClassLoader().getResource(name);
        }

        // Look for resource locally
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        // If this is an override resource, use override resources
        if (isOverrideResource(name)) {
            return overrideClassLoader().getResources(name);
        }

        // Use local resources
        return super.getResources(name);
    }

    private ClassLoader overrideClassLoader()
    {
        return getClass().getClassLoader();
    }

    private static boolean isOverrideResource(String name)
    {
        return isOverrideClass(name.replace('.', '/'));
    }

    private static boolean isOverrideClass(String name)
    {
        // SPI packages from io.trino.server.PluginManager and dependencies of trino-filesystem
        return hasPackage(name, "io.trino.spi.") ||
                hasPackage(name, "com.fasterxml.jackson.annotation.") ||
                hasPackage(name, "io.airlift.slice.") ||
                hasPackage(name, "org.openjdk.jol.") ||
                hasPackage(name, "io.opentelemetry.api.") ||
                hasPackage(name, "io.opentelemetry.context.") ||
                hasPackage(name, "com.google.common.") ||
                hasExactPackage(name, "io.trino.memory.context.") ||
                hasExactPackage(name, "io.trino.filesystem.");
    }

    private static boolean hasPackage(String name, String packageName)
    {
        checkArgument(!packageName.isEmpty() && packageName.charAt(packageName.length() - 1) == '.');
        return name.startsWith(packageName);
    }

    private static boolean hasExactPackage(String name, String packageName)
    {
        checkArgument(!packageName.isEmpty() && packageName.charAt(packageName.length() - 1) == '.');
        return name.startsWith(packageName) && (name.lastIndexOf('.') == (packageName.length() - 1));
    }
}
