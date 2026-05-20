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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SharedPackagesClassLoader
        extends URLClassLoader
{
    private final Set<String> sharedPackages = ConcurrentHashMap.newKeySet();

    public SharedPackagesClassLoader(ClassLoader parent)
    {
        super(new URL[0], parent);
    }

    public void registerPackages(Collection<String> packages)
    {
        sharedPackages.addAll(packages);
    }

    public void addUrls(Collection<URL> urls)
    {
        for (URL url : urls) {
            addURL(url);
        }
    }

    public boolean isSharedClass(String className)
    {
        return sharedPackages.stream().anyMatch(className::startsWith);
    }
}
