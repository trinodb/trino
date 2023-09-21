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
package io.trino.metadata;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.trino.server.PluginClassLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public final class HandleResolver
{
    private final Map<String, ClassLoader> classLoaders = new ConcurrentHashMap<>();

    @Inject
    public HandleResolver()
    {
        classLoaders.put("system", getClass().getClassLoader());
    }

    public void registerClassLoader(PluginClassLoader classLoader)
    {
        ClassLoader existingClassLoader = classLoaders.putIfAbsent(classLoader.getId(), classLoader);
        checkState(existingClassLoader == null, "Class loader already registered: %s", classLoader.getId());
    }

    public void unregisterClassLoader(PluginClassLoader classLoader)
    {
        boolean result = classLoaders.remove(classLoader.getId(), classLoader);
        checkState(result, "Class loader not registered: %s", classLoader.getId());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public String getId(Object tableHandle)
    {
        Class<?> handleClass = tableHandle.getClass();
        return classId(handleClass);
    }

    public Class<?> getHandleClass(String id)
    {
        int splitPoint = id.lastIndexOf(':');
        checkArgument(splitPoint > 1, "Invalid handle id: %s", id);
        String classLoaderId = id.substring(0, splitPoint);
        String className = id.substring(splitPoint + 1);

        ClassLoader classLoader = classLoaders.get(classLoaderId);
        checkArgument(classLoader != null, "Unknown handle id: %s", id);

        try {
            return classLoader.loadClass(className);
        }
        catch (ClassNotFoundException ignored) {
            throw new IllegalArgumentException("Handle ID not found: " + id);
        }
    }

    private static String classId(Class<?> handleClass)
    {
        return classLoaderId(handleClass) + ":" + handleClass.getName();
    }

    @SuppressWarnings("ObjectEquality")
    private static String classLoaderId(Class<?> handleClass)
    {
        ClassLoader classLoader = handleClass.getClassLoader();
        if (classLoader instanceof PluginClassLoader) {
            return ((PluginClassLoader) classLoader).getId();
        }
        checkArgument(classLoader == HandleResolver.class.getClassLoader(),
                "Handle [%s] has unknown class loader [%s]",
                handleClass.getName(),
                classLoader.getClass().getName());
        return "system";
    }
}
