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
package io.trino.hdfs.nativefs;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Native file system factories of each catalog, by scheme.
 * <p>
 * Hadoop instantiates file systems reflectively from the {@code Configuration}, so
 * {@link NativeHadoopFileSystem} cannot receive the factory through injection. Each catalog
 * registers its factories under a unique id, which is stored in the {@code Configuration}.
 */
public final class NativeFileSystemRegistry
{
    private static final Map<String, Map<String, TrinoFileSystemFactory>> FACTORIES = new ConcurrentHashMap<>();

    private NativeFileSystemRegistry() {}

    public static void register(String registryId, Map<String, TrinoFileSystemFactory> factories)
    {
        Map<String, TrinoFileSystemFactory> previous = FACTORIES.putIfAbsent(registryId, ImmutableMap.copyOf(factories));
        checkState(previous == null, "Native file systems already registered: %s", registryId);
    }

    public static void unregister(String registryId)
    {
        FACTORIES.remove(registryId);
    }

    public static Set<String> schemes(String registryId)
    {
        return FACTORIES.getOrDefault(registryId, ImmutableMap.of()).keySet();
    }

    public static Optional<TrinoFileSystemFactory> get(String registryId, String scheme)
    {
        return Optional.ofNullable(FACTORIES.getOrDefault(registryId, ImmutableMap.of()).get(scheme));
    }
}
