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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.trino.connector.informationschema.InformationSchemaColumnHandle;
import io.trino.connector.informationschema.InformationSchemaSplit;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.connector.informationschema.InformationSchemaTransactionHandle;
import io.trino.connector.system.SystemColumnHandle;
import io.trino.connector.system.SystemSplit;
import io.trino.connector.system.SystemTableHandle;
import io.trino.connector.system.SystemTransactionHandle;
import io.trino.server.PluginClassLoader;
import io.trino.split.EmptySplit;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.SystemPartitioningHandle;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static java.util.Objects.requireNonNull;

public final class HandleResolver
{
    @GuardedBy("this")
    private final Map<String, Set<Class<?>>> catalogHandleClasses = new HashMap<>();
    @GuardedBy("this")
    private final Multiset<Class<?>> allHandleClasses = HashMultiset.create();
    @GuardedBy("this")
    private final BiMap<Class<?>, String> handleClassIds = HashBiMap.create();

    @Inject
    public HandleResolver()
    {
        addCatalogHandleClasses(REMOTE_CONNECTOR_ID.toString(), ImmutableSet.<Class<?>>builder()
                .add(RemoteSplit.class)
                .add(RemoteTransactionHandle.class)
                .add(SystemPartitioningHandle.class)
                .build());
        addCatalogHandleClasses("$system", ImmutableSet.<Class<?>>builder()
                .add(SystemTableHandle.class)
                .add(SystemColumnHandle.class)
                .add(SystemSplit.class)
                .add(SystemTransactionHandle.class)
                .build());
        addCatalogHandleClasses("$info_schema", ImmutableSet.<Class<?>>builder()
                .add(InformationSchemaTableHandle.class)
                .add(InformationSchemaColumnHandle.class)
                .add(InformationSchemaSplit.class)
                .add(InformationSchemaTransactionHandle.class)
                .build());
        addCatalogHandleClasses("$empty", ImmutableSet.<Class<?>>builder()
                .add(EmptySplit.class)
                .build());
    }

    public synchronized void addCatalogHandleClasses(String catalogName, Set<Class<?>> handleClasses)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(handleClasses, "handleClasses is null");
        Set<Class<?>> existing = catalogHandleClasses.putIfAbsent(catalogName, ImmutableSet.copyOf(handleClasses));
        checkState(existing == null, "Catalog is already registered: %s", catalogName);
        for (Class<?> handleClass : handleClasses) {
            allHandleClasses.add(handleClass);
            handleClassIds.put(handleClass, classId(handleClass));
        }
    }

    public synchronized void removeCatalogHandleClasses(String catalogName)
    {
        Set<Class<?>> classes = catalogHandleClasses.remove(catalogName);
        checkState(classes != null, "Catalog not registered: %s", catalogName);
        for (Class<?> handleClass : classes) {
            if (allHandleClasses.remove(handleClass, 1) == 1) {
                handleClassIds.remove(handleClass);
            }
        }
    }

    public synchronized String getId(Object tableHandle)
    {
        Class<?> handleClass = tableHandle.getClass();
        String id = handleClassIds.get(handleClass);
        checkArgument(id != null, "Handle class not registered: " + handleClass.getName());
        return id;
    }

    public synchronized Class<?> getHandleClass(String id)
    {
        Class<?> handleClass = handleClassIds.inverse().get(id);
        checkArgument(handleClass != null, "Handle ID not found: " + id);
        return handleClass;
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
