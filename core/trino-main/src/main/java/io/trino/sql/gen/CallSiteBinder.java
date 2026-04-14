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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CallSiteBinder
{
    private int nextId;

    private final ClassLoader classLoader;
    private final Map<Long, MethodHandle> bindings = new HashMap<>();

    public CallSiteBinder()
    {
        this(CallSiteBinder.class.getClassLoader());
    }

    public CallSiteBinder(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    public Binding bind(MethodHandle method)
    {
        // Bound handles can come from plugin class loaders. Hide reference
        // types the generated-code loader cannot resolve to the same class.
        MethodType type = getAccessibleType(method.type());
        if (!method.type().equals(type)) {
            method = method.asType(type);
        }

        long bindingId = nextId++;
        Binding binding = new Binding(bindingId, type);

        bindings.put(bindingId, method);
        return binding;
    }

    public Binding bind(Object constant, Class<?> type)
    {
        return bind(MethodHandles.constant(type, constant));
    }

    public Map<Long, MethodHandle> getBindings()
    {
        return ImmutableMap.copyOf(bindings);
    }

    public MethodType getAccessibleType(MethodType type)
    {
        MethodType accessibleType = type.changeReturnType(getAccessibleType(type.returnType()));
        for (int i = 0; i < type.parameterCount(); i++) {
            accessibleType = accessibleType.changeParameterType(i, getAccessibleType(type.parameterType(i)));
        }
        return accessibleType;
    }

    public Class<?> getAccessibleType(Class<?> type)
    {
        if (isAccessible(type)) {
            return type;
        }
        return Object.class;
    }

    private boolean isAccessible(Class<?> type)
    {
        if (type.isPrimitive()) {
            return true;
        }
        try {
            return Class.forName(type.getName(), false, classLoader) == type;
        }
        catch (ClassNotFoundException | LinkageError _) {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nextId", nextId)
                .add("bindings", bindings)
                .toString();
    }
}
