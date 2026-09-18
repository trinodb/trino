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

import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.invoke.MethodType.methodType;

/**
 * Binds method handles and constants used by a generated class. The bindings are attached
 * to the hidden class as class data: constants load as dynamic constants and method handles
 * are invoked exactly through a dynamic constant load.
 */
public final class CallSiteBinder
{
    private final ClassLoader classLoader = CallSiteBinder.class.getClassLoader();
    private final List<Object> bindings = new ArrayList<>();
    // Bindings deduplicate by identity: repeated types and cached operator handles are
    // bound once per generated class, and identical dynamic constant loads then share a
    // single constant pool entry and resolution
    private final Map<MethodHandle, Binding> methodHandleBindings = new IdentityHashMap<>();
    private final Map<ConstantKey, Binding> constantBindings = new HashMap<>();

    public Binding bind(MethodHandle method)
    {
        return methodHandleBindings.computeIfAbsent(method, handle -> {
            // Bound handles can come from plugin class loaders. Hide reference
            // types the generated-code loader cannot resolve to the same class.
            MethodType type = getAccessibleType(handle.type());
            if (!handle.type().equals(type)) {
                handle = handle.asType(type);
            }

            return addBinding(handle, type, Binding.Kind.HANDLE);
        });
    }

    public Binding bind(Object constant, Class<?> type)
    {
        // stored raw so it can be loaded as a dynamic constant from the class data
        return constantBindings.computeIfAbsent(
                new ConstantKey(constant, type),
                _ -> addBinding(constant, methodType(getAccessibleType(type)), Binding.Kind.CONSTANT));
    }

    private Binding addBinding(Object value, MethodType type, Binding.Kind kind)
    {
        Binding binding = new Binding(bindings.size(), type, kind);
        bindings.add(value);
        return binding;
    }

    public List<Object> getClassData()
    {
        return ImmutableList.copyOf(bindings);
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
                .add("bindings", bindings)
                .toString();
    }

    private record ConstantKey(Object constant, Class<?> type)
    {
        @Override
        public boolean equals(Object other)
        {
            return other instanceof ConstantKey that && constant == that.constant && type == that.type;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode(constant) * 31 + type.hashCode();
        }
    }
}
