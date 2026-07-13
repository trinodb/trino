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
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class CallSiteBinder
{
    private final ClassLoader classLoader;
    private final boolean hiddenClassGeneration;
    private final List<Object> bindings = new ArrayList<>();
    // Bindings deduplicate by identity: repeated types and cached operator handles are
    // bound once per generated class, and identical dynamic constant loads then share a
    // single constant pool entry and resolution
    private final Map<MethodHandle, Binding> methodHandleBindings = new IdentityHashMap<>();
    private final Map<ConstantKey, Binding> constantBindings = new HashMap<>();

    public CallSiteBinder()
    {
        this(CallSiteBinder.class.getClassLoader());
    }

    public CallSiteBinder(ClassLoader classLoader)
    {
        this(classLoader, false);
    }

    private CallSiteBinder(ClassLoader classLoader, boolean hiddenClassGeneration)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.hiddenClassGeneration = hiddenClassGeneration;
    }

    /**
     * Binder for classes defined with {@code CompilerUtils.defineHiddenClass}: bindings are
     * attached to the class as class data instead of a {@code DynamicClassLoader}, and bound
     * constants are loaded as dynamic constants.
     */
    public static CallSiteBinder forHiddenClassGeneration()
    {
        return new CallSiteBinder(CallSiteBinder.class.getClassLoader(), true);
    }

    public Binding bind(MethodHandle method)
    {
        return methodHandleBindings.computeIfAbsent(method, handle -> {
            // Bound handles can come from plugin class loaders. Hide reference
            // types the generated-code loader cannot resolve to the same class.
            MethodType type = getAccessibleType(handle.type());
            if (!handle.type().equals(type)) {
                handle = handle.asType(type);
            }

            return addBinding(handle, type, hiddenClassGeneration ? Binding.Kind.CLASS_DATA_HANDLE : Binding.Kind.CALL_SITE);
        });
    }

    public Binding bind(Object constant, Class<?> type)
    {
        return constantBindings.computeIfAbsent(new ConstantKey(constant, type), _ -> {
            if (hiddenClassGeneration) {
                // stored raw so it can be loaded as a dynamic constant from the class data
                return addBinding(constant, methodType(getAccessibleType(type)), Binding.Kind.CLASS_DATA_CONSTANT);
            }
            return bind(MethodHandles.constant(type, constant));
        });
    }

    private Binding addBinding(Object value, MethodType type, Binding.Kind kind)
    {
        Binding binding = new Binding(bindings.size(), type, kind);
        bindings.add(value);
        return binding;
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

    public Map<Long, MethodHandle> getBindings()
    {
        checkState(!hiddenClassGeneration, "hidden class bindings must be retrieved with getClassData");
        ImmutableMap.Builder<Long, MethodHandle> map = ImmutableMap.builder();
        for (int i = 0; i < bindings.size(); i++) {
            map.put((long) i, (MethodHandle) bindings.get(i));
        }
        return map.buildOrThrow();
    }

    public List<Object> getClassData()
    {
        checkState(hiddenClassGeneration, "class data is only available for hidden class generation");
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
                .add("hiddenClassGeneration", hiddenClassGeneration)
                .add("bindings", bindings)
                .toString();
    }
}
