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

import io.airlift.bytecode.DynamicClassLoader;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.lang.constant.ConstantDescs.DEFAULT_NAME;
import static java.lang.invoke.MethodHandles.constant;
import static java.util.Objects.requireNonNull;

public final class Bootstrap
{
    public static final Method BOOTSTRAP_METHOD;

    static {
        try {
            BOOTSTRAP_METHOD = Bootstrap.class.getMethod("bootstrap", MethodHandles.Lookup.class, String.class, MethodType.class, long.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private Bootstrap() {}

    public static CallSite bootstrap(MethodHandles.Lookup callerLookup, String name, MethodType type, long bindingId)
    {
        if (callerLookup.lookupClass().isHidden()) {
            // hidden class defined with the call site bindings attached as class data
            List<?> bindings;
            try {
                bindings = MethodHandles.classData(callerLookup, DEFAULT_NAME, List.class);
            }
            catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Failed to read class data of " + callerLookup.lookupClass().getName(), e);
            }
            checkArgument(bindings != null, "Expected %s to have call site bindings as class data", callerLookup.lookupClass().getName());

            Object binding = requireNonNull(bindings.get(toIntExact(bindingId)), "binding is null");
            if (binding instanceof MethodHandle handle) {
                return new ConstantCallSite(handle);
            }
            return new ConstantCallSite(constant(type.returnType(), binding));
        }

        ClassLoader classLoader = callerLookup.lookupClass().getClassLoader();
        checkArgument(classLoader instanceof DynamicClassLoader, "Expected %s's classloader to be of type %s", callerLookup.lookupClass().getName(), DynamicClassLoader.class.getName());

        DynamicClassLoader dynamicClassLoader = (DynamicClassLoader) classLoader;
        MethodHandle target = dynamicClassLoader.getCallSiteBindings().get(bindingId);
        checkArgument(target != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());

        return new ConstantCallSite(target);
    }
}
