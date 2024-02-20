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
package io.trino.spi.testing;

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Defaults.defaultValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.difference;
import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public final class InterfaceTestUtils
{
    private InterfaceTestUtils() {}

    public static <I, C extends I> void assertAllMethodsOverridden(Class<I> iface, Class<C> clazz)
    {
        assertAllMethodsOverridden(iface, clazz, ImmutableSet.of());
    }

    public static <I, C extends I> void assertAllMethodsOverridden(Class<I> iface, Class<C> clazz, Set<Method> exclusions)
    {
        checkArgument(iface.isAssignableFrom(clazz), "%s is not supertype of %s", iface, clazz);
        exclusions = new HashSet<>(exclusions);
        for (Method method : iface.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }
            try {
                Method override = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                if (!method.getReturnType().isAssignableFrom(override.getReturnType())) {
                    fail(format("%s is not assignable from %s for method %s", method.getReturnType(), override.getReturnType(), method));
                }
            }
            catch (NoSuchMethodException e) {
                if (exclusions.remove(method)) {
                    // ignored
                }
                else {
                    fail(format("%s does not override [%s]", clazz.getName(), method));
                }
            }
        }

        if (!exclusions.isEmpty()) {
            fail("Following exclusions are redundant: " + exclusions);
        }
    }

    public static <I, C extends I> void assertProperForwardingMethodsAreCalled(Class<I> iface, Function<I, C> forwardingInstanceFactory)
    {
        assertProperForwardingMethodsAreCalled(iface, forwardingInstanceFactory, ImmutableSet.of());
    }

    public static <I, C extends I> void assertProperForwardingMethodsAreCalled(Class<I> iface, Function<I, C> forwardingInstanceFactory, Set<Method> exclusions)
    {
        for (Method actualMethod : difference(ImmutableSet.copyOf(iface.getMethods()), exclusions)) {
            Object[] actualArguments = new Object[actualMethod.getParameterCount()];
            for (int i = 0; i < actualArguments.length; i++) {
                if (actualMethod.getParameterTypes()[i].isPrimitive()) {
                    actualArguments[i] = defaultValue(actualMethod.getParameterTypes()[i]);
                }
            }
            C forwardingInstance = forwardingInstanceFactory.apply(
                    newProxy(iface, (proxy, expectedMethod, expectedArguments) -> {
                        assertThat(actualMethod.getName()).isEqualTo(expectedMethod.getName());
                        // TODO assert arguments

                        if (actualMethod.getReturnType().isPrimitive()) {
                            return defaultValue(actualMethod.getReturnType());
                        }
                        return null;
                    }));

            try {
                actualMethod.invoke(forwardingInstance, actualArguments);
            }
            catch (Exception e) {
                throw new RuntimeException(format("Invocation of %s has failed", actualMethod), e);
            }
        }
    }
}
