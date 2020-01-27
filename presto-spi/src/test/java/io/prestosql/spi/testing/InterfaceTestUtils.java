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
package io.prestosql.spi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Defaults.defaultValue;
import static com.google.common.reflect.Reflection.newProxy;
import static io.prestosql.spi.block.TestingSession.SESSION;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public final class InterfaceTestUtils
{
    private InterfaceTestUtils() {}

    public static <I, C extends I> void assertAllMethodsOverridden(Class<I> iface, Class<C> clazz)
    {
        assertEquals(ImmutableSet.copyOf(clazz.getInterfaces()), ImmutableSet.of(iface));
        for (Method method : iface.getMethods()) {
            try {
                Method override = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
                if (!method.getReturnType().isAssignableFrom(override.getReturnType())) {
                    fail(format("%s is not assignable from %s for method %s", method.getReturnType(), override.getReturnType(), method));
                }
            }
            catch (NoSuchMethodException e) {
                fail(format("%s does not override [%s]", clazz.getName(), method));
            }
        }
    }

    public static <I, C extends I> void assertProperForwardingMethodsAreCalled(Class<I> iface, Function<I, C> forwardingInstanceFactory)
    {
        assertProperForwardingMethodsAreCalled(iface, forwardingInstanceFactory, clazz -> null);

    }
    public static <I, C extends I> void assertProperForwardingMethodsAreCalled(
            Class<I> iface,
            Function<I, C> forwardingInstanceFactory,
            Function<Class<?>, Object> defaultValueSupplier)
    {
        for (Method actualMethod : iface.getDeclaredMethods()) {
            Object[] actualArguments = new Object[actualMethod.getParameterCount()];
            for (int i = 0; i < actualArguments.length; i++) {
                actualArguments[i] = getDefaultValue(defaultValueSupplier, actualMethod.getParameterTypes()[i]);
            }
            C forwardingInstance = forwardingInstanceFactory.apply(
                    newProxy(iface, (proxy, expectedMethod, expectedArguments) -> {
                        assertEquals(actualMethod.getName(), expectedMethod.getName());
                        // TODO assert arguments
                        return getDefaultValue(defaultValueSupplier, actualMethod.getReturnType());
                    }));

            try {
                if (!defines(forwardingInstance, actualMethod)) {
                    continue;
                }

                actualMethod.invoke(forwardingInstance, actualArguments);
            }
            catch (Exception e) {
                throw new RuntimeException(format("Invocation of %s has failed", actualMethod), e);
            }
        }
    }

    private static boolean defines(Object forwardingInstance, Method actualMethod)
            throws Exception
    {
        Class<?> forwardingClass = forwardingInstance.getClass();
        Method forwardingMethod = forwardingClass.getMethod(actualMethod.getName(), actualMethod.getParameterTypes());
        return forwardingClass == forwardingMethod.getDeclaringClass();
    }

    private static Object getDefaultValue(Function<Class<?>, Object> defaultValueSupplier, Class<?> type)
    {
        if (type.isPrimitive()) {
            return defaultValue(type);
        }
        Object value = DEFAULTS.apply(type);
        if (value != null) {
            return value;
        }
        return defaultValueSupplier.apply(type);
    }

    private static final Function<Class, Object> DEFAULTS = clazz -> {
        if (clazz.equals(String.class)) {
            return "string";
        }
        if (clazz.equals(Set.class)) {
            return ImmutableSet.of();
        }
        if (clazz.equals(List.class)) {
            return ImmutableList.of();
        }
        if (clazz.equals(Map.class)) {
            return ImmutableMap.of();
        }
        if (clazz.equals(Optional.class)) {
            return Optional.empty();
        }
        if (clazz.equals(SchemaTableName.class)) {
            return new SchemaTableName("schema", "table");
        }
        if (clazz.equals(ConnectorSession.class)) {
            return SESSION;
        }
        return null;
    };
}
