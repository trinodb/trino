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
package io.trino.spi.expression;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.module.ModuleReader;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectorExpressionJsonSubTypes
{
    @Test
    public void testAllConcreteSubtypesAreRegistered()
            throws IOException
    {
        Set<Class<?>> registeredSubtypes = Arrays.stream(ConnectorExpression.class.getAnnotation(JsonSubTypes.class).value())
                .map(JsonSubTypes.Type::value)
                .collect(toImmutableSet());

        Set<Class<?>> concreteSubtypes = findConcreteSubtypesInPackage();

        // Every concrete ConnectorExpression subtype must be registered for JSON deserialization,
        // otherwise deserializing an expression containing it fails at runtime with an unknown subtype error.
        assertThat(registeredSubtypes).containsExactlyInAnyOrderElementsOf(concreteSubtypes);
    }

    private static Set<Class<?>> findConcreteSubtypesInPackage()
            throws IOException
    {
        String packagePrefix = ConnectorExpression.class.getPackageName().replace('.', '/') + "/";
        try (ModuleReader moduleReader = ConnectorExpression.class.getModule().getLayer()
                .configuration()
                .findModule(ConnectorExpression.class.getModule().getName())
                .orElseThrow()
                .reference()
                .open()) {
            return moduleReader.list()
                    .filter(name -> name.startsWith(packagePrefix) && name.endsWith(".class") && !name.substring(packagePrefix.length()).contains("/"))
                    .map(name -> name.substring(0, name.length() - ".class".length()).replace('/', '.'))
                    .map(TestConnectorExpressionJsonSubTypes::loadClass)
                    .filter(ConnectorExpression.class::isAssignableFrom)
                    .filter(clazz -> clazz != ConnectorExpression.class)
                    .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                    .collect(toImmutableSet());
        }
    }

    private static Class<?> loadClass(String className)
    {
        try {
            return Class.forName(className, false, ConnectorExpression.class.getClassLoader());
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
