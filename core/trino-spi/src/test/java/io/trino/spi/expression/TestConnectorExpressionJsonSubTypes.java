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
import com.google.common.reflect.ClassPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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

        Set<Class<?>> concreteSubtypes = ClassPath.from(ConnectorExpression.class.getClassLoader())
                .getTopLevelClasses(ConnectorExpression.class.getPackageName()).stream()
                .map(ClassPath.ClassInfo::load)
                .filter(ConnectorExpression.class::isAssignableFrom)
                .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                .collect(toImmutableSet());

        // Every concrete ConnectorExpression subtype must be registered for JSON deserialization,
        // otherwise deserializing an expression containing it fails at runtime with an unknown subtype error.
        assertThat(concreteSubtypes).containsExactlyInAnyOrderElementsOf(registeredSubtypes);
    }
}
