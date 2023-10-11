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
package io.trino.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class SystemPropertyParameterResolver
        implements ParameterResolver
{
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return parameterContext.isAnnotated(SystemProperty.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        SystemProperty annotation = parameterContext.findAnnotation(SystemProperty.class).orElseThrow(() ->
                new ParameterResolutionException("Missing @SystemProperty for class: %s, parameter: %s".formatted(extensionContext.getRequiredTestClass(), parameterContext.getParameter().getName())));

        String propertyName = annotation.value();
        String value = System.getProperty(propertyName);
        if (value == null) {
            throw new ParameterResolutionException("Could not resolve system property '%s' for class: %s, parameter: %s".formatted(propertyName, extensionContext.getRequiredTestClass(), parameterContext.getParameter().getName()));
        }
        return value;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public @interface SystemProperty
    {
        String value();
    }
}
