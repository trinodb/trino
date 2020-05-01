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
package io.prestosql.tests.product.launcher.cli;

import io.airlift.airline.DefaultCommandFactory;
import io.prestosql.tests.product.launcher.Extensions;

import java.lang.reflect.Constructor;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExtensionsProvidingCommandFactory<T>
        extends DefaultCommandFactory<T>
{
    private Extensions extensions;

    public ExtensionsProvidingCommandFactory(Extensions extensions)
    {
        this.extensions = requireNonNull(extensions, "extensions is null");
    }

    @Override
    @SuppressWarnings("unchecked")
    public T createInstance(Class<?> type)
    {
        return (T) create(type);
    }

    private Object create(Class<?> type)
    {
        Constructor<?> constructor = null;
        try {
            constructor = type.getConstructor(Extensions.class);
        }
        catch (NoSuchMethodException ignore) {
        }

        try {
            if (constructor != null) {
                return constructor.newInstance(extensions);
            }

            try {
                constructor = type.getConstructor();
            }
            catch (NoSuchMethodException ignore) {
                throw new RuntimeException(format("%s has no constructor taking Extensions and no no-arg constructor", type));
            }

            return constructor.newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
