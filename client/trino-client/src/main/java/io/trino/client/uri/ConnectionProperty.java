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
package io.trino.client.uri;

import java.util.Optional;
import java.util.Properties;

public interface ConnectionProperty<V, T>
{
    default String getKey()
    {
        return getPropertyName().toString();
    }

    default String getDescription()
    {
        return getPropertyName().toString();
    }

    PropertyName getPropertyName();

    boolean isRequired(Properties properties);

    default boolean isValid(Properties properties)
    {
        return !validate(properties).isPresent();
    }

    Optional<RuntimeException> validate(Properties properties);

    Optional<T> resolveValue(Properties properties);

    default T resolveValueOrDefault(Properties properties, T defaultValue)
    {
        return resolveValue(properties).orElse(defaultValue);
    }

    V encodeValue(T value);

    T decodeValue(V value);

    default String[] getChoices()
    {
        return new String[]{};
    }
}
