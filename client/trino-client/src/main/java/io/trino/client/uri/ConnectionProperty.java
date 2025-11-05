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

import static java.lang.String.format;

public interface ConnectionProperty<V, T>
{
    default String getKey()
    {
        return getPropertyName().toString();
    }

    PropertyName getPropertyName();

    V encodeValue(T value);

    T decodeValue(V value);

    default String[] getChoices()
    {
        return null;
    }

    boolean isRequired(Properties properties);

    default boolean isValid(Properties properties)
    {
        return validate(properties).isEmpty();
    }

    Optional<T> getValue(Properties properties);

    default T getRequiredValue(Properties properties)
    {
        return getValue(properties).orElseThrow(() -> new RuntimeException(format("Connection property '%s' is required", getPropertyName())));
    }

    default T getValueOrDefault(Properties properties, T defaultValue)
    {
        return getValue(properties).orElse(defaultValue);
    }

    Optional<RuntimeException> validate(Properties properties);
}
