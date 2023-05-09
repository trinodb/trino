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
package io.trino.plugin.base.session;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;

import java.util.function.Consumer;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class PropertyMetadataUtil
{
    private PropertyMetadataUtil() {}

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return dataSizeProperty(name, description, defaultValue, value -> {}, hidden);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, Consumer<DataSize> validation, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                hidden,
                object -> {
                    DataSize value = DataSize.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                DataSize::toString);
    }

    public static void validateMinDataSize(String name, DataSize value, DataSize min)
    {
        if (value.compareTo(min) < 0) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "%s must be at least %s: %s".formatted(name, min, value));
        }
    }

    public static void validateMaxDataSize(String name, DataSize value, DataSize max)
    {
        if (value.compareTo(max) > 0) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "%s must be at most %s: %s".formatted(name, max, value));
        }
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, boolean hidden)
    {
        return durationProperty(name, description, defaultValue, value -> {}, hidden);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, Consumer<Duration> validation, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                Duration.class,
                defaultValue,
                hidden,
                object -> {
                    Duration value = Duration.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                Duration::toString);
    }
}
