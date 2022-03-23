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
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.session.PropertyMetadata.Flag;

import java.util.EnumSet;
import java.util.function.Consumer;

import static io.trino.spi.session.PropertyMetadata.Flag.HIDDEN;
import static io.trino.spi.session.PropertyMetadata.NO_FLAGS;
import static io.trino.spi.session.PropertyMetadata.flags;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class PropertyMetadataUtil
{
    private PropertyMetadataUtil() {}

    @Deprecated
    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return dataSizeProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue)
    {
        return dataSizeProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, EnumSet<Flag> flags)
    {
        return dataSizeProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, Consumer<DataSize> validation, boolean hidden)
    {
        return dataSizeProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, Consumer<DataSize> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                flags,
                object -> {
                    DataSize value = DataSize.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                DataSize::toString);
    }

    @Deprecated
    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, boolean hidden)
    {
        return durationProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue)
    {
        return durationProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, EnumSet<Flag> flags)
    {
        return durationProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, Consumer<Duration> validation, boolean hidden)
    {
        return durationProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, Consumer<Duration> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                Duration.class,
                defaultValue,
                flags,
                object -> {
                    Duration value = Duration.valueOf((String) object);
                    validation.accept(value);
                    return value;
                },
                Duration::toString);
    }
}
