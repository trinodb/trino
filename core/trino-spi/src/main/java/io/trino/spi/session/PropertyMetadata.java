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
package io.trino.spi.session;

import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.trino.spi.session.PropertyMetadata.Flag.HIDDEN;
import static io.trino.spi.session.PropertyMetadata.Flag.NOT_INHERITABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.EnumSet.noneOf;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;

public final class PropertyMetadata<T>
{
    public static final EnumSet<Flag> NO_FLAGS = noneOf(Flag.class);

    public static EnumSet<Flag> flags(Flag... flags)
    {
        return Arrays.stream(flags).collect(toCollection(() -> EnumSet.noneOf(Flag.class)));
    }

    public enum Flag
    {
        NOT_INHERITABLE,
        HIDDEN
    }

    private final String name;
    private final String description;
    private final Type sqlType;
    private final Class<T> javaType;
    private final T defaultValue;
    private final Function<Object, T> decoder;
    private final Function<T, Object> encoder;
    private final Set<Flag> flags;

    @Deprecated
    public PropertyMetadata(
            String name,
            String description,
            Type sqlType,
            Class<T> javaType,
            T defaultValue,
            boolean hidden,
            Function<Object, T> decoder,
            Function<T, Object> encoder)
    {
        this(name, description, sqlType, javaType, defaultValue, hidden ? flags(HIDDEN) : NO_FLAGS, decoder, encoder);
    }

    public PropertyMetadata(
            String name,
            String description,
            Type sqlType,
            Class<T> javaType,
            T defaultValue,
            Set<Flag> flags,
            Function<Object, T> decoder,
            Function<T, Object> encoder)
    {
        requireNonNull(name, "name is null");
        requireNonNull(description, "description is null");
        requireNonNull(sqlType, "sqlType is null");
        requireNonNull(javaType, "javaType is null");
        requireNonNull(flags, "flags is null");
        requireNonNull(decoder, "decoder is null");
        requireNonNull(encoder, "encoder is null");

        if (name.isEmpty() || !name.trim().toLowerCase(ENGLISH).equals(name)) {
            throw new IllegalArgumentException(format("Invalid property name '%s'", name));
        }
        if (description.isEmpty() || !description.trim().equals(description)) {
            throw new IllegalArgumentException(format("Invalid property description '%s'", description));
        }

        this.name = name;
        this.description = description;
        this.javaType = javaType;
        this.sqlType = sqlType;
        this.defaultValue = defaultValue;
        this.flags = unmodifiableSet(EnumSet.copyOf(flags));
        this.decoder = decoder;
        this.encoder = encoder;
    }

    /**
     * Name of the property.  This must be a valid identifier.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Description for the end user.
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * SQL type of the property.
     */
    public Type getSqlType()
    {
        return sqlType;
    }

    /**
     * Java type of this property.
     */
    public Class<T> getJavaType()
    {
        return javaType;
    }

    /**
     * Gets the default value for this property.
     */
    public T getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Is this property hidden from users?
     */
    public boolean isHidden()
    {
        return flags.contains(HIDDEN);
    }

    /**
     * Returns whether the property can be inherited in CREATE TABLE LIKE INCLUDING PROPERTIES statements
     */
    public boolean isInheritable()
    {
        return !flags.contains(NOT_INHERITABLE);
    }

    /**
     * Decodes the SQL type object value to the Java type of the property.
     */
    public T decode(Object value)
    {
        return decoder.apply(value);
    }

    /**
     * Encodes the Java type value to SQL type object value
     */
    public Object encode(T value)
    {
        return encoder.apply(value);
    }

    @Override
    public String toString()
    {
        return "PropertyMetadata{" + name + "}";
    }

    @Deprecated
    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, boolean hidden)
    {
        return booleanProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue)
    {
        return booleanProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, EnumSet<Flag> flags)
    {
        return booleanProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, Consumer<Boolean> validation, boolean hidden)
    {
        return booleanProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, Consumer<Boolean> validation)
    {
        return booleanProperty(name, description, defaultValue, validation, NO_FLAGS);
    }

    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, Consumer<Boolean> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                BOOLEAN,
                Boolean.class,
                defaultValue,
                flags,
                object -> {
                    boolean value = (Boolean) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }

    @Deprecated
    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, boolean hidden)
    {
        return integerProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue)
    {
        return integerProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, EnumSet<Flag> flags)
    {
        return integerProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, Consumer<Integer> validation, boolean hidden)
    {
        return integerProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, Consumer<Integer> validation)
    {
        return integerProperty(name, description, defaultValue, validation, NO_FLAGS);
    }

    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, Consumer<Integer> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                INTEGER,
                Integer.class,
                defaultValue,
                flags,
                object -> {
                    int value = (Integer) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }

    @Deprecated
    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, boolean hidden)
    {
        return longProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue)
    {
        return longProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, EnumSet<Flag> flags)
    {
        return longProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, Consumer<Long> validation, boolean hidden)
    {
        return longProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, Consumer<Long> validation)
    {
        return longProperty(name, description, defaultValue, validation, NO_FLAGS);
    }

    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, Consumer<Long> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                BIGINT,
                Long.class,
                defaultValue,
                flags,
                object -> {
                    long value = (Long) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }

    @Deprecated
    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, boolean hidden)
    {
        return doubleProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue)
    {
        return doubleProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, EnumSet<Flag> flags)
    {
        return doubleProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, Consumer<Double> validation, boolean hidden)
    {
        return doubleProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, Consumer<Double> validation)
    {
        return doubleProperty(name, description, defaultValue, validation, NO_FLAGS);
    }

    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, Consumer<Double> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                DOUBLE,
                Double.class,
                defaultValue,
                flags,
                object -> {
                    double value = (Double) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }

    @Deprecated
    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, boolean hidden)
    {
        return stringProperty(name, description, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue)
    {
        return stringProperty(name, description, defaultValue, value -> {}, NO_FLAGS);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, EnumSet<Flag> flags)
    {
        return stringProperty(name, description, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, Consumer<String> validation, boolean hidden)
    {
        return stringProperty(name, description, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, Consumer<String> validation)
    {
        return stringProperty(name, description, defaultValue, validation, NO_FLAGS);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, Consumer<String> validation, EnumSet<Flag> flags)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                String.class,
                defaultValue,
                flags,
                object -> {
                    String value = (String) object;
                    validation.accept(value);
                    return value;
                },
                object -> object);
    }

    @Deprecated
    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, boolean hidden)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, value -> {}, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, value -> {}, NO_FLAGS);
    }

    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, EnumSet<Flag> flags)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, value -> {}, flags);
    }

    @Deprecated
    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, Consumer<T> validation, boolean hidden)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, validation, hidden ? flags(HIDDEN) : NO_FLAGS);
    }

    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, Consumer<T> validation)
    {
        return enumProperty(name, descriptionPrefix, type, defaultValue, validation, NO_FLAGS);
    }

    public static <T extends Enum<T>> PropertyMetadata<T> enumProperty(String name, String descriptionPrefix, Class<T> type, T defaultValue, Consumer<T> validation, EnumSet<Flag> flags)
    {
        String allValues = EnumSet.allOf(type).stream()
                .map(Enum::name)
                .collect(joining(", ", "[", "]"));
        return new PropertyMetadata<>(
                name,
                format("%s. Possible values: %s", descriptionPrefix, allValues),
                createUnboundedVarcharType(),
                type,
                defaultValue,
                flags,
                value -> {
                    T enumValue;
                    try {
                        enumValue = Enum.valueOf(type, ((String) value).toUpperCase(ENGLISH));
                    }
                    catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(format("Invalid value [%s]. Valid values: %s", value, allValues), e);
                    }
                    validation.accept(enumValue);
                    return enumValue;
                },
                Enum::name);
    }
}
