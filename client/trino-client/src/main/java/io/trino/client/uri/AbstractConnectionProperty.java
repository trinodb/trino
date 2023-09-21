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

import com.google.common.reflect.TypeToken;

import java.io.File;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractConnectionProperty<V, T>
        implements ConnectionProperty<V, T>
{
    private final PropertyName propertyName;
    private final String key;
    private final Optional<T> defaultValue;
    private final Predicate<Properties> isRequired;
    private final Validator<Properties> validator;
    private final Converter<V, T> converter;
    private final String[] choices;

    protected AbstractConnectionProperty(
            PropertyName propertyName,
            Optional<T> defaultValue,
            Predicate<Properties> isRequired,
            Validator<Properties> validator,
            Converter<V, T> converter)
    {
        this.propertyName = requireNonNull(propertyName, "key is null");
        this.key = propertyName.toString();
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        this.isRequired = requireNonNull(isRequired, "isRequired is null");
        this.validator = requireNonNull(validator, "validator is null");
        this.converter = requireNonNull(converter, "converter is null");

        Class<? super T> type = new TypeToken<T>(getClass()) {}.getRawType();
        if (type == Boolean.class) {
            choices = new String[] {"true", "false"};
        }
        else if (Enum.class.isAssignableFrom(type)) {
            choices = Stream.of(type.getEnumConstants())
                    .map(Object::toString)
                    .toArray(String[]::new);
        }
        else {
            choices = null;
        }
    }

    protected AbstractConnectionProperty(
            PropertyName key,
            Predicate<Properties> required,
            Validator<Properties> allowed,
            Converter<V, T> converter)
    {
        this(key, Optional.empty(), required, allowed, converter);
    }

    @Override
    public PropertyName getPropertyName()
    {
        return propertyName;
    }

    @Override
    public DriverPropertyInfo getDriverPropertyInfo(Properties mergedProperties)
    {
        String currentValue = mergedProperties.getProperty(key);
        DriverPropertyInfo result = new DriverPropertyInfo(key, currentValue);
        result.required = isRequired.test(mergedProperties);
        result.choices = (choices != null) ? choices.clone() : null;
        return result;
    }

    @Override
    public boolean isRequired(Properties properties)
    {
        return isRequired.test(properties);
    }

    @Override
    public boolean isValid(Properties properties)
    {
        return !validator.validate(properties).isPresent();
    }

    @Override
    public Optional<T> getValue(Properties properties)
            throws SQLException
    {
        return getValueOrDefault(properties, defaultValue);
    }

    @Override
    public Optional<T> getValueOrDefault(Properties properties, Optional<T> defaultValue)
            throws SQLException
    {
        V value = (V) properties.get(key);
        if (value == null) {
            if (isRequired(properties) && !defaultValue.isPresent()) {
                throw new SQLException(format("Connection property %s is required", key));
            }
            return defaultValue;
        }

        try {
            return Optional.of(converter.convert(value));
        }
        catch (RuntimeException e) {
            if (isEmpty(value)) {
                throw new SQLException(format("Connection property %s value is empty", key), e);
            }
            throw new SQLException(format("Connection property %s value is invalid: %s", key, value), e);
        }
    }

    private boolean isEmpty(V value)
    {
        if (value instanceof String) {
            return ((String) value).isEmpty();
        }
        return false;
    }

    @Override
    public void validate(Properties properties)
            throws SQLException
    {
        if (properties.containsKey(key)) {
            Optional<String> message = validator.validate(properties);
            if (message.isPresent()) {
                throw new SQLException(message.get());
            }
        }

        getValue(properties);
    }

    protected static final Predicate<Properties> NOT_REQUIRED = properties -> false;

    protected static final Validator<Properties> ALLOWED = properties -> Optional.empty();

    interface Converter<V, T>
    {
        T convert(V value);
    }

    protected static final Converter<String, String> STRING_CONVERTER = String.class::cast;

    protected static final Converter<String, String> NON_EMPTY_STRING_CONVERTER = value -> {
        checkArgument(!value.isEmpty(), "value is empty");
        return value;
    };

    protected static final Converter<String, File> FILE_CONVERTER = File::new;

    protected static final Converter<String, Boolean> BOOLEAN_CONVERTER = value -> {
        switch (value.toLowerCase(ENGLISH)) {
            case "true":
                return true;
            case "false":
                return false;
        }
        throw new IllegalArgumentException("value must be 'true' or 'false'");
    };

    protected interface Validator<T>
    {
        /**
         * @param value Value to validate
         * @return An error message if the value is invalid or empty otherwise
         */
        Optional<String> validate(T value);

        default Validator<T> and(Validator<? super T> other)
        {
            requireNonNull(other, "other is null");
            // return the first non-empty optional
            return (t) -> {
                Optional<String> result = validate(t);
                if (result.isPresent()) {
                    return result;
                }
                return other.validate(t);
            };
        }
    }

    protected static <T> Validator<T> validator(Predicate<T> predicate, String errorMessage)
    {
        requireNonNull(predicate, "predicate is null");
        requireNonNull(errorMessage, "errorMessage is null");
        return value -> {
            if (predicate.test(value)) {
                return Optional.empty();
            }
            return Optional.of(errorMessage);
        };
    }

    protected interface CheckedPredicate<T>
    {
        boolean test(T t)
                throws SQLException;
    }

    protected static <T> Predicate<T> checkedPredicate(CheckedPredicate<T> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        return value -> {
            try {
                return predicate.test(value);
            }
            catch (SQLException e) {
                return false;
            }
        };
    }
}
