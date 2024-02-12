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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.DoNotCall;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public final class DataAttributes
{
    final Map<String, Object> attributes;

    DataAttributes(Map<String, Object> attributes)
    {
        this.attributes = ImmutableMap.copyOf(firstNonNull(attributes, ImmutableMap.of()));
    }

    public static DataAttributes empty()
    {
        return new DataAttributes(ImmutableMap.of());
    }

    public <T> T get(DataAttribute attribute, Class<T> clazz)
    {
        return getOptional(attribute, clazz)
                .orElseThrow(() -> new IllegalArgumentException(format("Required data attribute '%s' does not exist", attribute.name())));
    }

    public <T> Optional<T> getOptional(DataAttribute attribute, Class<T> clazz)
    {
        return Optional.ofNullable(attributes.get(attribute.attributeName()))
                .map(value -> attribute.decode(clazz, value));
    }

    @JsonProperty("attributes")
    public Map<String, Object> toMap()
    {
        return attributes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataAttributes that = (DataAttributes) o;
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("attributes", attributes.keySet())
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(attributes);
    }

    @JsonCreator
    @DoNotCall
    public static DataAttributes fromMap(Map<String, Object> attributes)
    {
        Builder builder = DataAttributes.builder();
        attributes.forEach(builder::set); // Fixes the types after the deserialization
        return builder.build();
    }

    public Builder toBuilder()
    {
        return new Builder(this);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(DataAttributes dataAttributes)
    {
        return new Builder(dataAttributes);
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        private Builder() {}

        private Builder(DataAttributes attributes)
        {
            builder.putAll(attributes.attributes);
        }

        public <T> Builder set(DataAttribute attribute, T value)
        {
            verify(attribute.javaClass().isInstance(value), "Invalid value type: %s for attribute: %s", value.getClass(), attribute.attributeName());
            builder.put(attribute.attributeName(), value);
            return this;
        }

        public Builder set(String key, Object value)
        {
            DataAttribute attribute = DataAttribute.getByName(key);
            return set(attribute, attribute.decode(attribute.javaClass(), value));
        }

        public DataAttributes build()
        {
            return new DataAttributes(builder.buildKeepingLast());
        }
    }
}
