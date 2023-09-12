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
package io.trino.client;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class Row
{
    private final List<RowField> fields;

    private Row(List<RowField> fields)
    {
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    public List<RowField> getFields()
    {
        return fields;
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
        Row row = (Row) o;
        return Objects.equals(fields, row.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    @Override
    public String toString()
    {
        return fields.stream()
                .map(field -> {
                    if (field.getName().isPresent()) {
                        return field.getName().get() + "=" + field.getValue();
                    }
                    return String.valueOf(field.getValue());
                })
                .collect(joining(", ", "{", "}"));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderWithExpectedSize(int fields)
    {
        return new Builder(fields);
    }

    public static final class Builder
    {
        private final ImmutableList.Builder<RowField> fields;
        private int size;

        private Builder()
        {
            this.fields = ImmutableList.builder();
        }

        private Builder(int fields)
        {
            this.fields = ImmutableList.builderWithExpectedSize(fields);
        }

        public Builder addField(String name, @Nullable Object value)
        {
            return addField(Optional.of(name), value);
        }

        public Builder addUnnamedField(@Nullable Object value)
        {
            return addField(Optional.empty(), value);
        }

        public Builder addField(Optional<String> name, @Nullable Object value)
        {
            requireNonNull(name, "name is null");
            fields.add(new RowField(size++, name, value));
            return this;
        }

        public Row build()
        {
            return new Row(fields.build());
        }
    }
}
