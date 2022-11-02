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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

// A public facade for Row from trino-client
public final class Row
{
    private final io.trino.client.Row row;

    Row(io.trino.client.Row row)
    {
        this.row = requireNonNull(row, "row is null");
    }

    public List<RowField> getFields()
    {
        List<io.trino.client.RowField> fields = row.getFields();
        ImmutableList.Builder<RowField> results = ImmutableList.builderWithExpectedSize(fields.size());
        for (io.trino.client.RowField field : fields) {
            results.add(new RowField(field));
        }
        return results.build();
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
        Row other = (Row) o;
        return Objects.equals(row, other.row);
    }

    @Override
    public int hashCode()
    {
        return row.hashCode();
    }

    @Override
    public String toString()
    {
        return row.toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderWithExpectedSize(int fields)
    {
        return new Builder(fields);
    }

    // A public facade for Row.Builder from trino-client
    public static final class Builder
    {
        private final io.trino.client.Row.Builder builder;

        private Builder()
        {
            builder = io.trino.client.Row.builder();
        }

        private Builder(int fields)
        {
            builder = io.trino.client.Row.builderWithExpectedSize(fields);
        }

        public Builder addField(String name, @Nullable Object value)
        {
            builder.addField(name, value);
            return this;
        }

        public Builder addUnnamedField(@Nullable Object value)
        {
            builder.addUnnamedField(value);
            return this;
        }

        Builder addField(Optional<String> name, @Nullable Object value)
        {
            requireNonNull(name, "name is null");
            if (name.isPresent()) {
                return addField(name.get(), value);
            }
            return addUnnamedField(value);
        }

        public Row build()
        {
            return new Row(builder.build());
        }
    }
}
